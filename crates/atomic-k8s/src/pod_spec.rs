//! Pure construction of the Kubernetes `Pod` object for one dedicated worker, plus
//! the readiness check used while waiting for it to come up. Kept separate from the
//! allocator's API lifecycle so the spec mapping is testable without a cluster.

use std::collections::BTreeMap;

use atomic_scheduler::{AllocatorError, AllocatorResult, ResourceProfile};
use k8s_openapi::api::core::v1::{
    Affinity, Container, ContainerPort, EnvVar, Pod, PodSpec, ResourceRequirements, Toleration,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};

/// Label carrying the per-allocation id; `release` deletes by this selector.
pub const ALLOC_ID_LABEL: &str = "atomic.dev/alloc-id";
/// Label marking every Atomic-managed worker pod.
pub const ROLE_LABEL: &str = "atomic.dev/role";

/// Driver pod identity used as an `OwnerReference` so Kubernetes garbage-collects
/// worker pods when the driver pod is deleted.
#[derive(Debug, Clone)]
pub struct DriverOwner {
    pub name: String,
    pub uid: String,
}

/// Inputs to [`build_pod`] that are constant across a job's workers.
pub(crate) struct PodTemplate<'a> {
    pub namespace: &'a str,
    pub image: &'a str,
    pub service_account: Option<&'a str>,
    pub task_port: u16,
    pub command: &'a [String],
    pub owner: Option<&'a DriverOwner>,
}

/// Build the `Pod` for worker `index` of allocation `alloc_id`.
pub(crate) fn build_pod(
    tmpl: &PodTemplate<'_>,
    alloc_id: &str,
    index: usize,
    profile: &ResourceProfile,
) -> AllocatorResult<Pod> {
    let mut labels = BTreeMap::new();
    labels.insert(ROLE_LABEL.to_string(), "worker".to_string());
    labels.insert(ALLOC_ID_LABEL.to_string(), alloc_id.to_string());
    for (k, v) in &profile.labels {
        labels.insert(k.clone(), v.clone());
    }

    let env: Vec<EnvVar> = profile
        .env
        .iter()
        .map(|(k, v)| EnvVar {
            name: k.clone(),
            value: Some(v.clone()),
            value_from: None,
        })
        .collect();

    let tolerations: Option<Vec<Toleration>> = match &profile.tolerations {
        Some(v) => Some(
            serde_json::from_value(v.clone())
                .map_err(|e| AllocatorError::Provision(format!("invalid tolerations: {e}")))?,
        ),
        None => None,
    };
    let affinity: Option<Affinity> = match &profile.affinity {
        Some(v) => Some(
            serde_json::from_value(v.clone())
                .map_err(|e| AllocatorError::Provision(format!("invalid affinity: {e}")))?,
        ),
        None => None,
    };
    let node_selector = (!profile.node_selector.is_empty()).then(|| profile.node_selector.clone());

    let container = Container {
        name: "worker".to_string(),
        image: Some(tmpl.image.to_string()),
        command: (!tmpl.command.is_empty()).then(|| tmpl.command.to_vec()),
        // The worker role is selected by these flags; AtomicApp::build reads them.
        args: Some(vec![
            "--worker".to_string(),
            "--port".to_string(),
            tmpl.task_port.to_string(),
        ]),
        ports: Some(vec![ContainerPort {
            container_port: i32::from(tmpl.task_port),
            ..Default::default()
        }]),
        resources: Some(build_resources(profile)),
        env: (!env.is_empty()).then_some(env),
        ..Default::default()
    };

    let owner_references = tmpl.owner.map(|o| {
        vec![OwnerReference {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            name: o.name.clone(),
            uid: o.uid.clone(),
            controller: Some(false),
            block_owner_deletion: Some(false),
        }]
    });

    Ok(Pod {
        metadata: ObjectMeta {
            name: Some(format!("atomic-worker-{alloc_id}-{index}")),
            namespace: Some(tmpl.namespace.to_string()),
            labels: Some(labels),
            owner_references,
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![container],
            service_account_name: tmpl.service_account.map(str::to_string),
            node_selector,
            tolerations,
            affinity,
            // Dedicated, single-job pods: never restart, the driver recreates if needed.
            restart_policy: Some("Never".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    })
}

fn build_resources(profile: &ResourceProfile) -> ResourceRequirements {
    let mut requests = BTreeMap::new();
    let mut limits = BTreeMap::new();
    if let Some(c) = &profile.cpu_request {
        requests.insert("cpu".to_string(), Quantity(c.clone()));
    }
    if let Some(c) = &profile.cpu_limit {
        limits.insert("cpu".to_string(), Quantity(c.clone()));
    }
    if let Some(m) = &profile.mem_request {
        requests.insert("memory".to_string(), Quantity(m.clone()));
    }
    if let Some(m) = &profile.mem_limit {
        limits.insert("memory".to_string(), Quantity(m.clone()));
    }
    // Extended resources (e.g. nvidia.com/gpu) must appear in both requests and
    // limits to be admitted by the device-plugin scheduler.
    for (k, v) in &profile.extended {
        requests.insert(k.clone(), Quantity(v.clone()));
        limits.insert(k.clone(), Quantity(v.clone()));
    }
    ResourceRequirements {
        requests: (!requests.is_empty()).then_some(requests),
        limits: (!limits.is_empty()).then_some(limits),
        ..Default::default()
    }
}

/// Return the pod's IP once it is `Running` and its `Ready` condition is `True`,
/// else `None` (still coming up).
pub(crate) fn ready_ip(pod: &Pod) -> Option<String> {
    let status = pod.status.as_ref()?;
    if status.phase.as_deref() != Some("Running") {
        return None;
    }
    let ready = status
        .conditions
        .as_ref()
        .is_some_and(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"));
    if !ready {
        return None;
    }
    status.pod_ip.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pod_carries_alloc_labels_and_resources() {
        let tmpl = PodTemplate {
            namespace: "atomic",
            image: "atomic:latest",
            service_account: Some("atomic-driver"),
            task_port: 10001,
            command: &[],
            owner: None,
        };
        let profile = ResourceProfile::new(1)
            .with_cpu("4", "8")
            .with_memory("8Gi", "16Gi")
            .with_gpu(1)
            .with_node_selector("pool", "gpu");
        let pod = build_pod(&tmpl, "abc", 0, &profile).unwrap();

        let labels = pod.metadata.labels.unwrap();
        assert_eq!(labels.get(ALLOC_ID_LABEL).map(String::as_str), Some("abc"));
        assert_eq!(labels.get(ROLE_LABEL).map(String::as_str), Some("worker"));

        let spec = pod.spec.unwrap();
        assert_eq!(
            spec.node_selector.unwrap().get("pool").map(String::as_str),
            Some("gpu")
        );
        let reqs = spec.containers[0]
            .resources
            .as_ref()
            .unwrap()
            .requests
            .as_ref()
            .unwrap();
        assert_eq!(reqs.get("cpu").unwrap().0, "4");
        assert_eq!(reqs.get("nvidia.com/gpu").unwrap().0, "1");
        assert_eq!(
            spec.containers[0].args.as_ref().unwrap(),
            &vec![
                "--worker".to_string(),
                "--port".to_string(),
                "10001".to_string()
            ]
        );
    }
}
