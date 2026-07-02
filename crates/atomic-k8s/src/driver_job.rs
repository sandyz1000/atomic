//! Construction of the Kubernetes `Job` that runs a driver submitted via
//! `atomic submit-k8s`. Kept separate from the allocator's API lifecycle so the spec
//! mapping is testable without a cluster, mirroring [`crate::pod_spec`].

use std::collections::BTreeMap;

use k8s_openapi::api::batch::v1::{Job, JobSpec};
use k8s_openapi::api::core::v1::{
    Container, EmptyDirVolumeSource, EnvVar, PodSpec, PodTemplateSpec, ResourceRequirements,
    Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

/// Shared mount point for the fetched binary in the `--binary` submission path.
const SHARED_VOLUME: &str = "atomic-bin";
const SHARED_DIR: &str = "/atomic";

/// Where to fetch the driver binary from before running it, when the job was submitted
/// with `--binary` instead of a pre-built `--image`. `bootstrap_image` doubles as both
/// the init container (fetch) and main container (exec) image.
pub struct InitFetch {
    pub bootstrap_image: String,
    /// `s3://` or `https://` location the bootstrap image downloads from.
    pub url: String,
}

/// Inputs to [`build_driver_job`].
pub struct DriverJobSpec<'a> {
    pub namespace: &'a str,
    /// Ignored when `fetch` is `Some` — the bootstrap image is used for both containers.
    pub image: &'a str,
    pub service_account: Option<&'a str>,
    /// Passed after `"--driver"`.
    pub job_args: &'a [String],
    pub env: BTreeMap<String, String>,
    pub resources: Option<ResourceRequirements>,
    pub ttl_seconds_after_finished: Option<i32>,
    /// `Some` selects the `--binary` submission path (initContainer fetch); `None` runs
    /// `image` directly (the `--image` submission path).
    pub fetch: Option<InitFetch>,
}

/// Build the `Job` for one `atomic submit-k8s` invocation.
pub fn build_driver_job(spec: &DriverJobSpec<'_>, name: &str) -> Job {
    let env: Vec<EnvVar> = spec
        .env
        .iter()
        .map(|(k, v)| EnvVar {
            name: k.clone(),
            value: Some(v.clone()),
            value_from: None,
        })
        .collect();

    let mut args = vec!["--driver".to_string()];
    args.extend(spec.job_args.iter().cloned());

    let (main_image, command, init_containers, volumes, volume_mounts) = match &spec.fetch {
        None => (spec.image.to_string(), None, None, None, None),
        Some(fetch) => {
            let dest = format!("{SHARED_DIR}/app");
            let init = Container {
                name: "fetch".to_string(),
                image: Some(fetch.bootstrap_image.clone()),
                env: Some(vec![
                    EnvVar {
                        name: "ATOMIC_FETCH_URL".to_string(),
                        value: Some(fetch.url.clone()),
                        value_from: None,
                    },
                    EnvVar {
                        name: "ATOMIC_FETCH_DEST".to_string(),
                        value: Some(dest.clone()),
                        value_from: None,
                    },
                ]),
                volume_mounts: Some(vec![VolumeMount {
                    name: SHARED_VOLUME.to_string(),
                    mount_path: SHARED_DIR.to_string(),
                    ..Default::default()
                }]),
                ..Default::default()
            };
            let volume = Volume {
                name: SHARED_VOLUME.to_string(),
                empty_dir: Some(EmptyDirVolumeSource::default()),
                ..Default::default()
            };
            let mount = VolumeMount {
                name: SHARED_VOLUME.to_string(),
                mount_path: SHARED_DIR.to_string(),
                ..Default::default()
            };
            // Main container execs the fetched binary directly; `command` overrides the
            // bootstrap image's own entrypoint (fetch-and-exit) so it instead runs `dest`,
            // with `args` (`--driver ...`) passed straight through unchanged.
            (
                fetch.bootstrap_image.clone(),
                Some(vec![dest]),
                Some(vec![init]),
                Some(vec![volume]),
                Some(vec![mount]),
            )
        }
    };

    let container = Container {
        name: "driver".to_string(),
        image: Some(main_image),
        command,
        args: Some(args),
        env: (!env.is_empty()).then_some(env),
        resources: spec.resources.clone(),
        volume_mounts,
        ..Default::default()
    };

    let mut labels = BTreeMap::new();
    labels.insert("atomic.dev/role".to_string(), "driver".to_string());

    Job {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(spec.namespace.to_string()),
            labels: Some(labels),
            ..Default::default()
        },
        spec: Some(JobSpec {
            backoff_limit: Some(0),
            ttl_seconds_after_finished: spec.ttl_seconds_after_finished,
            template: PodTemplateSpec {
                metadata: None,
                spec: Some(PodSpec {
                    containers: vec![container],
                    init_containers,
                    volumes,
                    service_account_name: spec.service_account.map(str::to_string),
                    restart_policy: Some("Never".to_string()),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_spec(fetch: Option<InitFetch>) -> DriverJobSpec<'static> {
        DriverJobSpec {
            namespace: "atomic",
            image: "myrepo/atomic:0.1",
            service_account: Some("atomic-driver"),
            job_args: &[],
            env: BTreeMap::new(),
            resources: None,
            ttl_seconds_after_finished: Some(3600),
            fetch,
        }
    }

    #[test]
    fn image_path_runs_image_directly() {
        let spec = base_spec(None);
        let job = build_driver_job(&spec, "job-abc");

        assert_eq!(job.metadata.name.as_deref(), Some("job-abc"));
        let job_spec = job.spec.unwrap();
        assert_eq!(job_spec.backoff_limit, Some(0));
        assert_eq!(job_spec.ttl_seconds_after_finished, Some(3600));

        let pod_spec = job_spec.template.spec.unwrap();
        assert_eq!(pod_spec.restart_policy.as_deref(), Some("Never"));
        assert!(pod_spec.init_containers.is_none());
        assert!(pod_spec.volumes.is_none());

        let container = &pod_spec.containers[0];
        assert_eq!(container.image.as_deref(), Some("myrepo/atomic:0.1"));
        assert!(
            container.command.is_none(),
            "--image path must not override the image's own entrypoint"
        );
        assert_eq!(
            container.args.as_ref().unwrap(),
            &vec!["--driver".to_string()]
        );
    }

    #[test]
    fn binary_path_adds_fetch_init_container() {
        let fetch = InitFetch {
            bootstrap_image: "ghcr.io/atomic-rs/bootstrap:0.1".to_string(),
            url: "s3://bucket/prefix/app".to_string(),
        };
        let spec = base_spec(Some(fetch));
        let job = build_driver_job(&spec, "job-xyz");

        let pod_spec = job.spec.unwrap().template.spec.unwrap();
        let init = &pod_spec.init_containers.as_ref().unwrap()[0];
        assert_eq!(
            init.image.as_deref(),
            Some("ghcr.io/atomic-rs/bootstrap:0.1")
        );
        let init_env = init.env.as_ref().unwrap();
        assert!(init_env.iter().any(|e| e.name == "ATOMIC_FETCH_URL"
            && e.value.as_deref() == Some("s3://bucket/prefix/app")));

        let volumes = pod_spec.volumes.as_ref().unwrap();
        assert_eq!(volumes[0].name, SHARED_VOLUME);
        assert!(volumes[0].empty_dir.is_some());

        let container = &pod_spec.containers[0];
        assert_eq!(
            container.image.as_deref(),
            Some("ghcr.io/atomic-rs/bootstrap:0.1")
        );
        assert_eq!(
            container.command.as_ref().unwrap(),
            &vec!["/atomic/app".to_string()],
            "command should exec the fetched binary directly, overriding the bootstrap image's own entrypoint"
        );
        assert_eq!(
            container.args.as_ref().unwrap(),
            &vec!["--driver".to_string()],
            "args must be unpolluted by the fetch destination path"
        );
        assert!(container.volume_mounts.is_some());
    }

    #[test]
    fn job_args_appended_after_driver_flag() {
        let job_args = vec!["--my-flag".to_string(), "foo".to_string()];
        let mut spec = base_spec(None);
        spec.job_args = &job_args;
        let job = build_driver_job(&spec, "job-args");

        let pod_spec = job.spec.unwrap().template.spec.unwrap();
        assert_eq!(
            pod_spec.containers[0].args.as_ref().unwrap(),
            &vec![
                "--driver".to_string(),
                "--my-flag".to_string(),
                "foo".to_string()
            ]
        );
    }
}
