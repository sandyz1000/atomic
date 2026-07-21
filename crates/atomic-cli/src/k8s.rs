//! `atomic submit-k8s`: creates one ad-hoc `batch/v1 Job` running the driver, either
//! from a pre-built `--image` or by staging a local `--binary` to S3 and running it
//! through the generic `atomic-bootstrap` fetch-and-exec image (see
//! [`atomic_k8s::driver_job`] for the Job spec itself — no image build required).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use atomic_k8s::{DriverJobSpec, InitFetch, build_driver_job};
use aws_config::BehaviorVersion;
use k8s_openapi::api::batch::v1::Job;
use kube::api::PostParams;
use kube::{Api, Client};

use crate::{Result, SubmitK8sArgs};

/// Errors specific to `atomic submit-k8s`. Kept in one enum so the whole k8s-only group
/// carries a single feature gate; wrapped into [`CliError`] via `#[from]` so `?` in these
/// (`Result<_, CliError>`) functions converts transparently.
#[derive(Debug, thiserror::Error)]
pub(crate) enum K8sError {
    #[error("--binary requires --s3-bucket to stage the driver binary")]
    MissingS3Bucket,

    #[error(
        "--dynamic-workers needs a worker image: pass --worker-image, or --image \
         (workers default to the driver's own image)"
    )]
    MissingWorkerImage,

    #[error("failed to read binary {0}: {1}")]
    BinaryReadFailed(PathBuf, std::io::Error),

    #[error("S3 upload to s3://{bucket}/{key} failed: {source}")]
    S3Upload {
        bucket: String,
        key: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    // `kube::Error` is a large enum; box it so `CliError` (and every `Result<_, CliError>`
    // in this crate, including non-k8s ones) doesn't inherit its size.
    #[error("failed to connect to the Kubernetes API: {0}")]
    KubeClient(#[source] Box<kube::Error>),

    #[error("failed to create Job {name} in namespace {namespace}: {source}")]
    KubeJobCreate {
        name: String,
        namespace: String,
        #[source]
        source: Box<kube::Error>,
    },
}

/// Fetch-and-exec image published by the project's own release process (not built
/// per-job, not per-user — see `crates/atomic-bootstrap`).
const DEFAULT_BOOTSTRAP_IMAGE: &str = "ghcr.io/atomic-rs/atomic-bootstrap:latest";

pub(crate) async fn cmd_submit_k8s(args: SubmitK8sArgs) -> Result<()> {
    let name = format!("atomic-job-{}", short_id());

    let worker_image = args
        .worker_image
        .clone()
        .or_else(|| args.source.image.clone());
    if args.dynamic_workers && worker_image.as_deref().unwrap_or("").is_empty() {
        return Err(K8sError::MissingWorkerImage.into());
    }

    let fetch = match &args.source.binary {
        None => None,
        Some(binary) => {
            let bucket = args.s3_bucket.clone().ok_or(K8sError::MissingS3Bucket)?;
            let key = stage_key(&args.s3_prefix, &name);
            upload_to_s3(binary, &bucket, &key).await?;
            let url = format!("s3://{bucket}/{key}");
            println!("Staged {} to {url}", binary.display());
            Some(InitFetch {
                bootstrap_image: args
                    .bootstrap_image
                    .clone()
                    .unwrap_or_else(|| DEFAULT_BOOTSTRAP_IMAGE.to_string()),
                url,
            })
        }
    };

    let mut env = BTreeMap::new();
    if args.dynamic_workers {
        env.insert("ATOMIC_ALLOCATOR".to_string(), "kube".to_string());
        env.insert("ATOMIC_K8S_NAMESPACE".to_string(), args.namespace.clone());
        env.insert(
            "ATOMIC_K8S_WORKER_IMAGE".to_string(),
            worker_image.unwrap_or_default(),
        );
        env.insert(
            "ATOMIC_K8S_TASK_PORT".to_string(),
            args.task_port.to_string(),
        );
        if let Some(sa) = &args.service_account {
            env.insert("ATOMIC_K8S_SERVICE_ACCOUNT".to_string(), sa.clone());
        }
    }

    let spec = DriverJobSpec {
        namespace: &args.namespace,
        // Ignored by build_driver_job when `fetch` is Some (bootstrap image used instead).
        image: args.source.image.as_deref().unwrap_or(""),
        service_account: args.service_account.as_deref(),
        job_args: &args.job_args,
        env,
        resources: None,
        ttl_seconds_after_finished: args.ttl_seconds_after_finished,
        fetch,
    };
    let job: Job = build_driver_job(&spec, &name);

    let client = Client::try_default()
        .await
        .map_err(|e| K8sError::KubeClient(Box::new(e)))?;
    let jobs: Api<Job> = Api::namespaced(client, &args.namespace);
    jobs.create(&PostParams::default(), &job)
        .await
        .map_err(|source| K8sError::KubeJobCreate {
            name: name.clone(),
            namespace: args.namespace.clone(),
            source: Box::new(source),
        })?;

    println!("Submitted job {name} in namespace {}", args.namespace);
    println!(
        "Follow logs: kubectl logs -f job/{name} -n {} -c driver",
        args.namespace
    );
    Ok(())
}

fn stage_key(prefix: &Option<String>, name: &str) -> String {
    match prefix.as_deref().map(|p| p.trim_matches('/')) {
        Some(p) if !p.is_empty() => format!("{p}/{name}/app"),
        _ => format!("{name}/app"),
    }
}

fn short_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..8].to_string()
}

async fn upload_to_s3(path: &Path, bucket: &str, key: &str) -> Result<()> {
    let data =
        std::fs::read(path).map_err(|e| K8sError::BinaryReadFailed(path.to_path_buf(), e))?;

    let cfg = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = aws_sdk_s3::Client::new(&cfg);
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(data.into())
        .send()
        .await
        .map_err(|e| K8sError::S3Upload {
            bucket: bucket.to_string(),
            key: key.to_string(),
            source: Box::new(e),
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_key_no_prefix() {
        assert_eq!(stage_key(&None, "job-abc"), "job-abc/app");
    }

    #[test]
    fn stage_key_with_prefix_trims_slashes() {
        assert_eq!(
            stage_key(&Some("/jobs/".to_string()), "job-abc"),
            "jobs/job-abc/app"
        );
    }
}
