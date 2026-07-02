//! atomic-bootstrap — fetch-and-exit init container for `atomic submit-k8s --binary`.
//!
//! Downloads the driver binary staged at `ATOMIC_FETCH_URL` (an `s3://bucket/key` URI)
//! to `ATOMIC_FETCH_DEST`, marks it executable, and exits. Runs as a Kubernetes
//! initContainer — the main container then execs `ATOMIC_FETCH_DEST` directly, so this
//! binary never runs the fetched program itself.
//!
//!   ATOMIC_FETCH_URL=s3://bucket/prefix/app ATOMIC_FETCH_DEST=/atomic/app atomic-bootstrap

use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;

#[derive(Debug, thiserror::Error)]
enum BootstrapError {
    #[error("{0} is not set")]
    MissingEnv(&'static str),

    #[error("ATOMIC_FETCH_URL must be an s3://bucket/key URI, got: {0}")]
    UnsupportedScheme(String),

    #[error("S3 get_object failed for s3://{bucket}/{key}: {source}")]
    GetObject {
        bucket: String,
        key: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("failed to collect S3 object body for s3://{bucket}/{key}: {source}")]
    CollectBody {
        bucket: String,
        key: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("failed to write {path}: {source}")]
    Write {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to set executable permission on {path}: {source}")]
    Chmod {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

/// A parsed `s3://bucket/key` URI. Deliberately not shared with
/// `atomic_compute::io::s3`'s equivalent type — pulling in that crate here would drag
/// the whole RDD engine into a minimal, fast-starting init container for one struct.
#[derive(Debug)]
struct S3Uri {
    bucket: String,
    key: String,
}

impl S3Uri {
    fn parse(uri: &str) -> Result<Self, BootstrapError> {
        let rest = uri
            .strip_prefix("s3://")
            .ok_or_else(|| BootstrapError::UnsupportedScheme(uri.to_string()))?;
        let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
        Ok(S3Uri {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })
    }
}

fn require_env(key: &'static str) -> Result<String, BootstrapError> {
    std::env::var(key).map_err(|_| BootstrapError::MissingEnv(key))
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("atomic-bootstrap: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), BootstrapError> {
    let url = require_env("ATOMIC_FETCH_URL")?;
    let dest = require_env("ATOMIC_FETCH_DEST")?;
    let uri = S3Uri::parse(&url)?;

    let cfg = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = Client::new(&cfg);

    let resp = client
        .get_object()
        .bucket(&uri.bucket)
        .key(&uri.key)
        .send()
        .await
        .map_err(|e| BootstrapError::GetObject {
            bucket: uri.bucket.clone(),
            key: uri.key.clone(),
            source: Box::new(e),
        })?;

    let bytes = resp
        .body
        .collect()
        .await
        .map_err(|e| BootstrapError::CollectBody {
            bucket: uri.bucket.clone(),
            key: uri.key.clone(),
            source: Box::new(e),
        })?
        .into_bytes();

    std::fs::write(&dest, &bytes).map_err(|e| BootstrapError::Write {
        path: dest.clone(),
        source: e,
    })?;

    set_executable(&dest)?;

    println!(
        "atomic-bootstrap: fetched s3://{}/{} -> {dest} ({} bytes)",
        uri.bucket,
        uri.key,
        bytes.len()
    );
    Ok(())
}

#[cfg(unix)]
fn set_executable(path: &str) -> Result<(), BootstrapError> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).map_err(|e| {
        BootstrapError::Chmod {
            path: path.to_string(),
            source: e,
        }
    })
}

#[cfg(not(unix))]
fn set_executable(_path: &str) -> Result<(), BootstrapError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bucket_and_key() {
        let uri = S3Uri::parse("s3://my-bucket/prefix/app").unwrap();
        assert_eq!(uri.bucket, "my-bucket");
        assert_eq!(uri.key, "prefix/app");
    }

    #[test]
    fn parses_bucket_with_no_key() {
        let uri = S3Uri::parse("s3://my-bucket").unwrap();
        assert_eq!(uri.bucket, "my-bucket");
        assert_eq!(uri.key, "");
    }

    #[test]
    fn rejects_non_s3_scheme() {
        let err = S3Uri::parse("https://example.com/app").unwrap_err();
        assert!(matches!(err, BootstrapError::UnsupportedScheme(_)));
    }
}
