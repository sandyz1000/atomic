/// S3 I/O helpers using the official AWS SDK.
///
/// Requires the `s3` feature flag. Credentials are loaded by `aws-config` from the
/// standard chain: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` env vars,
/// `~/.aws/credentials`, EC2 instance profile, etc.
#[cfg(feature = "s3")]
pub mod s3_impl {
    use aws_config::BehaviorVersion;
    use aws_sdk_s3::Client;

    /// A parsed `s3://bucket/prefix` URI.
    #[derive(Debug, Clone)]
    pub struct S3Uri {
        pub bucket: String,
        pub key: String,
    }

    impl S3Uri {
        /// Parse `s3://bucket/key`. Returns `None` if the scheme is not `s3://`.
        pub fn parse(uri: &str) -> Option<Self> {
            let rest = uri.strip_prefix("s3://")?;
            let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
            Some(S3Uri {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
            })
        }
    }

    /// Build an S3 client using the default AWS config chain.
    async fn make_client() -> Client {
        let cfg = aws_config::load_defaults(BehaviorVersion::latest()).await;
        Client::new(&cfg)
    }

    /// List all object keys under `bucket/prefix`. Returns at most 1000 keys per page
    /// (AWS default); for large prefixes this paginates automatically.
    pub fn list_keys(bucket: &str, prefix: &str) -> Vec<String> {
        let bucket = bucket.to_owned();
        let prefix = prefix.to_owned();
        run_sync(async move {
            let client = make_client().await;
            let mut keys = Vec::new();
            let mut paginator = client
                .list_objects_v2()
                .bucket(&bucket)
                .prefix(&prefix)
                .into_paginator()
                .send();
            while let Some(page) = paginator.next().await {
                match page {
                    Ok(output) => {
                        for obj in output.contents() {
                            if let Some(k) = obj.key() {
                                keys.push(k.to_owned());
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("S3 list_objects error: {e}");
                        break;
                    }
                }
            }
            keys
        })
    }

    /// Read an S3 object and return its content split into lines.
    pub fn read_lines(bucket: &str, key: &str) -> Vec<String> {
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        run_sync(async move {
            let client = make_client().await;
            match client.get_object().bucket(&bucket).key(&key).send().await {
                Ok(resp) => match resp.body.collect().await {
                    Ok(bytes) => {
                        let text = String::from_utf8_lossy(&bytes.into_bytes()).into_owned();
                        text.lines().map(|l| l.to_owned()).collect()
                    }
                    Err(e) => {
                        log::error!("S3 body collect error for s3://{bucket}/{key}: {e}");
                        vec![]
                    }
                },
                Err(e) => {
                    log::error!("S3 get_object error for s3://{bucket}/{key}: {e}");
                    vec![]
                }
            }
        })
    }

    /// Upload text content to `s3://bucket/key`.
    pub fn write_text(bucket: &str, key: &str, content: String) -> Result<(), String> {
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        run_sync(async move {
            let client = make_client().await;
            client
                .put_object()
                .bucket(&bucket)
                .key(&key)
                .body(content.into_bytes().into())
                .send()
                .await
                .map_err(|e| format!("S3 put_object error for s3://{bucket}/{key}: {e}"))
                .map(|_| ())
        })
    }

    /// Run an async block synchronously. Safe to call from `spawn_blocking` tasks
    /// because `block_in_place` yields the executor thread to the runtime while
    /// the blocking work runs.
    fn run_sync<F, T>(fut: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        // block_in_place is only available inside a multi-thread tokio runtime.
        // If we're outside tokio (e.g. unit tests), fall back to a one-shot runtime.
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(fut)),
            Err(_) => tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build tokio runtime for S3")
                .block_on(fut),
        }
    }
}
