use std::path::{Path, PathBuf};
use std::sync::Arc;

use russh::ChannelMsg;
use russh::client::{self, Handle};
use russh::keys::{PrivateKeyWithHashAlg, check_known_hosts_path, load_secret_key};
use russh_sftp::client::SftpSession;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt as _;

use crate::{CliError, Result};

pub(crate) fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// Wrap a string in single quotes, escaping any embedded single quotes.
fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

pub(crate) fn ssh_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".ssh"))
}

pub(crate) struct KnownHostsChecker {
    pub(crate) host: String,
    pub(crate) port: u16,
}

impl client::Handler for KnownHostsChecker {
    type Error = CliError;

    async fn check_server_key(
        &mut self,
        server_public_key: &russh::keys::PublicKey,
    ) -> Result<bool, Self::Error> {
        let known_hosts = ssh_dir()
            .map(|d| d.join("known_hosts"))
            .ok_or(CliError::NoSshDir)?;

        if !known_hosts.exists() {
            return Err(CliError::KnownHostsMissing {
                host: self.host.clone(),
            });
        }

        match check_known_hosts_path(&self.host, self.port, server_public_key, &known_hosts) {
            Ok(true) => Ok(true),
            Ok(false) => Err(CliError::HostNotKnown {
                host: self.host.clone(),
            }),
            Err(e) if e.to_string().to_lowercase().contains("changed") => {
                Err(CliError::HostKeyMismatch {
                    host: self.host.clone(),
                    source: e,
                })
            }
            Err(e) => Err(e.into()),
        }
    }
}

pub(crate) async fn authenticate(
    session: &mut Handle<KnownHostsChecker>,
    user: &str,
    key_path: Option<&Path>,
) -> Result<()> {
    if let Some(path) = key_path {
        let key = load_secret_key(path, None)?;
        let auth_key = PrivateKeyWithHashAlg::new(Arc::new(key), None);
        let result = session.authenticate_publickey(user, auth_key).await?;
        if !result.success() {
            return Err(CliError::SshKeyAuthRejected {
                user: user.to_owned(),
                key: path.to_path_buf(),
            });
        }
        return Ok(());
    }

    let ssh_dir = ssh_dir().ok_or(CliError::NoHomeDir)?;
    for name in &["id_ed25519", "id_ecdsa", "id_rsa"] {
        let path = ssh_dir.join(name);
        if !path.exists() {
            continue;
        }
        let Ok(key) = load_secret_key(&path, None) else {
            continue;
        };
        let auth_key = PrivateKeyWithHashAlg::new(Arc::new(key), None);
        if session
            .authenticate_publickey(user, auth_key)
            .await
            .map(|r| r.success())
            .unwrap_or(false)
        {
            return Ok(());
        }
    }

    Err(CliError::SshAuthFailed {
        user: user.to_owned(),
    })
}

pub(crate) async fn exec_checked(session: &mut Handle<KnownHostsChecker>, cmd: &str) -> Result<()> {
    let (output, code) = exec_output(session, cmd).await?;
    if code != 0 {
        return Err(CliError::RemoteCommandFailed {
            code,
            cmd: cmd.to_owned(),
            output: output.trim().to_owned(),
        });
    }
    Ok(())
}

pub(crate) async fn exec_output(
    session: &mut Handle<KnownHostsChecker>,
    cmd: &str,
) -> Result<(String, i32)> {
    let mut channel = session.channel_open_session().await?;
    channel.exec(true, cmd).await?;

    let mut stdout = Vec::new();
    let mut exit_code = 0i32;
    loop {
        match channel.wait().await {
            Some(ChannelMsg::Data { data }) => stdout.extend_from_slice(&data),
            Some(ChannelMsg::ExitStatus { exit_status }) => {
                exit_code = exit_status as i32;
            }
            Some(ChannelMsg::Eof) | None => break,
            _ => {}
        }
    }

    Ok((String::from_utf8_lossy(&stdout).into_owned(), exit_code))
}

/// SSH connect → authenticate → mkdir → SFTP upload → verify SHA-256 → atomic rename.
pub(crate) async fn ship_to_host(
    host: &str,
    user: &str,
    key_path: Option<&Path>,
    binary_data: &[u8],
    expected_sha256: &str,
    remote_path: &str,
) -> Result<()> {
    let config = Arc::new(client::Config::default());
    let checker = KnownHostsChecker {
        host: host.to_owned(),
        port: 22,
    };
    let mut session = client::connect(config, (host, 22u16), checker).await?;
    authenticate(&mut session, user, key_path).await?;

    let remote_dir = Path::new(remote_path)
        .parent()
        .and_then(|p| p.to_str())
        .unwrap_or("/opt/atomic");
    exec_checked(
        &mut session,
        &format!("mkdir -p {}", shell_quote(remote_dir)),
    )
    .await?;

    // Write to `<path>.tmp`; the rename below is atomic on POSIX systems.
    let tmp_path = format!("{}.tmp", remote_path);
    {
        let channel = session.channel_open_session().await?;
        channel.request_subsystem(true, "sftp").await?;
        let sftp = SftpSession::new(channel.into_stream()).await?;
        let mut remote_file = sftp.create(&tmp_path).await?;
        remote_file.write_all(binary_data).await?;
        remote_file.shutdown().await?;
    }

    // Verify SHA-256 before renaming to the final path.
    let (output, code) = exec_output(
        &mut session,
        &format!("sha256sum {}", shell_quote(&tmp_path)),
    )
    .await?;
    if code != 0 {
        let _ = exec_output(&mut session, &format!("rm -f {}", shell_quote(&tmp_path))).await;
        return Err(CliError::RemoteSha256Failed {
            host: host.to_owned(),
            code,
        });
    }
    let remote_sha256 = output
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim()
        .to_owned();
    if remote_sha256 != expected_sha256 {
        let _ = exec_output(&mut session, &format!("rm -f {}", shell_quote(&tmp_path))).await;
        return Err(CliError::Sha256Mismatch {
            host: host.to_owned(),
            expected: expected_sha256.to_owned(),
            got: remote_sha256,
        });
    }

    exec_checked(
        &mut session,
        &format!(
            "chmod 755 {} && mv {} {}",
            shell_quote(&tmp_path),
            shell_quote(&tmp_path),
            shell_quote(remote_path),
        ),
    )
    .await?;

    session
        .disconnect(russh::Disconnect::ByApplication, "", "English")
        .await
        .ok();
    Ok(())
}
