/// TLS helpers for mutual TLS (mTLS) between driver and workers.
///
/// Enabled via the `tls` feature flag.  When `Config::tls_ca_cert` is `None`,
/// all connections use plain TCP (no change in behaviour).
///
/// # Cert distribution
///
/// `atomic-cli`'s `atomic build` generates a cluster CA (stored in
/// `~/.atomic/cluster.ca.pem` + `~/.atomic/cluster.ca.key.pem`).
/// `atomic ship` uploads the CA cert alongside the binary.  Workers generate
/// their own cert signed by the cluster CA on first startup.
///
/// # Usage
///
/// ```ignore
/// let server_cfg = make_server_config(cert_path, key_path, ca_path)?;
/// let acceptor = TlsAcceptor::from(server_cfg);
/// let tls_stream = acceptor.accept(tcp_stream).await?;
/// ```
#[cfg(feature = "tls")]
pub mod tls_impl {
    use std::io::{self, BufReader};
    use std::path::Path;
    use std::sync::Arc;

    use rustls::ClientConfig;
    use rustls::RootCertStore;
    use rustls::ServerConfig;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use rustls_pemfile::{certs, pkcs8_private_keys};
    pub use tokio_rustls::{TlsAcceptor, TlsConnector};

    fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
        let f = std::fs::File::open(path).map_err(|e| {
            io::Error::new(io::ErrorKind::NotFound, format!("{}: {e}", path.display()))
        })?;
        certs(&mut BufReader::new(f))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
    }

    fn load_private_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
        let f = std::fs::File::open(path).map_err(|e| {
            io::Error::new(io::ErrorKind::NotFound, format!("{}: {e}", path.display()))
        })?;
        pkcs8_private_keys(&mut BufReader::new(f))
            .next()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "no PKCS#8 private key found")
            })?
            .map(PrivateKeyDer::Pkcs8)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
    }

    fn load_ca(path: &Path) -> io::Result<RootCertStore> {
        let mut store = RootCertStore::empty();
        for cert in load_certs(path)? {
            store
                .add(cert)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        }
        Ok(store)
    }

    /// Build a `rustls::ServerConfig` for mutual TLS.
    ///
    /// - `cert_path`: PEM file containing the server certificate chain.
    /// - `key_path`: PEM file containing the server's PKCS#8 private key.
    /// - `ca_path`: PEM file containing the cluster CA certificate used to
    ///   verify client (driver) certificates.
    pub fn make_server_config(
        cert_path: &Path,
        key_path: &Path,
        ca_path: &Path,
    ) -> io::Result<Arc<ServerConfig>> {
        let certs = load_certs(cert_path)?;
        let key = load_private_key(key_path)?;
        let ca_store = load_ca(ca_path)?;

        let client_auth = rustls::server::WebPkiClientVerifier::builder(Arc::new(ca_store))
            .build()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        let cfg = ServerConfig::builder()
            .with_client_cert_verifier(client_auth)
            .with_single_cert(certs, key)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        Ok(Arc::new(cfg))
    }

    /// Build a `rustls::ClientConfig` for mutual TLS.
    ///
    /// - `cert_path`: PEM file containing the client (driver) certificate chain.
    /// - `key_path`: PEM file containing the client's PKCS#8 private key.
    /// - `ca_path`: PEM file containing the cluster CA certificate used to
    ///   verify server (worker) certificates.
    pub fn make_client_config(
        cert_path: &Path,
        key_path: &Path,
        ca_path: &Path,
    ) -> io::Result<Arc<ClientConfig>> {
        let certs = load_certs(cert_path)?;
        let key = load_private_key(key_path)?;
        let ca_store = load_ca(ca_path)?;

        let cfg = ClientConfig::builder()
            .with_root_certificates(ca_store)
            .with_client_auth_cert(certs, key)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        Ok(Arc::new(cfg))
    }
}

/// Returns `true` if TLS is configured in the given cert/key/ca paths.
pub fn tls_is_configured(
    ca: Option<&std::path::Path>,
    cert: Option<&std::path::Path>,
    key: Option<&std::path::Path>,
) -> bool {
    ca.is_some() && cert.is_some() && key.is_some()
}
