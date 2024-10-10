//

use atomic_api::conf::AtomicConf;

pub fn main() {
    let conf = AtomicConf::new();
    let gateway_server: Py4JServer = Py4JServer::new(conf);

    gateway_server.start();
    let bound_port: i32 = gateway_server.getListeningPort();
    if bound_port == -1 {
        error!("{} failed to bind; exiting", gatewayServer.server);
        std::process::exit(1);
    } else {
        let address = InetAddress.getLoopbackAddress();
        debug!(
            "Started PythonGatewayServer on {} with port {}",
            address, boundPort
        );
    }
    // TODO: Fix here

    // Communicate the connection information back to the python process by writing the
    // information in the requested file. This needs to match the read side in gateway.py.
    let connection_info_path = File::new(sys.env("PY_DRIVER_CONN_INFO_PATH"));
    let tmp_path = Files::create_temp_file(
        connection_info_path.getParentFile().toPath(),
        "connection",
        ".info",
    )
    .toFile();

    let dos = DataOutputStream::new(FileOutputStream::new(tmp_path));
    dos.writeInt(bound_port);

    let secret_bytes = gateway_server.secret.getBytes(UTF_8);
    dos.writeInt(secret_bytes.length);
    dos.write(secret_bytes, 0, secret_bytes.length);
    dos.close();

    if !tmp_path.renameTo(connection_info_path) {
        error!(
            "Unable to write connection information to {}",
            connectionInfoPath
        );
        std::process::exit(1);
    }

    // Exit on EOF or broken pipe to ensure that this process dies when the Python driver dies:
    while std::io::stdin().read() != -1 {
        // Do nothing
    }
    debug!("Exiting due to broken pipe from Python driver");
    std::process::exit(0);
}
