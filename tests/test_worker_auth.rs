//! Cluster auth token: worker TCP connections must open with a matching `Auth`
//! frame and HTTP callers must send `Authorization: Bearer <token>` once
//! `ATOMIC_AUTH_TOKEN` is configured.
//!
//! One test function: the token lives in a process-global, so assertion order
//! (before / after `set_auth_token`) must be deterministic.

use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use atomic_compute::executor::Executor;
use atomic_data::distributed::{
    TRANSPORT_HEADER_LEN, TransportFrameKind, encode_transport_frame, parse_transport_header,
};

const TOKEN: &str = "cluster-secret";

fn wait_for_port(addr: SocketAddrV4) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("worker port {addr} never opened");
}

/// Send `frames` on a fresh connection; return the response frame kind, or
/// `None` when the worker closed the connection without responding.
fn send_frames(addr: SocketAddrV4, frames: &[u8]) -> Option<TransportFrameKind> {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");
    stream.write_all(frames).expect("write frames");
    let mut header = [0_u8; TRANSPORT_HEADER_LEN];
    match stream.read_exact(&mut header) {
        Ok(()) => {
            let (kind, payload_len) = parse_transport_header(&header).expect("parse header");
            let mut payload = vec![0_u8; payload_len];
            stream.read_exact(&mut payload).expect("read payload");
            Some(kind)
        }
        Err(_) => None,
    }
}

#[test]
fn test_worker_requires_token() {
    // Auth disabled: everything passes.
    assert!(atomic_data::env::bearer_authorized(None));
    assert!(atomic_data::env::auth_token_matches(b"anything"));

    let port = 29841;
    let addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
    let exec = Arc::new(Executor::new(port, 4));
    std::thread::spawn(move || {
        let _ = exec.worker();
    });
    wait_for_port(addr);

    let caps_frame = encode_transport_frame(TransportFrameKind::WorkerCapabilities, &[]);

    // No token configured yet: a plain handshake succeeds.
    assert_eq!(
        send_frames(addr, &caps_frame),
        Some(TransportFrameKind::WorkerCapabilities),
        "handshake must pass while auth is disabled"
    );

    atomic_data::env::set_auth_token(TOKEN.to_string());

    // Bearer helper honours the configured token.
    assert!(!atomic_data::env::bearer_authorized(None));
    assert!(!atomic_data::env::bearer_authorized(Some("Bearer wrong")));
    assert!(atomic_data::env::bearer_authorized(Some(&format!(
        "Bearer {TOKEN}"
    ))));

    // Un-authenticated handshake: worker closes without a response.
    assert_eq!(
        send_frames(addr, &caps_frame),
        None,
        "worker must drop connections that skip the Auth frame"
    );

    // Wrong token: also dropped.
    let mut bad = encode_transport_frame(TransportFrameKind::Auth, b"wrong-token");
    bad.extend_from_slice(&caps_frame);
    assert_eq!(
        send_frames(addr, &bad),
        None,
        "worker must drop connections with a bad token"
    );

    // Correct token: handshake succeeds.
    let mut good = encode_transport_frame(TransportFrameKind::Auth, TOKEN.as_bytes());
    good.extend_from_slice(&caps_frame);
    assert_eq!(
        send_frames(addr, &good),
        Some(TransportFrameKind::WorkerCapabilities),
        "authenticated handshake must succeed"
    );
}
