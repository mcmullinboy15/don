/// Find a free TCP port by binding to port 0 and reading the assigned port.
///
/// The port is released immediately after discovery, so there's a small race
/// window. For tests this is acceptable — use unique port variables per test.
pub fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}
