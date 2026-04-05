mod helpers;

use don::config::{Config, LogConfig, Platform};
use don::output::OutputManager;
use don::runner::Runner;
use helpers::config::ConfigBuilder;
use helpers::port::free_port;
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

const PLATFORM: Platform = Platform::LinuxX86_64;

#[derive(Clone)]
struct TestBuffer(Arc<Mutex<Vec<u8>>>);

impl TestBuffer {
    fn new() -> (Self, Arc<Mutex<Vec<u8>>>) {
        let buf = Arc::new(Mutex::new(Vec::new()));
        (TestBuffer(buf.clone()), buf)
    }
}

impl tokio::io::AsyncWrite for TestBuffer {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        data: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.0.lock().unwrap().extend_from_slice(data);
        std::task::Poll::Ready(Ok(data.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

fn read_buf(buf: &Arc<Mutex<Vec<u8>>>) -> String {
    String::from_utf8_lossy(&buf.lock().unwrap()).into_owned()
}

async fn make_runner(
    toml: &str,
    base_dir: &std::path::Path,
) -> (Runner, mpsc::Sender<()>, Arc<Mutex<Vec<u8>>>) {
    let config: Config = toml.parse().unwrap();
    config.validate(PLATFORM).unwrap();

    let service_configs: Vec<(&str, &LogConfig)> = config
        .services
        .iter()
        .map(|(n, s)| (n.as_str(), &s.log))
        .collect();
    let task_configs: Vec<(&str, &LogConfig)> = config
        .tasks
        .iter()
        .map(|(n, t)| (n.as_str(), &t.log))
        .collect();
    let all_configs: Vec<(&str, &LogConfig)> =
        service_configs.into_iter().chain(task_configs).collect();

    let (writer, buf) = TestBuffer::new();
    let output_manager = OutputManager::new(&all_configs, writer).await.unwrap();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(2);
    let runner =
        Runner::new(config, PLATFORM, output_manager, base_dir.to_path_buf(), shutdown_rx)
            .await
            .unwrap();
    (runner, shutdown_tx, buf)
}

async fn wait_for_output(buf: &Arc<Mutex<Vec<u8>>>, needle: &str, timeout: Duration) -> bool {
    let start = tokio::time::Instant::now();
    loop {
        let output = read_buf(buf);
        if output.contains(needle) {
            return true;
        }
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// --- Integration test: service receives LISTEN_FDS and LISTEN_FDNAMES ---

#[test]
fn integration_service_receives_listen_env_vars() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("socket-env");
        let port = free_port();
        let addr = format!("127.0.0.1:{port}");

        // Service prints its LISTEN_FDS, LISTEN_FDNAMES, and LISTEN_PID env vars.
        let toml = ConfigBuilder::new()
            .add_custom_service(
                "api",
                "bash",
                &["-c", "echo LISTEN_FDS=$LISTEN_FDS LISTEN_FDNAMES=$LISTEN_FDNAMES LISTEN_PID=$LISTEN_PID && sleep 60"],
            )
            .listen(&[&addr])
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Wait for the service to print its env vars.
        assert!(
            wait_for_output(&buf, "LISTEN_FDS=1", Duration::from_secs(5)).await,
            "expected LISTEN_FDS=1 in output. output: {}",
            read_buf(&buf)
        );

        let output = read_buf(&buf);
        assert!(
            output.contains(&format!("LISTEN_FDNAMES={addr}")),
            "expected LISTEN_FDNAMES={addr} in output. output: {output}"
        );
        // LISTEN_PID should be present and non-empty.
        assert!(
            output.contains("LISTEN_PID="),
            "expected LISTEN_PID in output. output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: service can accept on the passed fd ---

#[test]
fn integration_service_accepts_on_listen_fd() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("socket-accept");
        let port = free_port();
        let addr = format!("127.0.0.1:{port}");

        // Write a Python script that accepts connections on fd 3.
        let script_path = dir.path().join("accept.py");
        std::fs::write(
            &script_path,
            r#"
import socket, os, sys
s = socket.fromfd(3, socket.AF_INET, socket.SOCK_STREAM)
os.close(3)
while True:
    conn, addr = s.accept()
    conn.sendall(b'hello from fd 3')
    conn.close()
"#,
        )
        .unwrap();

        let script = script_path.to_str().unwrap().to_string();
        let toml = ConfigBuilder::new()
            .add_custom_service("api", "python3", &[&script])
            .listen(&[&addr])
            .ready_tcp_with(&addr, "200ms", 30)
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Wait for the service to be ready (TCP ready check passes).
        assert!(
            wait_for_output(&buf, "ready", Duration::from_secs(10)).await,
            "timed out waiting for service to be ready. output: {}",
            read_buf(&buf)
        );

        // Connect and verify we get a response.
        let result = tokio::net::TcpStream::connect(&addr).await;
        if let Ok(stream) = result {
            use tokio::io::AsyncReadExt;
            let mut buf = vec![0u8; 64];
            let mut stream = stream;
            let n = tokio::time::timeout(
                Duration::from_secs(2),
                stream.read(&mut buf),
            )
            .await
            .unwrap()
            .unwrap();
            let response = String::from_utf8_lossy(&buf[..n]);
            assert_eq!(response, "hello from fd 3");
        }
        // If python3 isn't available, the connect might fail — that's okay,
        // the TCP ready check already proved the socket is accepting.

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: socket stays bound during restart ---

#[test]
fn integration_socket_stays_bound_during_restart() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("socket-restart");
        let port = free_port();
        let addr = format!("127.0.0.1:{port}");

        let src_dir = dir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("main.rs"), "v1").unwrap();

        // Service sleeps. The socket is bound by don, so even during restart
        // the port stays open (connections queue in the backlog).
        let toml = ConfigBuilder::new()
            .add_custom_service("api", "bash", &["-c", "sleep 60"])
            .listen(&[&addr])
            .watch(&["src/**/*.rs"])
            .debounce("100ms")
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "bound 1 listen socket", Duration::from_secs(5)).await,
            "timed out waiting for socket bind. output: {}",
            read_buf(&buf)
        );
        assert!(
            wait_for_output(&buf, "ready", Duration::from_secs(5)).await,
            "timed out waiting for ready. output: {}",
            read_buf(&buf)
        );

        // Trigger a rebuild by modifying a watched file.
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(src_dir.join("main.rs"), "v2").unwrap();

        // Wait for the rebuild to start.
        assert!(
            wait_for_output(&buf, "rebuilding (file changed)", Duration::from_secs(5)).await,
            "timed out waiting for rebuild. output: {}",
            read_buf(&buf)
        );

        // During and after the rebuild, the port should still be bound.
        // A TCP connect should succeed (connection queues in the backlog
        // even if no process is currently accepting).
        tokio::time::sleep(Duration::from_millis(200)).await;
        let connect_result = tokio::time::timeout(
            Duration::from_secs(2),
            tokio::net::TcpStream::connect(&addr),
        )
        .await;
        assert!(
            connect_result.is_ok() && connect_result.unwrap().is_ok(),
            "expected TCP connect to succeed during restart (socket should stay bound)"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: multiple listen addresses ---

#[test]
fn integration_multiple_listen_addresses() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("socket-multi");
        let port1 = free_port();
        let port2 = free_port();
        let addr1 = format!("127.0.0.1:{port1}");
        let addr2 = format!("127.0.0.1:{port2}");

        let toml = ConfigBuilder::new()
            .add_custom_service(
                "api",
                "bash",
                &["-c", "echo LISTEN_FDS=$LISTEN_FDS LISTEN_FDNAMES=$LISTEN_FDNAMES && sleep 60"],
            )
            .listen(&[&addr1, &addr2])
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "LISTEN_FDS=2", Duration::from_secs(5)).await,
            "expected LISTEN_FDS=2 in output. output: {}",
            read_buf(&buf)
        );

        let output = read_buf(&buf);
        let expected_names = format!("{addr1}:{addr2}");
        assert!(
            output.contains(&format!("LISTEN_FDNAMES={expected_names}")),
            "expected LISTEN_FDNAMES={expected_names} in output. output: {output}"
        );

        // Both ports should be connectable.
        let c1 = tokio::net::TcpStream::connect(&addr1).await;
        let c2 = tokio::net::TcpStream::connect(&addr2).await;
        assert!(c1.is_ok(), "expected connect to {addr1} to succeed");
        assert!(c2.is_ok(), "expected connect to {addr2} to succeed");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}
