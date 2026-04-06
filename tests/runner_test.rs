#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
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

/// A test buffer that implements Write and allows reading back contents.
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

/// Read the test buffer as a string.
fn read_buf(buf: &Arc<Mutex<Vec<u8>>>) -> String {
    String::from_utf8_lossy(&buf.lock().unwrap()).into_owned()
}

/// Helper: parse a config string, validate, create OutputManager, Runner,
/// and a shutdown sender for test control.
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
        Runner::new(config, base_dir.join("don.toml"), PLATFORM, output_manager, base_dir.to_path_buf(), None, shutdown_rx)
            .await
            .unwrap();
    (runner, shutdown_tx, buf)
}

// --- Parallel executor test ---

#[test]
fn integration_parallel_services_start_concurrently() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("parallel-exec");

        let toml = ConfigBuilder::new()
            .add_custom_service("svc-a", "sleep", &["1"])
            .log("ignore")
            .done()
            .add_custom_service("svc-b", "sleep", &["1"])
            .log("ignore")
            .done()
            .add_custom_service("svc-c", "sleep", &["1"])
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let start = std::time::Instant::now();

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Give services time to start concurrently.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = shutdown_tx.send(()).await;

        handle.await.unwrap();
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_secs(5),
            "parallel start should complete quickly, took {elapsed:?}"
        );

        let output = read_buf(&buf);
        assert!(output.contains("starting svc-a"), "should start svc-a");
        assert!(output.contains("starting svc-b"), "should start svc-b");
        assert!(output.contains("starting svc-c"), "should start svc-c");
    });
}

// --- Dependency ordering test ---

#[test]
fn integration_dependency_ordering_a_before_b() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("dep-order");
        let port = free_port();

        let listen_script = dir.path().join("listen.sh");
        std::fs::write(
            &listen_script,
            format!(
                "#!/bin/sh\n\
                 exec python3 -c \"\nimport socket, time\ns = socket.socket()\ns.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\ns.bind(('127.0.0.1', {port}))\ns.listen(1)\nwhile True: time.sleep(60)\n\"\n"
            ),
        )
        .unwrap();
        std::fs::set_permissions(&listen_script, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();

        let toml = ConfigBuilder::new()
            .add_custom_service("svc-a", listen_script.to_str().unwrap(), &[])
            .log("ignore")
            .done()
            .add_custom_service("svc-b", "sleep", &["300"])
            .depends_on(&["svc-a"])
            .ready_tcp_with(&format!("127.0.0.1:{port}"), "200ms", 30)
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        tokio::time::sleep(Duration::from_secs(5)).await;
        let _ = shutdown_tx.send(()).await;

        handle.await.unwrap();
        let output = read_buf(&buf);

        let a_start = output.find("starting svc-a");
        let b_start = output.find("starting svc-b");

        assert!(a_start.is_some(), "svc-a should start: {output}");
        if let (Some(a), Some(b)) = (a_start, b_start) {
            assert!(a < b, "svc-a should start before svc-b: a at {a}, b at {b}");
        }
    });
}

// --- Task depends on service ---

#[test]
fn integration_task_depends_on_service() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-dep-svc");
        let port = free_port();

        let listen_script = dir.path().join("listen.sh");
        std::fs::write(
            &listen_script,
            format!(
                "#!/bin/sh\nexec python3 -c \"\nimport socket, time\ns = socket.socket()\ns.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\ns.bind(('127.0.0.1', {port}))\ns.listen(1)\nwhile True: time.sleep(60)\n\"\n"
            ),
        )
        .unwrap();
        std::fs::set_permissions(&listen_script, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();

        let toml = ConfigBuilder::new()
            .add_custom_service("mydb", listen_script.to_str().unwrap(), &[])
            .ready_tcp_with(&format!("127.0.0.1:{port}"), "200ms", 30)
            .log("ignore")
            .done()
            .add_task("migrate", "echo", &["migration done"])
            .depends_on(&["mydb"])
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        tokio::time::sleep(Duration::from_secs(5)).await;
        let _ = shutdown_tx.send(()).await;

        handle.await.unwrap();
        let output = read_buf(&buf);

        let svc_start = output.find("starting mydb");
        let task_run = output.find("running migrate");
        assert!(svc_start.is_some(), "mydb should start: {output}");
        assert!(task_run.is_some(), "migrate should run: {output}");

        if let (Some(s), Some(t)) = (svc_start, task_run) {
            assert!(s < t, "mydb should start before migrate runs");
        }
    });
}

// --- TCP ready check ---

#[test]
fn integration_tcp_ready_check() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("tcp-ready");
        let port = free_port();

        let listen_script = dir.path().join("listen.sh");
        std::fs::write(
            &listen_script,
            format!(
                "#!/bin/sh\nexec python3 -c \"\nimport socket, time\ns = socket.socket()\ns.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\ns.bind(('127.0.0.1', {port}))\ns.listen(1)\nwhile True: time.sleep(60)\n\"\n"
            ),
        )
        .unwrap();
        std::fs::set_permissions(&listen_script, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();

        let toml = ConfigBuilder::new()
            .add_custom_service("tcpsvc", listen_script.to_str().unwrap(), &[])
            .ready_tcp_with(&format!("127.0.0.1:{port}"), "200ms", 30)
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        tokio::time::sleep(Duration::from_secs(5)).await;
        let _ = shutdown_tx.send(()).await;

        handle.await.unwrap();
        let output = read_buf(&buf);

        assert!(
            output.contains("tcpsvc") && output.contains("ready"),
            "should show tcpsvc ready: {output}"
        );
    });
}

// --- Exec ready check ---

#[test]
fn integration_exec_ready_check_with_retries() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("exec-ready");

        let counter_file = dir.path().join("counter");
        std::fs::write(&counter_file, "0").unwrap();

        let check_script = dir.path().join("check.sh");
        std::fs::write(
            &check_script,
            format!(
                "#!/bin/sh\n\
                 COUNT=$(cat {})\n\
                 COUNT=$((COUNT + 1))\n\
                 echo $COUNT > {}\n\
                 if [ $COUNT -ge 3 ]; then exit 0; else exit 1; fi\n",
                counter_file.display(),
                counter_file.display(),
            ),
        )
        .unwrap();
        std::fs::set_permissions(&check_script, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();

        let toml = ConfigBuilder::new()
            .add_custom_service("execsvc", "sleep", &["300"])
            .ready_exec_with(check_script.to_str().unwrap(), &[], "200ms", 10)
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        tokio::time::sleep(Duration::from_secs(5)).await;
        let _ = shutdown_tx.send(()).await;

        handle.await.unwrap();
        let output = read_buf(&buf);

        assert!(
            output.contains("execsvc") && output.contains("ready"),
            "exec ready check should pass after retries: {output}"
        );

        let count: i32 = std::fs::read_to_string(&counter_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert!(count >= 3, "check should have run at least 3 times, ran {count}");
    });
}

// --- Ready check exhaustion ---

#[test]
fn integration_ready_check_exhausted() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("ready-exhausted");

        let toml = ConfigBuilder::new()
            .add_custom_service("badsvc", "sleep", &["300"])
            .ready_exec_with("false", &[], "100ms", 3)
            .log("ignore")
            .done()
            .add_custom_service("dependent", "sleep", &["300"])
            .depends_on(&["badsvc"])
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        tokio::time::sleep(Duration::from_secs(5)).await;
        let _ = shutdown_tx.send(()).await;

        handle.await.unwrap();
        let output = read_buf(&buf);

        assert!(
            output.contains("badsvc") && output.contains("retries"),
            "badsvc should show retry exhaustion: {output}"
        );
        assert!(
            output.contains("dependent") && output.contains("dependency failed"),
            "dependent should be skipped: {output}"
        );
    });
}

// --- Task with watch files: skip on no changes ---

#[test]
fn integration_task_watch_skip() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-watch-skip");

        std::fs::write(dir.path().join("data.sql"), "CREATE TABLE test;").unwrap();

        let task_state = don::TaskState::new(dir.path().join(".don").join("task-state"));
        let patterns = vec![format!("{}/*.sql", dir.path().display())];
        task_state
            .record_success("migrate", &patterns, None)
            .await
            .unwrap();

        let toml = ConfigBuilder::new()
            .add_task("migrate", "echo", &["running migration"])
            .watch(&[&format!("{}/*.sql", dir.path().display())])
            .done()
            .build();

        // Task-only config — runner exits on its own when no services remain.
        let (runner, _shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        runner.run().await.unwrap();

        let output = read_buf(&buf);
        assert!(
            output.contains("skipped (no changes)"),
            "task should be skipped when files unchanged: {output}"
        );
    });
}

// --- Task with watch files: run on changes ---

#[test]
fn integration_task_watch_run_on_change() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-watch-run");

        std::fs::write(dir.path().join("data.sql"), "CREATE TABLE test;").unwrap();

        let task_state = don::TaskState::new(dir.path().join(".don").join("task-state"));
        let patterns = vec![format!("{}/*.sql", dir.path().display())];
        task_state
            .record_success("migrate", &patterns, None)
            .await
            .unwrap();

        std::fs::write(dir.path().join("data.sql"), "CREATE TABLE test_v2;").unwrap();

        let toml = ConfigBuilder::new()
            .add_task("migrate", "echo", &["running migration"])
            .watch(&[&format!("{}/*.sql", dir.path().display())])
            .done()
            .build();

        let (runner, _shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        runner.run().await.unwrap();

        let output = read_buf(&buf);
        assert!(
            output.contains("running migrate") && !output.contains("skipped"),
            "task should run when files changed: {output}"
        );
        assert!(
            output.contains("migrate complete"),
            "task should complete: {output}"
        );
    });
}

// --- Task timeout ---

#[test]
fn integration_task_timeout() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-timeout");

        let toml = ConfigBuilder::new()
            .add_task("slow-task", "sleep", &["300"])
            .timeout("1s")
            .done()
            .build();

        let (runner, _shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let start = std::time::Instant::now();
        runner.run().await.unwrap();
        let elapsed = start.elapsed();

        let output = read_buf(&buf);
        assert!(
            output.contains("slow-task") && output.contains("failed"),
            "timed out task should be reported as failed: {output}"
        );

        assert!(
            elapsed < Duration::from_secs(10),
            "should have timed out quickly, took {elapsed:?}"
        );
    });
}

// --- HTTP ready check ---

#[test]
fn integration_http_ready_check() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("http-ready");
        let port = free_port();

        let server_script = dir.path().join("server.sh");
        std::fs::write(
            &server_script,
            format!(
                "#!/bin/sh\nexec python3 -c \"\nfrom http.server import HTTPServer, BaseHTTPRequestHandler\nclass H(BaseHTTPRequestHandler):\n    def do_GET(self):\n        self.send_response(200)\n        self.end_headers()\n        self.wfile.write(b'ok')\n    def log_message(self, format, *args): pass\nHTTPServer(('127.0.0.1', {port}), H).serve_forever()\n\"\n"
            ),
        )
        .unwrap();
        std::fs::set_permissions(&server_script, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();

        let toml = ConfigBuilder::new()
            .add_custom_service("httpsvc", server_script.to_str().unwrap(), &[])
            .ready_http_with(
                &format!("http://127.0.0.1:{port}/"),
                "200ms",
                30,
            )
            .log("ignore")
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        tokio::time::sleep(Duration::from_secs(5)).await;
        let _ = shutdown_tx.send(()).await;

        handle.await.unwrap();
        let output = read_buf(&buf);

        assert!(
            output.contains("httpsvc") && output.contains("ready"),
            "HTTP ready check should pass: {output}"
        );
    });
}

// --- Don PID file prevents double start ---

#[test]
fn integration_don_pid_file_prevents_double_start() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("don-pid-double");

        let toml = ConfigBuilder::new()
            .add_custom_service("svc", "sleep", &["1"])
            .log("ignore")
            .done()
            .build();

        let config: Config = toml.parse().unwrap();
        config.validate(PLATFORM).unwrap();

        let (writer1, _buf1) = TestBuffer::new();
        let output_manager = OutputManager::new(&[("svc", &LogConfig::Ignore)], writer1).await.unwrap();
        let (_shutdown_tx1, shutdown_rx1) = mpsc::channel(2);

        // First runner acquires the PID file.
        let _runner1 = Runner::new(
            config,
            dir.path().join("don.toml"),
            PLATFORM,
            output_manager,
            dir.path().to_path_buf(),
            None,
            shutdown_rx1,
        )
        .await
        .unwrap();

        // Second runner should fail — PID file is held.
        let config2: Config = toml.parse().unwrap();
        config2.validate(PLATFORM).unwrap();
        let (writer2, _buf2) = TestBuffer::new();
        let output_manager2 = OutputManager::new(&[("svc", &LogConfig::Ignore)], writer2).await.unwrap();
        let (_shutdown_tx2, shutdown_rx2) = mpsc::channel(2);
        let result = Runner::new(
            config2,
            dir.path().join("don.toml"),
            PLATFORM,
            output_manager2,
            dir.path().to_path_buf(),
            None,
            shutdown_rx2,
        )
        .await;

        assert!(result.is_err(), "second runner should fail");
        let err = match result {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("already running"),
            "error should mention already running: {err}"
        );
    });
}
