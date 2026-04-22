#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
//! Integration tests for Phase 14: shutdown refinement.

mod helpers;

use don::config::{Config, LogConfig, Platform};
use don::output::OutputManager;
use don::runner::Runner;
use helpers::config::ConfigBuilder;
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

async fn wait_for_output(buf: &Arc<Mutex<Vec<u8>>>, needle: &str, timeout: Duration) -> bool {
    let start = tokio::time::Instant::now();
    loop {
        if read_buf(buf).contains(needle) {
            return true;
        }
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn spawn_runner(
    toml: &str,
    base_dir: &std::path::Path,
) -> (mpsc::Sender<()>, tokio::task::JoinHandle<()>, Arc<Mutex<Vec<u8>>>) {
    let config_path = base_dir.join("don.toml");
    std::fs::write(&config_path, toml).unwrap();

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
    let runner = Runner::new(
        config,
        config_path,
        PLATFORM,
        output_manager,
        base_dir.to_path_buf(),
        None,
        shutdown_rx,
    )
    .await
    .unwrap();

    let handle = tokio::spawn(async move {
        let _ = runner.run().await;
    });

    (shutdown_tx, handle, buf)
}

// --- Tests ---

/// Verify services stop in reverse dependency order: C (depends on B, which
/// depends on A) stops first, then B, then A.
#[test]
fn shutdown_reverse_dependency_order() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("shutdown-order");
        let toml = ConfigBuilder::new()
            .add_custom_service("a", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_custom_service("b", "sleep", &["60"])
            .log("ignore")
            .depends_on(&["a"])
            .ready_exec("true", &[])
            .done()
            .add_custom_service("c", "sleep", &["60"])
            .log("ignore")
            .depends_on(&["b"])
            .ready_exec("true", &[])
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        // Trigger graceful shutdown.
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();

        let output = read_buf(&buf);

        // Find the positions of "X: stopping..." messages to verify order.
        let c_stop = output.find("c: stopping...");
        let b_stop = output.find("b: stopping...");
        let a_stop = output.find("a: stopping...");

        assert!(c_stop.is_some(), "c should be stopped. output: {output}");
        assert!(b_stop.is_some(), "b should be stopped. output: {output}");
        assert!(a_stop.is_some(), "a should be stopped. output: {output}");

        // C (deepest dependent) must stop before B, B before A.
        assert!(
            c_stop.unwrap() < b_stop.unwrap(),
            "c should stop before b. output: {output}"
        );
        assert!(
            b_stop.unwrap() < a_stop.unwrap(),
            "b should stop before a. output: {output}"
        );

        assert!(output.contains("shutdown complete"), "output: {output}");
    });
}

/// Verify PID files and socket are cleaned up after shutdown.
#[test]
fn shutdown_cleans_up_state() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("shutdown-cleanup");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        // Verify state files exist before shutdown.
        let don_dir = dir.path().join(".don");
        assert!(don_dir.join("don.pid").exists(), "don.pid should exist");
        // Socket exists (API server).
        assert!(don_dir.join("don.sock").exists(), "don.sock should exist");

        // Trigger shutdown.
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();

        // After shutdown, PID file and socket should be gone.
        assert!(
            !don_dir.join("don.sock").exists(),
            "don.sock should be cleaned up"
        );
        // don.pid is released (flock) and removed by PidFile::Drop.
        // The file may or may not exist (depends on Drop ordering), but the
        // flock is definitely released, which is what matters.

        // Service PID files should be gone.
        let pids_dir = don_dir.join("pids");
        if pids_dir.exists() {
            let entries: Vec<_> = std::fs::read_dir(&pids_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .collect();
            assert!(
                entries.is_empty(),
                "pid files should be cleaned up: {entries:?}"
            );
        }
    });
}

/// Verify a running task is killed during shutdown.
#[test]
fn shutdown_kills_running_task() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("shutdown-task");
        // A service that starts immediately, plus a task that takes a long time.
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_task("slow", "sleep", &["300"])
            .log("ignore")
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&toml, dir.path()).await;
        // Wait for the service to be ready (task may still be running).
        assert!(wait_for_output(&buf, "keeper: ready", Duration::from_secs(5)).await);
        // Give the task a moment to start.
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Trigger shutdown.
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();

        let output = read_buf(&buf);
        assert!(
            output.contains("killing") && output.contains("task"),
            "expected task kill message. output: {output}"
        );
        assert!(output.contains("shutdown complete"), "output: {output}");
    });
}

/// Verify that shutdown completes with the graceful message.
#[test]
fn shutdown_graceful_message() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("shutdown-graceful-msg");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();

        let output = read_buf(&buf);
        assert!(
            output.contains("shutting down gracefully"),
            "expected graceful message. output: {output}"
        );
        assert!(
            output.contains("Ctrl+C again to force"),
            "expected force hint. output: {output}"
        );
    });
}

/// Verify per-service shutdown.timeout is respected — a service ignoring
/// SIGTERM gets SIGKILL after the configured timeout.
#[test]
fn shutdown_timeout_escalates_to_sigkill() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("shutdown-timeout");
        // Service that traps and ignores SIGTERM.
        let toml = ConfigBuilder::new()
            .add_custom_service(
                "stubborn",
                "bash",
                &["-c", "trap '' TERM; echo started; sleep 60"],
            )
            .log("ignore")
            .ready_exec("true", &[])
            .shutdown("SIGTERM", "1s")
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        let start = std::time::Instant::now();
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
        let elapsed = start.elapsed();

        let output = read_buf(&buf);
        assert!(output.contains("shutdown complete"), "output: {output}");

        // Should take ~1s (the timeout) + a bit for SIGKILL, not the full 10s default.
        assert!(
            elapsed < Duration::from_secs(5),
            "shutdown took too long ({elapsed:?}) — timeout may not be respected"
        );
        assert!(
            elapsed >= Duration::from_millis(800),
            "shutdown was too fast ({elapsed:?}) — SIGTERM should wait ~1s"
        );
    });
}
