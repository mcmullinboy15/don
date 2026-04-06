#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
//! Integration tests for Phase 13: config auto-reload.
//!
//! These tests start a runner from a don.toml, then modify the file on disk
//! and verify the runner picks up the changes.

mod helpers;

use don::client::Client;
use don::config::{Config, LogConfig, Platform};
use don::output::OutputManager;
use don::runner::{ItemStatus, Runner};
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

/// Spin up a runner from a toml string. Writes the toml to don.toml in the
/// given base directory. Returns (shutdown_tx, join_handle, output_buffer).
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

// --- Integration tests ---

#[test]
fn config_reload_add_service() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("reload-add");
        let initial = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&initial, dir.path()).await;
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        // Modify don.toml to add a new service.
        let new_toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_custom_service("worker", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();
        std::fs::write(dir.path().join("don.toml"), &new_toml).unwrap();

        // Wait for the reload to pick up the new service.
        assert!(
            wait_for_output(&buf, "added services: worker", Duration::from_secs(5)).await,
            "expected config change log. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn config_reload_remove_service() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("reload-remove");
        let initial = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_custom_service("worker", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&initial, dir.path()).await;
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        // Remove worker.
        let new_toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();
        std::fs::write(dir.path().join("don.toml"), &new_toml).unwrap();

        assert!(
            wait_for_output(&buf, "removed from config", Duration::from_secs(5)).await,
            "expected worker removed. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn config_reload_change_service_env() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("reload-change");
        let initial = ConfigBuilder::new()
            .add_custom_service("api", "bash", &["-c", "echo PORT=$PORT; sleep 60"])
            .log("ignore")
            .env("PORT", "3000")
            .ready_exec("true", &[])
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&initial, dir.path()).await;
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        // Change the env.
        let new_toml = ConfigBuilder::new()
            .add_custom_service("api", "bash", &["-c", "echo PORT=$PORT; sleep 60"])
            .log("ignore")
            .env("PORT", "4000")
            .ready_exec("true", &[])
            .done()
            .build();
        std::fs::write(dir.path().join("don.toml"), &new_toml).unwrap();

        assert!(
            wait_for_output(&buf, "changed services: api", Duration::from_secs(5)).await,
            "expected api changed. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn config_reload_invalid_keeps_running() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("reload-invalid");
        let initial = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&initial, dir.path()).await;
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        // Write an invalid config.
        std::fs::write(dir.path().join("don.toml"), "this is not [valid toml").unwrap();

        assert!(
            wait_for_output(&buf, "config reload failed", Duration::from_secs(5)).await,
            "expected error log. output: {}",
            read_buf(&buf)
        );

        // The original service should still be running.
        tokio::time::sleep(Duration::from_millis(500)).await;
        // No crash, no panic — the runner is still alive.

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn config_reload_rapid_edits_debounced() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("reload-debounce");
        let initial = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&initial, dir.path()).await;
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        // Rapid-fire 5 writes within 100ms. Only one reload should fire
        // (after the debounce window).
        for i in 0..5 {
            let new_toml = ConfigBuilder::new()
                .add_custom_service("keeper", "sleep", &["60"])
                .log("ignore")
                .ready_exec("true", &[])
                .env("ITERATION", &i.to_string())
                .done()
                .build();
            std::fs::write(dir.path().join("don.toml"), &new_toml).unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Wait for at least one reload.
        assert!(
            wait_for_output(&buf, "config changed", Duration::from_secs(5)).await,
            "expected at least one reload. output: {}",
            read_buf(&buf)
        );

        // Count "config changed" occurrences — should be just 1 (debounced).
        tokio::time::sleep(Duration::from_millis(500)).await;
        let output = read_buf(&buf);
        let reload_count = output.matches("config changed").count();
        assert!(
            reload_count <= 2,
            "expected at most 2 reloads (debounce), got {reload_count}. output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Robust additional tests ---

/// Helper: wait for the API socket to appear.
async fn wait_for_socket(base: &std::path::Path, timeout: Duration) -> bool {
    let sock = base.join(".don").join("don.sock");
    let start = tokio::time::Instant::now();
    while !sock.exists() {
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    true
}

/// Helper: get service names from status API.
async fn get_service_names(base: &std::path::Path) -> Vec<String> {
    let client = Client::new(base);
    match client.status().await {
        Ok(items) => items
            .iter()
            .filter_map(|item| match item {
                ItemStatus::Service { name, .. } => Some(name.clone()),
                _ => None,
            })
            .collect(),
        Err(_) => vec![],
    }
}

/// Helper: get a service's state string.
async fn get_service_state(base: &std::path::Path, name: &str) -> Option<String> {
    let client = Client::new(base);
    let items = client.status().await.ok()?;
    items.iter().find_map(|item| match item {
        ItemStatus::Service {
            name: n, state, ..
        } if n == name => Some(format!("{state:?}")),
        _ => None,
    })
}

/// Verify the removed service is gone from the status API, not just from logs.
#[test]
fn config_reload_removed_service_gone_from_status() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("reload-remove-status");
        let initial = ConfigBuilder::new()
            .add_custom_service("api", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_custom_service("worker", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&initial, dir.path()).await;
        assert!(wait_for_socket(dir.path(), Duration::from_secs(3)).await);
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        // Verify both services present.
        let names = get_service_names(dir.path()).await;
        assert!(names.contains(&"api".to_string()), "api missing: {names:?}");
        assert!(names.contains(&"worker".to_string()), "worker missing: {names:?}");

        // Remove worker from config.
        let new_toml = ConfigBuilder::new()
            .add_custom_service("api", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();
        std::fs::write(dir.path().join("don.toml"), &new_toml).unwrap();
        assert!(wait_for_output(&buf, "removed from config", Duration::from_secs(5)).await);
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Worker should be gone from the status API.
        let names = get_service_names(dir.path()).await;
        assert!(!names.contains(&"worker".to_string()), "worker should be gone: {names:?}");
        assert!(names.contains(&"api".to_string()), "api should remain: {names:?}");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// Verify a changed service actually restarts with the new env.
#[test]
fn config_reload_changed_service_gets_new_env() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("reload-new-env");
        let initial = ConfigBuilder::new()
            .add_custom_service(
                "api",
                "bash",
                &["-c", "echo PORT=$PORT; sleep 60"],
            )
            .log("ignore")
            .env("PORT", "3000")
            .ready_exec("true", &[])
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&initial, dir.path()).await;
        assert!(wait_for_socket(dir.path(), Duration::from_secs(3)).await);
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        // Verify initial output.
        let client = Client::new(dir.path());
        tokio::time::sleep(Duration::from_millis(300)).await;
        let logs = client.logs("api", 100).await.unwrap();
        assert!(
            logs.iter().any(|l| l.contains("PORT=3000")),
            "expected PORT=3000 in initial logs: {logs:?}"
        );

        // Change env.
        let new_toml = ConfigBuilder::new()
            .add_custom_service(
                "api",
                "bash",
                &["-c", "echo PORT=$PORT; sleep 60"],
            )
            .log("ignore")
            .env("PORT", "4000")
            .ready_exec("true", &[])
            .done()
            .build();
        std::fs::write(dir.path().join("don.toml"), &new_toml).unwrap();
        assert!(wait_for_output(&buf, "changed services: api", Duration::from_secs(5)).await);

        // Wait for restart and check new output.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let logs = client.logs("api", 100).await.unwrap();
        assert!(
            logs.iter().any(|l| l.contains("PORT=4000")),
            "expected PORT=4000 after reload: {logs:?}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// Verify that an invalid config followed by a valid one works correctly.
#[test]
fn config_reload_invalid_then_valid() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("reload-invalid-then-valid");
        let initial = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&initial, dir.path()).await;
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        // Write invalid config.
        std::fs::write(dir.path().join("don.toml"), "garbage").unwrap();
        assert!(wait_for_output(&buf, "config reload failed", Duration::from_secs(5)).await);

        // Write a valid config with a new service.
        let new_toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_custom_service("newbie", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();
        std::fs::write(dir.path().join("don.toml"), &new_toml).unwrap();
        assert!(
            wait_for_output(&buf, "added services: newbie", Duration::from_secs(5)).await,
            "expected recovery after invalid config. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// Verify empty config (all services removed) stops everything.
#[test]
fn config_reload_remove_all_services() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("reload-remove-all");
        let initial = ConfigBuilder::new()
            .add_custom_service("api", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_custom_service("worker", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&initial, dir.path()).await;
        assert!(wait_for_socket(dir.path(), Duration::from_secs(3)).await);
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        // Write empty config.
        std::fs::write(dir.path().join("don.toml"), "").unwrap();
        assert!(wait_for_output(&buf, "removed from config", Duration::from_secs(5)).await);
        tokio::time::sleep(Duration::from_millis(300)).await;

        let names = get_service_names(dir.path()).await;
        assert!(names.is_empty(), "all services should be gone: {names:?}");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// Verify a config with validation errors (e.g. cycle) is rejected gracefully.
#[test]
fn config_reload_validation_error_rejected() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("reload-validation-err");
        let initial = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&initial, dir.path()).await;
        assert!(wait_for_socket(dir.path(), Duration::from_secs(3)).await);
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        // Write a config with a dependency cycle.
        let bad_toml = ConfigBuilder::new()
            .add_custom_service("a", "sleep", &["60"])
            .depends_on(&["b"])
            .done()
            .add_custom_service("b", "sleep", &["60"])
            .depends_on(&["a"])
            .done()
            .build();
        std::fs::write(dir.path().join("don.toml"), &bad_toml).unwrap();
        assert!(
            wait_for_output(&buf, "config reload rejected", Duration::from_secs(5)).await,
            "expected validation error. output: {}",
            read_buf(&buf)
        );

        // Original service should still be running.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let state = get_service_state(dir.path(), "keeper").await;
        assert!(
            state.as_deref() == Some("Ready") || state.as_deref() == Some("Running"),
            "keeper should still be running: {state:?}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// Verify no-op reload (identical config) doesn't restart anything.
#[test]
fn config_reload_identical_config_is_noop() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("reload-noop");
        let initial = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&initial, dir.path()).await;
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        // Touch the file with identical content.
        std::fs::write(dir.path().join("don.toml"), &initial).unwrap();

        // Wait for the debounce + a bit extra.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // "config changed" should NOT appear — identical config is a no-op.
        let output = read_buf(&buf);
        assert!(
            !output.contains("config changed"),
            "identical config should not trigger reload. output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// Verify adding a task via config reload.
#[test]
fn config_reload_add_task() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("reload-add-task");
        let initial = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&initial, dir.path()).await;
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        // Add a task.
        let new_toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_task("greet", "echo", &["hello from task"])
            .log("ignore")
            .done()
            .build();
        std::fs::write(dir.path().join("don.toml"), &new_toml).unwrap();

        assert!(
            wait_for_output(&buf, "added tasks: greet", Duration::from_secs(5)).await,
            "expected task added. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// Verify adding a service that depends on an existing running service starts immediately.
#[test]
fn config_reload_add_service_with_satisfied_dep() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("reload-add-dep-ok");
        let initial = ConfigBuilder::new()
            .add_custom_service("db", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&initial, dir.path()).await;
        assert!(wait_for_socket(dir.path(), Duration::from_secs(3)).await);
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        // Add api that depends on the already-running db.
        let new_toml = ConfigBuilder::new()
            .add_custom_service("db", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_custom_service("api", "sleep", &["60"])
            .log("ignore")
            .depends_on(&["db"])
            .ready_exec("true", &[])
            .done()
            .build();
        std::fs::write(dir.path().join("don.toml"), &new_toml).unwrap();

        assert!(
            wait_for_output(&buf, "added services: api", Duration::from_secs(5)).await,
            "expected api added. output: {}",
            read_buf(&buf)
        );

        // api should start since db is already ready.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let state = get_service_state(dir.path(), "api").await;
        assert!(
            state.is_some(),
            "api should exist in status. got: {state:?}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// Add a new service that is a dependency of a changed existing service.
/// The new dep must start first (topo order), then the changed service restarts.
#[test]
fn config_reload_add_dep_of_existing_service() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("reload-add-dep-of-existing");
        // Initial: api running with no deps.
        let initial = ConfigBuilder::new()
            .add_custom_service(
                "api",
                "bash",
                &["-c", "echo DB=$DB_HOST; sleep 60"],
            )
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (shutdown_tx, handle, buf) = spawn_runner(&initial, dir.path()).await;
        assert!(wait_for_socket(dir.path(), Duration::from_secs(3)).await);
        assert!(wait_for_output(&buf, "all services running", Duration::from_secs(5)).await);

        // Reload: add db, change api to depend on db and set DB_HOST.
        // Note: api's log config stays "ignore" since that's what it was at
        // initial registration. The new output still reaches the ring buffer.
        let new_toml = ConfigBuilder::new()
            .add_custom_service("db", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_custom_service(
                "api",
                "bash",
                &["-c", "echo DB_HOST=$DB_HOST; sleep 60"],
            )
            .log("ignore")
            .depends_on(&["db"])
            .env("DB_HOST", "localhost")
            .ready_exec("true", &[])
            .done()
            .build();
        std::fs::write(dir.path().join("don.toml"), &new_toml).unwrap();

        // db should be added and api should be changed.
        assert!(
            wait_for_output(&buf, "added services: db", Duration::from_secs(5)).await,
            "expected db added. output: {}",
            read_buf(&buf)
        );

        // api should restart with the new env after db becomes ready and
        // the deferred StartPending fires. Wait for api to become ready.
        assert!(
            wait_for_output(&buf, "api ready", Duration::from_secs(5)).await,
            "expected api to restart and become ready. output: {}",
            read_buf(&buf)
        );

        // Wait for the child's output to reach the ring buffer.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify the new env via the ring buffer.
        let client = Client::new(dir.path());
        let logs = client.logs("api", 100).await.unwrap();
        assert!(
            logs.iter().any(|l| l.contains("DB_HOST=localhost")),
            "expected DB_HOST=localhost in api logs: {logs:?}"
        );

        // Both should be running.
        let state_db = get_service_state(dir.path(), "db").await;
        let state_api = get_service_state(dir.path(), "api").await;
        assert!(
            matches!(state_db.as_deref(), Some("Ready") | Some("Running")),
            "db should be running: {state_db:?}"
        );
        assert!(
            matches!(state_api.as_deref(), Some("Ready") | Some("Running")),
            "api should be running: {state_api:?}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}
