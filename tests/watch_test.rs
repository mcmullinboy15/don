#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
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
    let runner = Runner::new(
        config,
        PLATFORM,
        output_manager,
        base_dir.to_path_buf(),
        None,
        shutdown_rx,
    )
    .await
    .unwrap();
    (runner, shutdown_tx, buf)
}

/// Wait until the output buffer contains the given string, with a timeout.
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

// --- Integration test: service restarts on file change ---

#[test]
fn integration_service_restarts_on_file_change() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("watch-restart");

        // Create a watched file.
        let src_dir = dir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("main.rs"), "initial content").unwrap();

        // Service that prints its PID. When it restarts, a new PID appears.
        let toml = ConfigBuilder::new()
            .add_custom_service("api", "bash", &["-c", "echo PID=$$ && sleep 60"])
            .watch(&["src/**/*.rs"])
            .debounce("100ms")
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Wait for "all services running".
        assert!(
            wait_for_output(&buf, "all services running", Duration::from_secs(5)).await,
            "timed out waiting for services to start. output: {}",
            read_buf(&buf)
        );

        // Modify the watched file.
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(src_dir.join("main.rs"), "modified content").unwrap();

        // Wait for rebuild lifecycle event.
        assert!(
            wait_for_output(&buf, "rebuilding (file changed)", Duration::from_secs(5)).await,
            "timed out waiting for rebuild. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: build then restart on file change ---

#[test]
fn integration_build_then_restart_on_file_change() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("watch-build");

        let src_dir = dir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("app.rs"), "v1").unwrap();

        // Build writes a marker file, service reads it.
        let toml = ConfigBuilder::new()
            .add_custom_service(
                "api",
                "bash",
                &["-c", "cat built.txt 2>/dev/null; sleep 60"],
            )
            .build_cmd("bash", &["-c", "echo build-ran > built.txt"])
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
            wait_for_output(&buf, "all services running", Duration::from_secs(5)).await,
            "timed out waiting for services to start"
        );

        // Modify watched file to trigger rebuild.
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(src_dir.join("app.rs"), "v2").unwrap();

        // Should see rebuild lifecycle events (the initial build also ran,
        // so we check for "rebuilding" to confirm the file-watch triggered).
        assert!(
            wait_for_output(&buf, "rebuilding (file changed)", Duration::from_secs(5)).await,
            "timed out waiting for rebuild trigger. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: edit during build skips intermediate restart ---

#[test]
fn integration_edit_during_build_skips_intermediate_restart() {
    run_with_timeout(Duration::from_secs(20), async {
        let dir = TempDir::new("watch-build-stale");

        let src_dir = dir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("app.rs"), "v1").unwrap();

        let toml = ConfigBuilder::new()
            .add_custom_service("api", "bash", &["-c", "echo PID=$$ && sleep 60"])
            .build_cmd(
                "bash",
                &[
                    "-c",
                    "if [ -f slow-build ]; then sleep 1; rm -f slow-build; fi",
                ],
            )
            .watch(&["src/**/*.rs"])
            .debounce("100ms")
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "all services running", Duration::from_secs(5)).await,
            "timed out waiting for services to start. output: {}",
            read_buf(&buf)
        );

        std::fs::write(dir.path().join("slow-build"), "1").unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(src_dir.join("app.rs"), "v2").unwrap();

        assert!(
            wait_for_output(&buf, "rebuilding (file changed)", Duration::from_secs(5)).await,
            "timed out waiting for first rebuild. output: {}",
            read_buf(&buf)
        );

        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(src_dir.join("app.rs"), "v3").unwrap();

        let start = tokio::time::Instant::now();
        loop {
            let output = read_buf(&buf);
            let rebuilds = output.matches("rebuilding (file changed)").count();
            let restarts = output.matches("restarting...").count();
            let build_successes = output.matches("bash build succeeded").count();
            if rebuilds >= 2 && build_successes >= 3 {
                assert_eq!(
                    restarts, 1,
                    "expected only one restart after two rebuild cycles. output: {output}"
                );
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "timed out waiting for stale rebuild flow. output: {output}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        tokio::time::sleep(Duration::from_millis(300)).await;
        let output = read_buf(&buf);
        assert_eq!(
            output.matches("restarting...").count(),
            1,
            "unexpected extra restart after stale rebuild. output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: build failure keeps old process ---

#[test]
fn integration_build_failure_keeps_old_process() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("watch-build-fail");

        let src_dir = dir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("app.rs"), "v1").unwrap();

        // Build script: succeeds the first time (creates marker), fails on subsequent runs.
        let build_script_path = dir.path().join("build.sh");
        let marker = dir.path().join("build-done");
        std::fs::write(
            &build_script_path,
            format!(
                "#!/bin/bash\nif [ -f '{}' ]; then exit 1; fi\ntouch '{}'\n",
                marker.display(),
                marker.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(
            &build_script_path,
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .unwrap();

        let toml = ConfigBuilder::new()
            .add_custom_service("api", "bash", &["-c", "echo running && sleep 60"])
            .build_cmd(build_script_path.to_str().unwrap(), &[])
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
            wait_for_output(&buf, "all services running", Duration::from_secs(5)).await,
            "timed out waiting for services to start"
        );

        // Trigger a rebuild.
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(src_dir.join("app.rs"), "v2").unwrap();

        // Should see build failure.
        assert!(
            wait_for_output(&buf, "build failed", Duration::from_secs(5)).await,
            "timed out waiting for build failure. output: {}",
            read_buf(&buf)
        );

        // Old service is still running — no "restarted" or "stopped" message.
        let output = read_buf(&buf);
        assert!(
            !output.contains("restarted"),
            "service should not have restarted after build failure"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: rapid-fire changes result in one restart ---

#[test]
fn integration_rapid_fire_changes_one_restart() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("watch-rapid");

        let src_dir = dir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("main.rs"), "v0").unwrap();

        let toml = ConfigBuilder::new()
            .add_custom_service("api", "bash", &["-c", "sleep 60"])
            .watch(&["src/**/*.rs"])
            .debounce("300ms")
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "all services running", Duration::from_secs(5)).await,
            "timed out waiting for services to start"
        );

        // Fire 5 rapid changes with 50ms gaps (total 250ms < 300ms debounce).
        tokio::time::sleep(Duration::from_millis(200)).await;
        for i in 1..=5 {
            std::fs::write(src_dir.join("main.rs"), format!("v{i}")).unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Wait for the single rebuild.
        assert!(
            wait_for_output(&buf, "rebuilding (file changed)", Duration::from_secs(5)).await,
            "timed out waiting for rebuild. output: {}",
            read_buf(&buf)
        );

        // Give a moment for any extra rebuilds to appear.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Count how many times "rebuilding" appears — should be exactly 1.
        let output = read_buf(&buf);
        let rebuild_count = output.matches("rebuilding (file changed)").count();
        assert_eq!(
            rebuild_count, 1,
            "expected exactly 1 rebuild, got {rebuild_count}. output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: file edit during startup triggers rebuild ---

#[test]
fn integration_file_edit_during_startup_triggers_rebuild() {
    use std::os::unix::fs::PermissionsExt;

    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("watch-during-startup");

        let src_dir = dir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("main.rs"), "v1").unwrap();

        // Ready check script: succeeds only when a marker file exists.
        // This lets us control exactly when startup finishes.
        let ready_script = dir.path().join("ready.sh");
        std::fs::write(&ready_script, "#!/bin/bash\n[ -f \"$1\" ]").unwrap();
        std::fs::set_permissions(&ready_script, std::fs::Permissions::from_mode(0o755)).unwrap();

        let marker = dir.path().join("ready-marker");

        let toml = ConfigBuilder::new()
            .add_custom_service("api", "bash", &["-c", "sleep 60"])
            .watch(&["src/**/*.rs"])
            .debounce("100ms")
            .log("ignore")
            .ready_exec_with(
                ready_script.to_str().unwrap(),
                &[marker.to_str().unwrap()],
                "200ms",
                30,
            )
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Wait for the service to start (but not be ready yet).
        assert!(
            wait_for_output(&buf, "api: starting", Duration::from_secs(5)).await,
            "timed out waiting for service to start. output: {}",
            read_buf(&buf)
        );

        // Edit a watched file while the ready check is still retrying.
        tokio::time::sleep(Duration::from_millis(300)).await;
        std::fs::write(src_dir.join("main.rs"), "v2").unwrap();

        // Now let startup complete by creating the marker file.
        tokio::time::sleep(Duration::from_millis(300)).await;
        std::fs::write(&marker, "ready").unwrap();

        // The service should become ready, then the queued rebuild should fire.
        assert!(
            wait_for_output(&buf, "rebuilding (file changed)", Duration::from_secs(5)).await,
            "expected rebuild from edit during startup. output: {}",
            read_buf(&buf)
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: reload = false prevents file-watch restart ---

#[test]
fn integration_reload_false_skips_file_watch() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("watch-reload-false");

        let src_dir = dir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("main.rs"), "initial").unwrap();

        // Service with reload = false and explicit watch patterns.
        // Even though watch patterns are set, reload = false should prevent
        // don from setting up file watches entirely.
        let toml = ConfigBuilder::new()
            .add_custom_service("frontend", "bash", &["-c", "echo STARTED && sleep 60"])
            .watch(&["src/**/*.rs"])
            .debounce("100ms")
            .reload(false)
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "all services running", Duration::from_secs(5)).await,
            "timed out waiting for services to start. output: {}",
            read_buf(&buf)
        );

        // Modify the watched file — should NOT trigger a rebuild.
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(src_dir.join("main.rs"), "modified").unwrap();

        // Wait long enough for a rebuild to have been triggered (debounce + margin).
        tokio::time::sleep(Duration::from_millis(800)).await;

        let output = read_buf(&buf);
        assert!(
            !output.contains("rebuilding"),
            "reload = false should prevent rebuilds on file change. output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_global_watch_ignore_skips_service_rebuild() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("watch-global-ignore");

        let src_dir = dir.path().join("src");
        let generated_dir = src_dir.join("generated");
        std::fs::create_dir_all(&generated_dir).unwrap();
        std::fs::write(src_dir.join("main.rs"), "initial").unwrap();
        std::fs::write(generated_dir.join("schema.rs"), "initial").unwrap();

        let toml = ConfigBuilder::new()
            .watch_ignore(&["src/generated/**"])
            .add_custom_service("api", "bash", &["-c", "echo STARTED && sleep 60"])
            .watch(&["src/**/*.rs"])
            .debounce("100ms")
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        assert!(
            wait_for_output(&buf, "all services running", Duration::from_secs(5)).await,
            "timed out waiting for services to start. output: {}",
            read_buf(&buf)
        );

        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(generated_dir.join("schema.rs"), "modified").unwrap();
        tokio::time::sleep(Duration::from_millis(800)).await;

        let output = read_buf(&buf);
        assert!(
            !output.contains("rebuilding"),
            "global watch_ignore should prevent rebuilds on ignored file change. output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

// --- Integration test: task with auto_run=false skips initial run and goes pending on change ---

#[test]
fn integration_task_auto_run_false_skips_initial_and_goes_pending() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("watch-task-manual");

        let defs_dir = dir.path().join("definitions");
        std::fs::create_dir_all(&defs_dir).unwrap();
        let schema = defs_dir.join("users.sql");
        std::fs::write(&schema, "CREATE TABLE users (id INT);").unwrap();

        // A service to keep don running after the task completes,
        // plus a task with auto_run = false.
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "bash", &["-c", "sleep 60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_task("migrate", "echo", &["migrating"])
            .watch(&["definitions/**/*.sql"])
            .auto_run(false)
            .done()
            .build();

        let (runner, shutdown_tx, buf) = make_runner(&toml, dir.path()).await;

        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Task should NOT run at startup — auto_run = false means it starts
        // in PendingRun state immediately.
        assert!(
            wait_for_output(&buf, "pending — auto_run = false", Duration::from_secs(5)).await,
            "migrate should be pending at startup. output: {}",
            read_buf(&buf)
        );

        // Modify the watched file.
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(&schema, "CREATE TABLE users (id INT, name TEXT);").unwrap();

        // Should log pending again, NOT actually run the task.
        assert!(
            wait_for_output(&buf, "files changed (pending", Duration::from_secs(5)).await,
            "expected pending event on file change. output: {}",
            read_buf(&buf)
        );

        // Give any rogue run a chance to start.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let output = read_buf(&buf);
        assert_eq!(
            output.matches("migrate complete").count(),
            0,
            "migrate should never have run; output: {output}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}
