//! End-to-end tests for task params: schema, CLI parsing, runner dispatch,
//! template substitution, and the file-watch gate that parks param'd tasks
//! at PendingRun.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod helpers;

use don::config::{Config, LogConfig, Platform};
use don::output::OutputManager;
use don::runner::{Runner, RunnerCommand, RunnerEvent, TaskItemState};
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot};

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

#[allow(dead_code)]
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
        base_dir.join("don.toml"),
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

/// Wait for a `TaskStateChanged` to `target` for `task_name`. Returns once
/// observed or the broadcast closes (returns `false`).
async fn wait_for_task_state(
    events: &mut broadcast::Receiver<RunnerEvent>,
    task_name: &str,
    target: TaskItemState,
) -> bool {
    loop {
        match events.recv().await {
            Ok(RunnerEvent::TaskStateChanged { name, state })
                if name == task_name && state == target =>
            {
                return true;
            }
            Ok(_) => continue,
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => return false,
        }
    }
}

/// Common setup: a "keeper" service that idles long enough for the test to
/// exercise commands, plus the task definition the test passes in.
fn toml_with_keeper(task_toml: &str) -> String {
    format!(
        r#"
[services.keeper]
run.cmd = "sleep"
run.args = ["60"]
log = "ignore"
ready.exec.cmd = "true"

{task_toml}
"#
    )
}

#[test]
fn integration_task_with_params_substitutes_values() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-params-substitute");
        let out_path = dir.path().join("captured.txt");

        // Use `sh -c` to write the substituted args + DON_PARAM_INDEX into a
        // file we can inspect after the task completes.
        let toml = toml_with_keeper(&format!(
            r#"
[tasks.sync]
cmd = "sh"
args = ["-c", "echo index={{{{index}}}} batch={{{{batch_size}}}} env=$DON_PARAM_INDEX > {}"]
log = "ignore"

[[tasks.sync.params]]
name = "index"
required = true

[[tasks.sync.params]]
name = "batch_size"
kind = "int"
default = "100"
"#,
            out_path.display()
        ));
        let (runner, shutdown_tx, _buf) = make_runner(&toml, dir.path()).await;
        let cmd_tx = runner.command_sender();
        let mut events = runner.subscribe();

        let runner_handle = tokio::spawn(async move { runner.run().await });
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Trigger the task with --index=users.
        let mut params = HashMap::new();
        params.insert("index".to_string(), "users".to_string());
        let (reply_tx, reply_rx) = oneshot::channel();
        cmd_tx
            .send(RunnerCommand::RunTask {
                name: "sync".to_string(),
                params,
                reply: reply_tx,
            })
            .await
            .unwrap();
        reply_rx.await.unwrap().unwrap();

        // Wait for completion.
        assert!(
            wait_for_task_state(&mut events, "sync", TaskItemState::Completed).await,
            "task didn't reach Completed"
        );

        // Verify substituted values landed in the output file.
        let captured = std::fs::read_to_string(&out_path).unwrap();
        assert!(
            captured.contains("index=users"),
            "missing index=users in {captured:?}"
        );
        assert!(
            captured.contains("batch=100"),
            "missing batch=100 (default) in {captured:?}"
        );
        assert!(
            captured.contains("env=users"),
            "missing DON_PARAM_INDEX export in {captured:?}"
        );

        let _ = shutdown_tx.send(()).await;
        let _ = runner_handle.await;
    });
}

#[test]
fn integration_task_without_params_keeps_working() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-params-backcompat");
        let out_path = dir.path().join("captured.txt");

        // auto_run = false so the task starts in PendingRun and we can drive
        // it explicitly via RunTask. With auto_run = true the task would
        // already have run by the time we send the command.
        let toml = toml_with_keeper(&format!(
            r#"
[tasks.plain]
cmd = "sh"
args = ["-c", "echo plain > {}"]
log = "ignore"
auto_run = false
"#,
            out_path.display()
        ));
        let (runner, shutdown_tx, _buf) = make_runner(&toml, dir.path()).await;
        let cmd_tx = runner.command_sender();
        let mut events = runner.subscribe();
        let runner_handle = tokio::spawn(async move { runner.run().await });

        // Wait until the keeper is up and the runner is processing commands.
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Empty params map mirrors what `dispatch_action` sends for plain
        // RunTask actions.
        let (reply_tx, reply_rx) = oneshot::channel();
        cmd_tx
            .send(RunnerCommand::RunTask {
                name: "plain".to_string(),
                params: HashMap::new(),
                reply: reply_tx,
            })
            .await
            .unwrap();
        reply_rx.await.unwrap().unwrap();

        assert!(
            wait_for_task_state(&mut events, "plain", TaskItemState::Completed).await,
            "task didn't complete"
        );
        let captured = std::fs::read_to_string(&out_path).unwrap();
        assert_eq!(captured.trim(), "plain");

        let _ = shutdown_tx.send(()).await;
        let _ = runner_handle.await;
    });
}

#[test]
fn integration_missing_required_param_errors() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-params-missing");

        let toml = toml_with_keeper(
            r#"
[tasks.sync]
cmd = "sh"
args = ["-c", "true"]
log = "ignore"

[[tasks.sync.params]]
name = "index"
required = true
"#,
        );
        let (runner, shutdown_tx, _buf) = make_runner(&toml, dir.path()).await;
        let cmd_tx = runner.command_sender();
        let runner_handle = tokio::spawn(async move { runner.run().await });
        tokio::time::sleep(Duration::from_millis(300)).await;

        let (reply_tx, reply_rx) = oneshot::channel();
        cmd_tx
            .send(RunnerCommand::RunTask {
                name: "sync".to_string(),
                params: HashMap::new(),
                reply: reply_tx,
            })
            .await
            .unwrap();
        let result = reply_rx.await.unwrap();
        match result {
            Err(don::runner::CommandError::InvalidParams { name, message }) => {
                assert_eq!(name, "sync");
                assert!(
                    message.contains("missing required"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("expected InvalidParams, got {other:?}"),
        }

        let _ = shutdown_tx.send(()).await;
        let _ = runner_handle.await;
    });
}

#[test]
fn integration_unknown_param_errors() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-params-unknown");

        let toml = toml_with_keeper(
            r#"
[tasks.sync]
cmd = "sh"
args = ["-c", "true"]
log = "ignore"

[[tasks.sync.params]]
name = "index"
"#,
        );
        let (runner, shutdown_tx, _buf) = make_runner(&toml, dir.path()).await;
        let cmd_tx = runner.command_sender();
        let runner_handle = tokio::spawn(async move { runner.run().await });
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut params = HashMap::new();
        params.insert("nope".to_string(), "x".to_string());
        let (reply_tx, reply_rx) = oneshot::channel();
        cmd_tx
            .send(RunnerCommand::RunTask {
                name: "sync".to_string(),
                params,
                reply: reply_tx,
            })
            .await
            .unwrap();
        let result = reply_rx.await.unwrap();
        match result {
            Err(don::runner::CommandError::InvalidParams { message, .. }) => {
                assert!(
                    message.contains("unknown param '--nope'"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("expected InvalidParams, got {other:?}"),
        }

        let _ = shutdown_tx.send(()).await;
        let _ = runner_handle.await;
    });
}

#[test]
fn integration_paramd_task_initial_state_is_pending_run() {
    // Param'd tasks must NOT auto-run at startup — the user has to trigger
    // them explicitly with values.
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-params-pending");

        let toml = toml_with_keeper(
            r#"
[tasks.interactive]
cmd = "sh"
args = ["-c", "true"]
log = "ignore"

[[tasks.interactive.params]]
name = "x"
"#,
        );
        let (runner, shutdown_tx, _buf) = make_runner(&toml, dir.path()).await;
        let mut events = runner.subscribe();
        let runner_handle = tokio::spawn(async move { runner.run().await });

        // The runner emits a TaskStateChanged for every task it considers at
        // startup. For our param'd task that should be PendingRun, never
        // Running/Completed without an explicit trigger.
        let mut saw_pending = false;
        let mut saw_run = false;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let timeout = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .unwrap_or(Duration::from_millis(0));
            match tokio::time::timeout(timeout, events.recv()).await {
                Ok(Ok(RunnerEvent::TaskStateChanged { name, state })) if name == "interactive" => {
                    match state {
                        TaskItemState::PendingRun => saw_pending = true,
                        TaskItemState::Running | TaskItemState::Completed => saw_run = true,
                        _ => {}
                    }
                }
                Ok(_) => continue,
                Err(_) => break,
            }
        }
        assert!(saw_pending, "expected PendingRun state for param'd task");
        assert!(!saw_run, "param'd task should NOT have auto-run at startup");

        let _ = shutdown_tx.send(()).await;
        let _ = runner_handle.await;
    });
}

#[test]
fn integration_completion_failure_writes_log() {
    // Resolving completions for a param whose command fails returns an
    // error AND writes a log file under .don/logs/completions/.
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-params-completion-fail");

        let toml = toml_with_keeper(
            r#"
[tasks.sync]
cmd = "sh"
args = ["-c", "true"]
log = "ignore"

[[tasks.sync.params]]
name = "index"

[tasks.sync.params.completions]
cmd = "false"
"#,
        );
        let (runner, shutdown_tx, _buf) = make_runner(&toml, dir.path()).await;
        let cmd_tx = runner.command_sender();
        let runner_handle = tokio::spawn(async move { runner.run().await });
        tokio::time::sleep(Duration::from_millis(300)).await;

        let (reply_tx, reply_rx) = oneshot::channel();
        cmd_tx
            .send(RunnerCommand::ResolveCompletions {
                task: "sync".to_string(),
                param: "index".to_string(),
                partial: HashMap::new(),
                force_refresh: false,
                reply: reply_tx,
            })
            .await
            .unwrap();
        let result = reply_rx.await.unwrap();
        let err = result.expect_err("completion command exits 1, should be an error");
        let log_path = err.log_path.expect("error should carry a log path");
        assert!(log_path.exists(), "log file should have been written");
        let body = std::fs::read_to_string(&log_path).unwrap();
        assert!(body.contains("task: sync"), "log missing task name: {body}");
        assert!(body.contains("param: index"), "log missing param name: {body}");

        let _ = shutdown_tx.send(()).await;
        let _ = runner_handle.await;
    });
}

#[test]
fn integration_static_choices_resolve_without_shelling_out() {
    // Static `choices` skip the shell-out path — the runner returns the
    // configured list directly.
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("task-params-static-choices");

        let toml = toml_with_keeper(
            r#"
[tasks.sync]
cmd = "sh"
args = ["-c", "true"]
log = "ignore"

[[tasks.sync.params]]
name = "env"
choices = ["dev", "staging", "prod"]
"#,
        );
        let (runner, shutdown_tx, _buf) = make_runner(&toml, dir.path()).await;
        let cmd_tx = runner.command_sender();
        let runner_handle = tokio::spawn(async move { runner.run().await });
        tokio::time::sleep(Duration::from_millis(300)).await;

        let (reply_tx, reply_rx) = oneshot::channel();
        cmd_tx
            .send(RunnerCommand::ResolveCompletions {
                task: "sync".to_string(),
                param: "env".to_string(),
                partial: HashMap::new(),
                force_refresh: false,
                reply: reply_tx,
            })
            .await
            .unwrap();
        let values = reply_rx.await.unwrap().unwrap();
        assert_eq!(values, vec!["dev", "staging", "prod"]);

        let _ = shutdown_tx.send(()).await;
        let _ = runner_handle.await;
    });
}
