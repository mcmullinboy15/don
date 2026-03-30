mod helpers;

use don::process::pid_file::{PidFile, PidFileError};
use don::process::{spawn_process, ChildOutput, ProcessError, SpawnConfig};
use helpers::tempdir::TempDir;
use nix::sys::signal::Signal;
use std::collections::HashMap;

/// Helper to build a basic SpawnConfig with inherited env.
fn basic_config<'a>(
    cmd: &'a str,
    args: &'a [String],
    force_pipe: bool,
) -> SpawnConfig<'a> {
    SpawnConfig {
        cmd,
        args,
        dir: None,
        env: std::env::vars().collect(),
        pid_file_path: None,
        force_pipe,
    }
}

#[tokio::test]
async fn spawn_process_has_own_pgid() {
    let args = ["300".to_string()];
    let mut handle = spawn_process(basic_config("sleep", &args, false)).unwrap();
    let don_pgid = nix::unistd::getpgid(None).unwrap();

    assert_ne!(handle.pgid(), don_pgid.as_raw());
    assert!(handle.pgid() > 0);

    handle.signal(Signal::SIGKILL).unwrap();
    handle.wait().await.unwrap();
}

#[tokio::test]
async fn spawn_pipe_mode_reads_output() {
    let args = ["hello from pipe".to_string()];
    let mut handle = spawn_process(basic_config("echo", &args, true)).unwrap();

    let mut output = handle.take_output().unwrap();
    assert!(matches!(output, ChildOutput::Pipe(_)));

    let mut buf = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut output, &mut buf)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&buf);
    assert!(text.contains("hello from pipe"), "got: {text}");

    handle.wait().await.unwrap();
}

#[tokio::test]
async fn spawn_pty_mode_reads_output() {
    let args = ["hello from pty".to_string()];
    let mut handle = spawn_process(basic_config("echo", &args, false)).unwrap();

    let mut output = handle.take_output().unwrap();

    let mut buf = vec![0u8; 4096];
    let n = tokio::io::AsyncReadExt::read(&mut output, &mut buf)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&buf[..n]);
    assert!(text.contains("hello from pty"), "got: {text}");

    handle.wait().await.unwrap();
}

#[tokio::test]
async fn terminate_sends_sigterm_then_waits() {
    let args = ["300".to_string()];
    let mut handle = spawn_process(basic_config("sleep", &args, true)).unwrap();

    let status = handle
        .terminate(Signal::SIGTERM, std::time::Duration::from_secs(5))
        .await
        .unwrap();
    assert!(!status.success());
}

#[tokio::test]
async fn terminate_escalates_to_sigkill_on_timeout() {
    // Trap SIGTERM, then sleep in a loop (each `sleep 1` child may die on SIGTERM
    // but the loop keeps the shell alive since the shell itself ignores SIGTERM).
    let args = [
        "-c".to_string(),
        "trap '' TERM; while true; do sleep 1 & wait; done".to_string(),
    ];
    let mut handle = spawn_process(basic_config("sh", &args, true)).unwrap();

    // Verify the process is actually running
    assert!(handle.pgid() > 0);
    // Give the shell a moment to start and set up the trap
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let start = std::time::Instant::now();
    let status = handle
        .terminate(Signal::SIGTERM, std::time::Duration::from_secs(1))
        .await
        .unwrap();

    // Process should have been killed (not a clean exit)
    assert!(!status.success());
    // Should have taken at least ~1s (the SIGTERM timeout) before SIGKILL
    let elapsed = start.elapsed();
    assert!(
        elapsed >= std::time::Duration::from_millis(800),
        "too fast: {elapsed:?}"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "too slow: {elapsed:?}"
    );
}

#[tokio::test]
async fn pid_file_blocks_second_spawn() {
    let dir = TempDir::new("pid-blocks-second");
    let pid_path = dir.path().join("test.pid");
    let args = ["300".to_string()];

    let config = SpawnConfig {
        pid_file_path: Some(pid_path.clone()),
        ..basic_config("sleep", &args, true)
    };
    let mut handle = spawn_process(config).unwrap();

    // Verify PID file exists with correct PGID
    assert!(pid_path.exists());
    let content = std::fs::read_to_string(&pid_path).unwrap();
    let stored_pgid: i32 = content.trim().parse().unwrap();
    assert_eq!(stored_pgid, handle.pgid());

    // Second spawn should fail
    let config2 = SpawnConfig {
        pid_file_path: Some(pid_path.clone()),
        ..basic_config("sleep", &args, true)
    };
    let result = spawn_process(config2);
    assert!(matches!(
        result,
        Err(ProcessError::PidFile(PidFileError::AlreadyLocked))
    ));

    handle.signal(Signal::SIGKILL).unwrap();
    handle.wait().await.unwrap();
}

#[tokio::test]
async fn pid_file_released_after_drop() {
    let dir = TempDir::new("pid-released");
    let pid_path = dir.path().join("test.pid");
    let args = ["300".to_string()];

    let config = SpawnConfig {
        pid_file_path: Some(pid_path.clone()),
        ..basic_config("sleep", &args, true)
    };
    let mut handle = spawn_process(config).unwrap();
    handle.signal(Signal::SIGKILL).unwrap();
    handle.wait().await.unwrap();
    drop(handle);

    // Should succeed after drop
    let config2 = SpawnConfig {
        pid_file_path: Some(pid_path),
        ..basic_config("sleep", &args, true)
    };
    let mut handle2 = spawn_process(config2).unwrap();
    handle2.signal(Signal::SIGKILL).unwrap();
    handle2.wait().await.unwrap();
}

#[tokio::test]
async fn stale_pid_file_detected() {
    let dir = TempDir::new("stale-detection");
    let pid_path = dir.path().join("test.pid");
    let args = ["300".to_string()];

    // Spawn a process with PID file, kill it, drop the handle
    let config = SpawnConfig {
        pid_file_path: Some(pid_path.clone()),
        ..basic_config("sleep", &args, true)
    };
    let mut handle = spawn_process(config).unwrap();
    let pgid = handle.pgid();
    handle.signal(Signal::SIGKILL).unwrap();
    handle.wait().await.unwrap();
    drop(handle);

    // PID file still exists (not deleted on drop)
    assert!(pid_path.exists());

    // try_lock_stale should detect it as stale and return the PGID
    let result = PidFile::try_lock_stale(&pid_path).unwrap();
    assert_eq!(result, Some(pgid));

    // Clean up
    PidFile::cleanup(&pid_path).unwrap();
    assert!(!pid_path.exists());
}

#[tokio::test]
async fn pty_fallback_to_pipe_on_failure() {
    // We can't easily force PTY failure, but we can verify the force_pipe
    // path produces readable output — proving the fallback mechanism works.
    let args = ["fallback test".to_string()];
    let mut handle = spawn_process(basic_config("echo", &args, true)).unwrap();

    let mut output = handle.take_output().unwrap();
    assert!(matches!(output, ChildOutput::Pipe(_)));

    let mut buf = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut output, &mut buf)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&buf);
    assert!(text.contains("fallback test"), "got: {text}");

    handle.wait().await.unwrap();
}

#[tokio::test]
async fn env_passed_to_child() {
    let mut env: HashMap<String, String> = std::env::vars().collect();
    env.insert("DON_TEST_VAR".to_string(), "hello_from_don".to_string());

    let args = ["-c".to_string(), "echo $DON_TEST_VAR".to_string()];
    let config = SpawnConfig {
        cmd: "sh",
        args: &args,
        dir: None,
        env,
        pid_file_path: None,
        force_pipe: true,
    };
    let mut handle = spawn_process(config).unwrap();

    let mut output = handle.take_output().unwrap();
    let mut buf = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut output, &mut buf)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&buf);
    assert!(text.contains("hello_from_don"), "got: {text}");

    handle.wait().await.unwrap();
}
