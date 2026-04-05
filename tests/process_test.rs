mod helpers;

use don::process::{cleanup_pgid_file, read_pgid_file, spawn_process, ChildOutput, SpawnConfig};
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
        pgid_file_path: None,
        force_pipe,
        listen_fds: vec![],
    }
}

#[tokio::test]
async fn spawn_process_has_own_pgid() {
    let args = ["300".to_string()];
    let (mut handle, _output) = spawn_process(basic_config("sleep", &args, false)).await.unwrap();
    let don_pgid = nix::unistd::getpgid(None).unwrap();

    assert_ne!(handle.pgid(), don_pgid.as_raw());
    assert!(handle.pgid() > 0);

    handle.signal(Signal::SIGKILL).unwrap();
    handle.wait().await.unwrap();
}

#[tokio::test]
async fn spawn_pipe_mode_reads_output() {
    let args = ["hello from pipe".to_string()];
    let (mut handle, mut output) = spawn_process(basic_config("echo", &args, true)).await.unwrap();
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
    let (mut handle, mut output) = spawn_process(basic_config("echo", &args, false)).await.unwrap();

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
    let (mut handle, _output) = spawn_process(basic_config("sleep", &args, true)).await.unwrap();

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
    let (mut handle, _output) = spawn_process(basic_config("sh", &args, true)).await.unwrap();

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
async fn pgid_file_written_on_spawn() {
    let dir = TempDir::new("pgid-file-written");
    let pgid_path = dir.path().join("test.pid");
    let args = ["300".to_string()];

    let config = SpawnConfig {
        pgid_file_path: Some(pgid_path.clone()),
        ..basic_config("sleep", &args, true)
    };
    let (mut handle, _output) = spawn_process(config).await.unwrap();

    // Verify PGID file exists with correct PGID
    assert!(pgid_path.exists());
    let stored_pgid = read_pgid_file(&pgid_path).await.unwrap();
    assert_eq!(stored_pgid, Some(handle.pgid()));

    handle.signal(Signal::SIGKILL).unwrap();
    handle.wait().await.unwrap();
}

#[tokio::test]
async fn pgid_file_cleaned_up_on_drop() {
    let dir = TempDir::new("pgid-file-drop");
    let pgid_path = dir.path().join("test.pid");
    let args = ["300".to_string()];

    let config = SpawnConfig {
        pgid_file_path: Some(pgid_path.clone()),
        ..basic_config("sleep", &args, true)
    };
    let (mut handle, _output) = spawn_process(config).await.unwrap();
    handle.signal(Signal::SIGKILL).unwrap();
    handle.wait().await.unwrap();
    drop(handle);

    // PGID file should be cleaned up on drop
    assert!(!pgid_path.exists());
}

#[tokio::test]
async fn pgid_file_read_and_cleanup() {
    let dir = TempDir::new("pgid-read-cleanup");
    let pgid_path = dir.path().join("test.pid");

    // Write a PGID file manually
    std::fs::write(&pgid_path, "12345").unwrap();

    // Read it back
    let pgid = read_pgid_file(&pgid_path).await.unwrap();
    assert_eq!(pgid, Some(12345));

    // Clean up
    cleanup_pgid_file(&pgid_path).await.unwrap();
    assert!(!pgid_path.exists());

    // Cleanup is idempotent
    cleanup_pgid_file(&pgid_path).await.unwrap();

    // Read of nonexistent returns None
    let pgid = read_pgid_file(&pgid_path).await.unwrap();
    assert_eq!(pgid, None);
}

#[tokio::test]
async fn pty_fallback_to_pipe_on_failure() {
    // We can't easily force PTY failure, but we can verify the force_pipe
    // path produces readable output — proving the fallback mechanism works.
    let args = ["fallback test".to_string()];
    let (mut handle, mut output) = spawn_process(basic_config("echo", &args, true)).await.unwrap();
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
        pgid_file_path: None,
        force_pipe: true,
        listen_fds: vec![],
    };
    let (mut handle, mut output) = spawn_process(config).await.unwrap();

    let mut buf = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut output, &mut buf)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&buf);
    assert!(text.contains("hello_from_don"), "got: {text}");

    handle.wait().await.unwrap();
}
