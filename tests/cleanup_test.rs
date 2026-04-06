#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
//! Integration tests for Phase 12: stale state cleanup.

mod helpers;

use don::process::cleanup::run_cleanup;
use helpers::config::ConfigBuilder;
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use std::time::Duration;

fn docker_available() -> bool {
    std::env::var("DON_TEST_DOCKER").is_ok()
}

macro_rules! skip_unless_docker {
    () => {
        if !docker_available() {
            eprintln!("skipping: DON_TEST_DOCKER not set");
            return;
        }
    };
}

/// Write a pid file in the new (pgid, start_time) format.
fn write_pid_file(path: &std::path::Path, pgid: i32, start_time: u64) {
    std::fs::write(path, format!("{pgid}\n{start_time}")).unwrap();
}

#[test]
fn cleanup_kills_orphaned_process() {
    use std::os::unix::process::CommandExt;

    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("cleanup-kill");
        let base = dir.path();
        let pid_dir = base.join(".don").join("pids");
        std::fs::create_dir_all(&pid_dir).unwrap();

        // Spawn a background process in its own process group (setpgid)
        // so cleanup's killpg doesn't hit the test itself.
        let mut cmd = std::process::Command::new("sleep");
        cmd.arg("300")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        // Safety: setpgid is async-signal-safe.
        unsafe {
            cmd.pre_exec(|| {
                nix::unistd::setpgid(nix::unistd::Pid::from_raw(0), nix::unistd::Pid::from_raw(0))
                    .map_err(std::io::Error::other)?;
                Ok(())
            });
        }
        let mut child = cmd.spawn().unwrap();
        let pgid = child.id() as i32;

        // Read start_time and write the pid file in the new format.
        let ident = don::process::identity::capture(pgid).unwrap().unwrap();
        write_pid_file(&pid_dir.join("test-svc"), ident.pgid, ident.start_time);

        // Run cleanup ��� it should find the orphan, kill it, and remove the file.
        let report = run_cleanup(base, &[]).await;
        assert!(report.pids_killed >= 1, "report: {report}");
        assert!(report.pid_files_removed >= 1, "report: {report}");
        assert!(!pid_dir.join("test-svc").exists());

        // Reap the child so it doesn't become a zombie.
        let _ = child.wait();
    });
}

#[test]
fn cleanup_does_not_kill_on_starttime_mismatch() {
    run_with_timeout(Duration::from_secs(5), async {
        let dir = TempDir::new("cleanup-mismatch");
        let base = dir.path();
        let pid_dir = base.join(".don").join("pids");
        std::fs::create_dir_all(&pid_dir).unwrap();

        // Write a pid file pointing at our own PID but with a wrong start_time.
        let our_pid = std::process::id() as i32;
        write_pid_file(&pid_dir.join("stale-svc"), our_pid, 1);

        let report = run_cleanup(base, &[]).await;
        assert_eq!(report.pids_killed, 0, "should NOT kill — identity mismatch");
        assert_eq!(report.pid_files_removed, 1);
        assert!(!pid_dir.join("stale-svc").exists());
    });
}

#[test]
fn cleanup_removes_old_format_pid_file() {
    run_with_timeout(Duration::from_secs(5), async {
        let dir = TempDir::new("cleanup-old-format");
        let base = dir.path();
        let pid_dir = base.join(".don").join("pids");
        std::fs::create_dir_all(&pid_dir).unwrap();

        // Old format: just a PGID number, no start_time.
        std::fs::write(pid_dir.join("old-svc"), "99999").unwrap();

        let report = run_cleanup(base, &[]).await;
        assert_eq!(report.pids_killed, 0, "no kill for old format");
        assert_eq!(report.pid_files_removed, 1);
    });
}

#[test]
fn cleanup_removes_stale_socket() {
    run_with_timeout(Duration::from_secs(5), async {
        let dir = TempDir::new("cleanup-sock");
        let base = dir.path();
        let don_dir = base.join(".don");
        std::fs::create_dir_all(&don_dir).unwrap();

        // Create a socket file with nobody listening.
        let sock_path = don_dir.join("don.sock");
        let listener = std::os::unix::net::UnixListener::bind(&sock_path).unwrap();
        drop(listener); // Close listener — file remains, connections refused.

        let report = run_cleanup(base, &[]).await;
        assert!(report.sock_removed, "stale socket should be removed");
        assert!(!sock_path.exists());
    });
}

#[test]
fn cleanup_leaves_live_socket_alone() {
    run_with_timeout(Duration::from_secs(5), async {
        let dir = TempDir::new("cleanup-live-sock");
        let base = dir.path();
        let don_dir = base.join(".don");
        std::fs::create_dir_all(&don_dir).unwrap();

        let sock_path = don_dir.join("don.sock");
        let _listener = std::os::unix::net::UnixListener::bind(&sock_path).unwrap();
        // Listener is still alive — socket is NOT stale.

        let report = run_cleanup(base, &[]).await;
        assert!(!report.sock_removed, "live socket should not be removed");
        assert!(sock_path.exists());
    });
}

#[test]
fn cleanup_multiple_pid_files() {
    run_with_timeout(Duration::from_secs(5), async {
        let dir = TempDir::new("cleanup-multi");
        let base = dir.path();
        let pid_dir = base.join(".don").join("pids");
        std::fs::create_dir_all(&pid_dir).unwrap();

        write_pid_file(&pid_dir.join("svc-a"), 999_999, 1);
        write_pid_file(&pid_dir.join("svc-b"), 999_998, 1);
        std::fs::write(pid_dir.join("svc-c"), "12345").unwrap();

        let report = run_cleanup(base, &[]).await;
        assert_eq!(report.pid_files_removed, 3);
        assert_eq!(report.pids_killed, 0, "none should be alive");
    });
}

#[test]
fn cleanup_no_stale_state() {
    run_with_timeout(Duration::from_secs(5), async {
        let dir = TempDir::new("cleanup-none");
        let base = dir.path();
        // Don't create .don/ at all.

        let report = run_cleanup(base, &[]).await;
        assert_eq!(report.pid_files_removed, 0);
        assert!(!report.sock_removed);
        assert_eq!(report.containers_removed, 0);
    });
}

#[test]
fn cleanup_cli_no_stale_state() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("cleanup-cli");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .done()
            .build();
        let config_path = dir.path().join("don.toml");
        std::fs::write(&config_path, &toml).unwrap();

        let output = tokio::task::spawn_blocking(move || {
            std::process::Command::new(env!("CARGO_BIN_EXE_don"))
                .args(["--config", config_path.to_str().unwrap(), "cleanup"])
                .output()
                .unwrap()
        })
        .await
        .unwrap();

        assert!(
            output.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains("no stale state"),
            "stdout: {stdout}"
        );
    });
}

// --- Docker-gated tests ---

/// Pre-create an orphaned container (simulating a don crash that left it
/// behind), then run cleanup with that container name. Verify it's removed.
#[test]
fn cleanup_removes_orphaned_docker_container() {
    skip_unless_docker!();

    run_with_timeout(Duration::from_secs(30), async {
        let container_name = "don-cleanup-test-orphan";

        // Ensure clean slate.
        let docker = bollard::Docker::connect_with_socket_defaults().unwrap();
        let _ = docker
            .remove_container(
                container_name,
                Some(
                    bollard::query_parameters::RemoveContainerOptionsBuilder::new()
                        .force(true)
                        .build(),
                ),
            )
            .await;

        // Create an orphaned container (stopped, but not removed).
        docker
            .create_container(
                Some(
                    bollard::query_parameters::CreateContainerOptionsBuilder::new()
                        .name(container_name)
                        .build(),
                ),
                bollard::models::ContainerCreateBody {
                    image: Some("alpine:latest".to_string()),
                    cmd: Some(vec!["echo".to_string(), "orphan".to_string()]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Verify it exists.
        assert!(
            docker.inspect_container(container_name, None).await.is_ok(),
            "container should exist before cleanup"
        );

        // Run cleanup with this container name in the docker list.
        let dir = TempDir::new("cleanup-docker");
        let report = run_cleanup(dir.path(), &[container_name.to_string()]).await;
        assert_eq!(report.containers_removed, 1, "report: {report}");

        // Verify it's gone.
        let inspect = docker.inspect_container(container_name, None).await;
        assert!(
            inspect.is_err(),
            "container should be removed after cleanup"
        );
    });
}
