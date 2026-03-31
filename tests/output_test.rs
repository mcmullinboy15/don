mod helpers;

use don::config::LogConfig;
use don::output::OutputManager;
use don::process::{spawn_process, SpawnConfig};
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use std::time::Duration;

/// Helper to build a SpawnConfig for a simple command.
fn basic_config<'a>(cmd: &'a str, args: &'a [String], force_pipe: bool) -> SpawnConfig<'a> {
    SpawnConfig {
        cmd,
        args,
        dir: None,
        env: std::env::vars().collect(),
        pid_file_path: None,
        force_pipe,
    }
}

/// Check if a `Vec<&[u8]>` contains a given byte slice.
fn lines_contain(lines: &[&[u8]], needle: &[u8]) -> bool {
    lines.iter().any(|l| *l == needle)
}

/// Check if any line in a `Vec<&[u8]>` contains a byte subsequence.
fn any_line_contains(lines: &[&[u8]], needle: &[u8]) -> bool {
    lines
        .iter()
        .any(|l| l.windows(needle.len()).any(|w| w == needle))
}

#[test]
fn integration_service_output_prefixed() {
    run_with_timeout(Duration::from_secs(10), async {
        let args = [
            "-e".to_string(),
            "line one\nline two\nline three".to_string(),
        ];
        let mut handle = spawn_process(basic_config("echo", &args, true)).unwrap();
        let output = handle.take_output().unwrap();

        let config = LogConfig::Stdout;
        let mut mgr = OutputManager::new(&[("myservice", &config)]).await.unwrap();
        let mut svc = mgr.take_service_output("myservice").unwrap();
        let mut stdout = Vec::new();

        svc.process_stream(output, &mut stdout).await.unwrap();

        let output_str = String::from_utf8_lossy(&stdout);

        // Each line should be prefixed with the service name.
        for expected in &["line one", "line two", "line three"] {
            assert!(
                output_str.contains(expected),
                "output should contain {expected:?}, got: {output_str:?}"
            );
        }

        assert!(
            output_str.contains("myservice"),
            "output should contain the service name prefix"
        );

        // Ring buffer should have the lines too.
        let lines = svc.ring_buffer().last_n(10);
        assert!(lines.len() >= 3, "ring buffer should have at least 3 lines");
        assert!(lines_contain(&lines, b"line one"));
        assert!(lines_contain(&lines, b"line two"));
        assert!(lines_contain(&lines, b"line three"));
    });
}

#[test]
fn integration_service_log_ignore_suppresses_stdout_but_feeds_ring_buffer() {
    run_with_timeout(Duration::from_secs(10), async {
        let args = [
            "-e".to_string(),
            "secret output\nanother line".to_string(),
        ];
        let mut handle = spawn_process(basic_config("echo", &args, true)).unwrap();
        let output = handle.take_output().unwrap();

        let config = LogConfig::Ignore;
        let mut mgr = OutputManager::new(&[("quiet", &config)]).await.unwrap();
        let mut svc = mgr.take_service_output("quiet").unwrap();
        let mut stdout = Vec::new();

        svc.process_stream(output, &mut stdout).await.unwrap();

        assert!(
            stdout.is_empty(),
            "ignore mode should not write to stdout, got {} bytes",
            stdout.len()
        );

        let lines = svc.ring_buffer().last_n(10);
        assert!(
            lines_contain(&lines, b"secret output"),
            "ring buffer should have the output even in ignore mode: {lines:?}"
        );
        assert!(
            lines_contain(&lines, b"another line"),
            "ring buffer should have all lines: {lines:?}"
        );
    });
}

#[test]
fn integration_service_log_file_writes_raw() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("output-file-test");
        let log_path = dir.path().join("service.log");

        let args = [
            "-e".to_string(),
            "file line 1\nfile line 2".to_string(),
        ];
        let mut handle = spawn_process(basic_config("echo", &args, true)).unwrap();
        let output = handle.take_output().unwrap();

        let config = LogConfig::File(log_path.clone());
        let mut mgr = OutputManager::new(&[("filesvc", &config)]).await.unwrap();
        let mut svc = mgr.take_service_output("filesvc").unwrap();
        let mut stdout = Vec::new();

        svc.process_stream(output, &mut stdout).await.unwrap();

        assert!(stdout.is_empty(), "file mode should not write to stdout");

        let file_content = std::fs::read_to_string(&log_path).unwrap();
        assert!(
            file_content.contains("file line 1"),
            "file should contain raw output"
        );
        assert!(
            !file_content.contains("filesvc"),
            "file should NOT contain the prefix"
        );

        assert!(lines_contain(&svc.ring_buffer().last_n(10), b"file line 1"));
    });
}

#[test]
fn integration_concurrent_service_outputs() {
    run_with_timeout(Duration::from_secs(10), async {
        let config_a = LogConfig::Stdout;
        let config_b = LogConfig::Stdout;
        let mut mgr =
            OutputManager::new(&[("alpha", &config_a), ("beta", &config_b)]).await.unwrap();

        let mut alpha = mgr.take_service_output("alpha").unwrap();
        let mut beta = mgr.take_service_output("beta").unwrap();

        let args_a = [
            "-e".to_string(),
            "alpha line 1\nalpha line 2\nalpha line 3".to_string(),
        ];
        let args_b = [
            "-e".to_string(),
            "beta line 1\nbeta line 2\nbeta line 3".to_string(),
        ];
        let mut handle_a = spawn_process(basic_config("echo", &args_a, true)).unwrap();
        let mut handle_b = spawn_process(basic_config("echo", &args_b, true)).unwrap();

        let output_a = handle_a.take_output().unwrap();
        let output_b = handle_b.take_output().unwrap();

        let mut stdout_a = Vec::new();
        let mut stdout_b = Vec::new();

        let (r_a, r_b) = tokio::join!(
            alpha.process_stream(output_a, &mut stdout_a),
            beta.process_stream(output_b, &mut stdout_b),
        );
        r_a.unwrap();
        r_b.unwrap();

        let ring_a = alpha.ring_buffer().last_n(10);
        let ring_b = beta.ring_buffer().last_n(10);
        assert!(lines_contain(&ring_a, b"alpha line 1"), "alpha ring buffer: {ring_a:?}");
        assert!(lines_contain(&ring_b, b"beta line 1"), "beta ring buffer: {ring_b:?}");

        let out_a = String::from_utf8_lossy(&stdout_a);
        let out_b = String::from_utf8_lossy(&stdout_b);
        assert!(out_a.contains("alpha") && !out_a.contains("beta"),
            "alpha stdout should only have alpha output");
        assert!(out_b.contains("beta") && !out_b.contains("alpha"),
            "beta stdout should only have beta output");

        for (label, output) in [("alpha", &out_a), ("beta", &out_b)] {
            for line in output.lines() {
                assert!(
                    line.contains(" | "),
                    "{label}: every line should have prefix separator, got: {line:?}"
                );
            }
        }
    });
}

#[test]
fn integration_pty_mode_output_prefixed() {
    run_with_timeout(Duration::from_secs(10), async {
        let args = [
            "-e".to_string(),
            "pty line one\npty line two".to_string(),
        ];
        let mut handle = spawn_process(basic_config("echo", &args, false)).unwrap();
        let output = handle.take_output().unwrap();

        let config = LogConfig::Stdout;
        let mut mgr = OutputManager::new(&[("ptysvc", &config)]).await.unwrap();
        let mut svc = mgr.take_service_output("ptysvc").unwrap();
        let mut stdout = Vec::new();

        svc.process_stream(output, &mut stdout).await.unwrap();

        let lines = svc.ring_buffer().last_n(10);
        assert!(
            any_line_contains(&lines, b"pty line"),
            "PTY mode should capture output in ring buffer: {lines:?}"
        );

        let output_str = String::from_utf8_lossy(&stdout);
        assert!(
            output_str.contains("ptysvc"),
            "PTY mode output should contain the service prefix"
        );
    });
}
