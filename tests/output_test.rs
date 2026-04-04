mod helpers;

use don::config::LogConfig;
use don::output::OutputManager;
use don::process::{SpawnConfig, spawn_process};
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Helper to build a SpawnConfig for a simple command.
fn basic_config<'a>(cmd: &'a str, args: &'a [String], force_pipe: bool) -> SpawnConfig<'a> {
    SpawnConfig {
        cmd,
        args,
        dir: None,
        env: std::env::vars().collect(),
        pgid_file_path: None,
        force_pipe,
    }
}

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

/// Check if a `Bytes` blob contains a given byte slice as a substring.
fn logs_contain(logs: &bytes::Bytes, needle: &[u8]) -> bool {
    logs.windows(needle.len()).any(|w| w == needle)
}

#[test]
fn integration_service_output_prefixed() {
    run_with_timeout(Duration::from_secs(10), async {
        let args = [
            "-e".to_string(),
            "line one\nline two\nline three".to_string(),
        ];
        let (_handle, output) = spawn_process(basic_config("echo", &args, true))
            .await
            .unwrap();

        let (writer, buf) = TestBuffer::new();
        let config = LogConfig::Stdout;
        let mgr = OutputManager::new(&[("myservice", &config)], writer)
            .await
            .unwrap();
        let svc = mgr.service_writer("myservice").unwrap();

        svc.process_stream(output).await.unwrap();

        let logs = mgr.read_logs("myservice", 10).await.unwrap();
        assert!(
            logs_contain(&logs, b"line one"),
            "ring buffer missing 'line one'"
        );
        assert!(
            logs_contain(&logs, b"line two"),
            "ring buffer missing 'line two'"
        );
        assert!(
            logs_contain(&logs, b"line three"),
            "ring buffer missing 'line three'"
        );

        mgr.shutdown().await;

        let output_str = read_buf(&buf);
        assert!(
            output_str.contains("myservice"),
            "should contain service prefix"
        );
        for expected in &["line one", "line two", "line three"] {
            assert!(output_str.contains(expected), "should contain {expected:?}");
        }
    });
}

#[test]
fn integration_service_log_ignore_suppresses_stdout_but_feeds_ring_buffer() {
    run_with_timeout(Duration::from_secs(10), async {
        let args = ["-e".to_string(), "secret output\nanother line".to_string()];
        let (_handle, output) = spawn_process(basic_config("echo", &args, true))
            .await
            .unwrap();

        let (writer, buf) = TestBuffer::new();
        let config = LogConfig::Ignore;
        let mgr = OutputManager::new(&[("quiet", &config)], writer)
            .await
            .unwrap();
        let svc = mgr.service_writer("quiet").unwrap();

        svc.process_stream(output).await.unwrap();

        let logs = mgr.read_logs("quiet", 10).await.unwrap();
        assert!(
            logs_contain(&logs, b"secret output"),
            "ring buffer missing 'secret output'"
        );
        assert!(
            logs_contain(&logs, b"another line"),
            "ring buffer missing 'another line'"
        );

        mgr.shutdown().await;

        let output_str = read_buf(&buf);
        assert!(
            output_str.is_empty(),
            "ignore mode should not write to stdout"
        );
    });
}

#[test]
fn integration_service_log_file_writes_raw() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("output-file-test");
        let log_path = dir.path().join("service.log");

        let args = ["-e".to_string(), "file line 1\nfile line 2".to_string()];
        let (_handle, output) = spawn_process(basic_config("echo", &args, true))
            .await
            .unwrap();

        let (writer, buf) = TestBuffer::new();
        let config = LogConfig::File(log_path.clone());
        let mgr = OutputManager::new(&[("filesvc", &config)], writer)
            .await
            .unwrap();
        let svc = mgr.service_writer("filesvc").unwrap();

        svc.process_stream(output).await.unwrap();

        let logs = mgr.read_logs("filesvc", 10).await.unwrap();
        assert!(
            logs_contain(&logs, b"file line 1"),
            "ring buffer missing 'file line 1'"
        );

        mgr.shutdown().await;

        // Stdout should be empty (file mode).
        let output_str = read_buf(&buf);
        assert!(
            output_str.is_empty(),
            "file mode should not write to stdout"
        );

        // File should contain raw output without prefix.
        let file_content = std::fs::read_to_string(&log_path).unwrap();
        assert!(file_content.contains("file line 1"));
        assert!(
            !file_content.contains("filesvc"),
            "file should not contain prefix"
        );
    });
}

#[test]
fn integration_concurrent_service_outputs() {
    run_with_timeout(Duration::from_secs(10), async {
        let (writer, buf) = TestBuffer::new();
        let config_a = LogConfig::Stdout;
        let config_b = LogConfig::Stdout;
        let mgr = OutputManager::new(&[("alpha", &config_a), ("beta", &config_b)], writer)
            .await
            .unwrap();

        let alpha = mgr.service_writer("alpha").unwrap();
        let beta = mgr.service_writer("beta").unwrap();

        let args_a = ["-e".to_string(), "alpha line 1\nalpha line 2".to_string()];
        let args_b = ["-e".to_string(), "beta line 1\nbeta line 2".to_string()];
        let (_handle_a, output_a) = spawn_process(basic_config("echo", &args_a, true))
            .await
            .unwrap();
        let (_handle_b, output_b) = spawn_process(basic_config("echo", &args_b, true))
            .await
            .unwrap();

        let (r_a, r_b) = tokio::join!(
            alpha.process_stream(output_a),
            beta.process_stream(output_b),
        );
        r_a.unwrap();
        r_b.unwrap();

        let logs_a = mgr.read_logs("alpha", 10).await.unwrap();
        let logs_b = mgr.read_logs("beta", 10).await.unwrap();
        assert!(
            logs_contain(&logs_a, b"alpha line 1"),
            "ring buffer missing 'alpha line 1'"
        );
        assert!(
            logs_contain(&logs_b, b"beta line 1"),
            "ring buffer missing 'beta line 1'"
        );

        mgr.shutdown().await;

        let output_str = read_buf(&buf);
        assert!(output_str.contains("alpha"), "should have alpha output");
        assert!(output_str.contains("beta"), "should have beta output");
    });
}

#[test]
fn integration_pty_mode_output_prefixed() {
    run_with_timeout(Duration::from_secs(10), async {
        let args = ["-e".to_string(), "pty line one\npty line two".to_string()];
        let (_handle, output) = spawn_process(basic_config("echo", &args, false))
            .await
            .unwrap();

        let (writer, buf) = TestBuffer::new();
        let config = LogConfig::Stdout;
        let mgr = OutputManager::new(&[("ptysvc", &config)], writer)
            .await
            .unwrap();
        let svc = mgr.service_writer("ptysvc").unwrap();

        svc.process_stream(output).await.unwrap();

        let logs = mgr.read_logs("ptysvc", 10).await.unwrap();
        assert!(
            logs_contain(&logs, b"pty line"),
            "PTY mode should capture output: {logs:?}"
        );

        mgr.shutdown().await;

        let output_str = read_buf(&buf);
        assert!(
            output_str.contains("ptysvc"),
            "should contain service prefix"
        );
    });
}
