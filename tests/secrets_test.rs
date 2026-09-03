#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod helpers;

use don::config::{Config, LogConfig, Platform};
use don::output::OutputManager;
use don::runner::Runner;
use helpers::config::ConfigBuilder;
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use std::os::unix::fs::PermissionsExt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

const PLATFORM: Platform = Platform::LinuxX86_64;

#[derive(Clone)]
struct TestBuffer(Arc<Mutex<Vec<u8>>>);

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

fn write_fake_key(dir: &std::path::Path) {
    let key = dir.join("key");
    std::fs::write(
        &key,
        r#"#!/usr/bin/env python3
import json
print(json.dumps({
    "STRIPE_SECRET_KEY": "injected-secret-value",
    "DD_API_KEY": "dd-api-key-value",
}))
"#,
    )
    .unwrap();
    std::fs::set_permissions(&key, PermissionsExt::from_mode(0o755)).unwrap();
}

#[test]
fn integration_declared_secrets_are_injected_stripped_and_redacted() {
    run_with_timeout(Duration::from_secs(20), async {
        let dir = TempDir::new("secrets-inject");
        let bin = dir.child("bin");
        std::fs::create_dir_all(&bin).unwrap();
        write_fake_key(&bin);

        std::fs::write(
            dir.child("key.toml"),
            r#"
provider = "aws-ssm"
[vars]
STRIPE_SECRET_KEY = "/app/StripeSecretKey"
DD_API_KEY = "/app/Datadog/ApiKey"
"#,
        )
        .unwrap();

        std::fs::write(
            dir.child("check.sh"),
            r#"#!/bin/sh
if [ "$STRIPE_SECRET_KEY" = "injected-secret-value" ]; then
  echo STRIPE=ok
else
  echo STRIPE=bad
fi
echo "DD=${DD_API_KEY:-empty}"
echo leaked=injected-secret-value
exec sleep 60
"#,
        )
        .unwrap();
        std::fs::set_permissions(dir.child("check.sh"), PermissionsExt::from_mode(0o755)).unwrap();

        let toml = ConfigBuilder::new()
            .raw("[secrets]\n")
            .add_custom_service("api", "./check.sh", &[])
            .secrets(&["STRIPE_SECRET_KEY"])
            .done()
            .build();
        std::fs::write(dir.child("don.toml"), &toml).unwrap();

        let original_path = std::env::var("PATH").unwrap();
        let new_path = format!("{}:{original_path}", bin.display());
        unsafe {
            std::env::set_var("PATH", &new_path);
            std::env::set_var("STRIPE_SECRET_KEY", "from-shell");
            std::env::set_var("DD_API_KEY", "from-shell");
        }

        let config = Config::from_file(&dir.child("don.toml")).unwrap();
        config.validate(PLATFORM).unwrap();
        let service_configs: Vec<(&str, &LogConfig)> = config
            .services
            .iter()
            .map(|(n, s)| (n.as_str(), &s.log))
            .collect();
        let buf = Arc::new(Mutex::new(Vec::new()));
        let output_manager = OutputManager::new(&service_configs, TestBuffer(buf.clone()))
            .await
            .unwrap();
        let (shutdown_tx, shutdown_rx) = mpsc::channel(2);
        let runner = Runner::new(
            config,
            PLATFORM,
            output_manager,
            dir.path().to_path_buf(),
            None,
            shutdown_rx,
            true,
        )
        .await
        .unwrap();
        let handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        loop {
            let output = read_buf(&buf);
            if output.contains("STRIPE=ok")
                && output.contains("DD=empty")
                && output.contains("leaked=***")
                && !output.contains("injected-secret-value")
            {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                let _ = shutdown_tx.send(()).await;
                panic!("missing inject/strip/redact lines in output:\n{output}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
        unsafe {
            std::env::set_var("PATH", original_path);
            std::env::remove_var("STRIPE_SECRET_KEY");
            std::env::remove_var("DD_API_KEY");
        }
    });
}
