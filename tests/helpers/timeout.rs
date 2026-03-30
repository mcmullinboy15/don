use std::time::Duration;

/// Run an async block with a timeout. Panics if the timeout is exceeded.
///
/// Use this in integration tests to prevent hangs from blocking CI.
/// Works both inside `#[tokio::test]` and in plain `#[test]` contexts —
/// if no runtime exists, one is created.
pub fn run_with_timeout<F: std::future::Future>(duration: Duration, f: F) -> F::Output {
    // If we're already inside a tokio runtime (e.g. #[tokio::test]),
    // use the current runtime. Otherwise create a new one.
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => handle.block_on(async move {
            tokio::time::timeout(duration, f)
                .await
                .expect("test timed out")
        }),
        Err(_) => tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move {
                tokio::time::timeout(duration, f)
                    .await
                    .expect("test timed out")
            }),
    }
}
