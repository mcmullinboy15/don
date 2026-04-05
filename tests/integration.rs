mod helpers;

use don::config::{Config, ConfigError, Platform};
use helpers::config::ConfigBuilder;
use helpers::tempdir::TempDir;

const TEST_PLATFORM: Platform = Platform::LinuxX86_64;

#[test]
fn validate_valid_config() {
    let dir = TempDir::new("validate-valid");
    let config_path = ConfigBuilder::new()
        .add_custom_service("api", "mybin", &["serve"])
        .done()
        .add_docker_service("postgres", "postgres:16")
        .done()
        .write_to(dir.path());

    let config = Config::from_file(&config_path).unwrap();
    assert!(config.validate(TEST_PLATFORM).is_ok());
}

#[test]
fn validate_invalid_config_unknown_dep() {
    let dir = TempDir::new("validate-invalid-dep");
    let config_path = ConfigBuilder::new()
        .add_custom_service("api", "mybin", &[])
        .depends_on(&["ghost"])
        .write_to(dir.path());

    let config = Config::from_file(&config_path).unwrap();
    let err = config.validate(TEST_PLATFORM).unwrap_err();
    let ConfigError::Validation { errors } = &err else {
        panic!("expected validation error");
    };
    assert!(errors[0].contains("unknown service or task 'ghost'"));
}

#[test]
fn validate_missing_config_file() {
    let dir = TempDir::new("validate-missing");
    let config_path = dir.child("nonexistent.toml");

    let err = Config::from_file(&config_path).unwrap_err();
    assert!(matches!(err, ConfigError::ReadFile { .. }));
}

#[test]
fn validate_malformed_toml() {
    let dir = TempDir::new("validate-malformed");
    let config_path = dir.child("don.toml");
    std::fs::write(&config_path, "this is not [valid toml").unwrap();

    let err = Config::from_file(&config_path).unwrap_err();
    assert!(matches!(err, ConfigError::Parse(_)));
}

#[test]
fn validate_empty_config_is_valid() {
    let dir = TempDir::new("validate-empty");
    let config_path = dir.child("don.toml");
    std::fs::write(&config_path, "").unwrap();

    let config = Config::from_file(&config_path).unwrap();
    assert!(config.validate(TEST_PLATFORM).is_ok());
}

#[test]
fn validate_config_with_tasks_and_profiles() {
    let dir = TempDir::new("validate-full");
    let config_path = ConfigBuilder::new()
        .add_docker_service("postgres", "postgres:16")
        .done()
        .add_custom_service("api", "api-server", &["serve"])
        .depends_on(&["migrate"])
        .done()
        .add_task("migrate", "dbmate", &["up"])
        .done()
        .add_profile("frontend", &["api", "postgres"], &["migrate"])
        .write_to(dir.path());

    let config = Config::from_file(&config_path).unwrap();
    assert!(config.validate(TEST_PLATFORM).is_ok());
    assert_eq!(config.services.len(), 2);
    assert_eq!(config.tasks.len(), 1);
    assert_eq!(config.profiles.len(), 1);
}

#[test]
fn validate_cycle_detected() {
    let dir = TempDir::new("validate-cycle");
    let config_path = ConfigBuilder::new()
        .add_custom_service("a", "a", &[])
        .depends_on(&["b"])
        .done()
        .add_custom_service("b", "b", &[])
        .depends_on(&["a"])
        .write_to(dir.path());

    let config = Config::from_file(&config_path).unwrap();
    let err = config.validate(TEST_PLATFORM).unwrap_err();
    let ConfigError::Validation { errors } = &err else {
        panic!("expected validation error");
    };
    assert!(errors.iter().any(|e| e.contains("dependency cycle")));
}

#[test]
fn validate_invalid_duration_strings() {
    let dir = TempDir::new("validate-duration");
    let config_path = ConfigBuilder::new()
        .raw(r#"
            [services.api]
            run.cmd = "api"
            debounce = "not-a-duration"
        "#)
        .write_to(dir.path());

    let config = Config::from_file(&config_path).unwrap();
    let err = config.validate(TEST_PLATFORM).unwrap_err();
    let ConfigError::Validation { errors } = &err else {
        panic!("expected validation error");
    };
    assert!(errors.iter().any(|e| e.contains("invalid debounce")));
}

#[test]
fn validate_invalid_watch_pattern() {
    let dir = TempDir::new("validate-watch-pattern");
    let config_path = ConfigBuilder::new()
        .raw(r#"
            [services.api]
            run.cmd = "api"
            watch = ["src/[*.rs"]

            [tasks.build]
            cmd = "make"
            watch = ["lib/[bad"]
        "#)
        .write_to(dir.path());

    let config = Config::from_file(&config_path).unwrap();
    let err = config.validate(TEST_PLATFORM).unwrap_err();
    let ConfigError::Validation { errors } = &err else {
        panic!("expected validation error");
    };
    assert!(
        errors.iter().any(|e| e.contains("service 'api'") && e.contains("invalid watch pattern")),
        "expected service watch pattern error, got: {errors:?}"
    );
    assert!(
        errors.iter().any(|e| e.contains("task 'build'") && e.contains("invalid watch pattern")),
        "expected task watch pattern error, got: {errors:?}"
    );
}

#[test]
fn validate_tcp_ready_check_on_listen_address_warns() {
    let toml = ConfigBuilder::new()
        .add_custom_service("api", "mybin", &[])
        .listen(&["0.0.0.0:3000"])
        .ready_tcp("0.0.0.0:3000")
        .done()
        .build();

    let config: Config = toml.parse().unwrap();
    let warnings = config.validate(TEST_PLATFORM).unwrap();
    assert!(
        warnings.iter().any(|w| w.contains("TCP ready check") && w.contains("don holds that socket")),
        "expected warning about TCP ready check on listen address, got: {warnings:?}"
    );
}

#[test]
fn validate_tcp_ready_check_on_different_address_no_warning() {
    let toml = ConfigBuilder::new()
        .add_custom_service("api", "mybin", &[])
        .listen(&["0.0.0.0:3000"])
        .ready_tcp("0.0.0.0:4000")
        .done()
        .build();

    let config: Config = toml.parse().unwrap();
    let warnings = config.validate(TEST_PLATFORM).unwrap();
    assert!(
        warnings.is_empty(),
        "expected no warnings when TCP check is on a different address, got: {warnings:?}"
    );
}

#[test]
fn don_validate_cli_valid_config() {
    let dir = TempDir::new("cli-validate-valid");
    ConfigBuilder::new()
        .add_custom_service("api", "mybin", &[])
        .write_to(dir.path());

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_don"))
        .args([
            "--config",
            dir.child("don.toml").to_str().unwrap(),
            "validate",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Config is valid"));
}

#[test]
fn don_validate_cli_invalid_config() {
    let dir = TempDir::new("cli-validate-invalid");
    let config_path = dir.child("don.toml");
    std::fs::write(&config_path, "[services.broken]\nenv = { FOO = \"bar\" }").unwrap();

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_don"))
        .args(["--config", config_path.to_str().unwrap(), "validate"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("validation failed"));
}

#[test]
fn don_validate_cli_missing_config() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_don"))
        .args([
            "--config",
            "/tmp/don-test-nonexistent-12345.toml",
            "validate",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("failed to read config file"));
}

// --- Download validation ---

fn download_toml(extra_lines: &str) -> String {
    format!(
        r#"
[services.tool]
run.cmd = "tool"

[services.tool.download.platform.linux-x86_64]
url = "https://example.com/tool.tar.gz"
sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
path = "tool"
{extra_lines}
"#
    )
}

#[test]
fn validate_download_bad_sha256_length() {
    let toml = r#"
[services.tool]
run.cmd = "tool"

[services.tool.download.platform.linux-x86_64]
url = "https://example.com/tool"
sha256 = "tooshort"
"#;
    let config: Config = toml.parse().unwrap();
    let err = config.validate(TEST_PLATFORM).unwrap_err();
    let ConfigError::Validation { errors } = &err else {
        panic!("expected validation error");
    };
    assert!(
        errors.iter().any(|e| e.contains("64 hex characters")),
        "expected sha256 length error, got: {errors:?}"
    );
}

#[test]
fn validate_download_bad_url_scheme() {
    let toml = r#"
[services.tool]
run.cmd = "tool"

[services.tool.download.platform.linux-x86_64]
url = "file:///etc/passwd"
sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
"#;
    let config: Config = toml.parse().unwrap();
    let err = config.validate(TEST_PLATFORM).unwrap_err();
    let ConfigError::Validation { errors } = &err else {
        panic!("expected validation error");
    };
    assert!(
        errors.iter().any(|e| e.contains("must start with http")),
        "expected url scheme error, got: {errors:?}"
    );
}

#[test]
fn validate_download_without_run_cmd() {
    // Download but no run.cmd → should error.
    let toml = r#"
[services.tool]

[services.tool.download.platform.linux-x86_64]
url = "https://example.com/tool"
sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
"#;
    let config: Config = toml.parse().unwrap();
    let err = config.validate(TEST_PLATFORM).unwrap_err();
    let ConfigError::Validation { errors } = &err else {
        panic!("expected validation error");
    };
    assert!(
        errors
            .iter()
            .any(|e| e.contains("download requires a run command")),
        "expected run command error, got: {errors:?}"
    );
}

#[test]
fn validate_valid_download_config_passes() {
    let toml = download_toml("");
    let config: Config = toml.parse().unwrap();
    assert!(config.validate(TEST_PLATFORM).is_ok());
}

#[test]
fn validate_download_bin_name_collision() {
    // Two services download different binaries that would both link to
    // `.don/bin/cockroach` — must error unless disambiguated.
    let toml = r#"
[services.crdb_v25]
run.cmd = "cockroach"
[services.crdb_v25.download.platform.linux-x86_64]
url = "https://example.com/v25.tgz"
sha256 = "0000000000000000000000000000000000000000000000000000000000000001"
path = "cockroach-v25/cockroach"

[services.crdb_v24]
run.cmd = "cockroach"
[services.crdb_v24.download.platform.linux-x86_64]
url = "https://example.com/v24.tgz"
sha256 = "0000000000000000000000000000000000000000000000000000000000000002"
path = "cockroach-v24/cockroach"
"#;
    let config: Config = toml.parse().unwrap();
    let err = config.validate(TEST_PLATFORM).unwrap_err();
    let ConfigError::Validation { errors } = &err else {
        panic!("expected validation error");
    };
    assert!(
        errors.iter().any(|e| e.contains("bin_name 'cockroach'")
            && e.contains("crdb_v25")
            && e.contains("crdb_v24")),
        "expected bin_name collision error, got: {errors:?}"
    );
}

#[test]
fn validate_download_bin_name_override_resolves_collision() {
    // Same two-crdb setup but with explicit bin_names → passes.
    let toml = r#"
[services.crdb_v25]
run.cmd = "cockroach"
[services.crdb_v25.download]
bin_name = "cockroach-v25"
[services.crdb_v25.download.platform.linux-x86_64]
url = "https://example.com/v25.tgz"
sha256 = "0000000000000000000000000000000000000000000000000000000000000001"
path = "cockroach-v25/cockroach"

[services.crdb_v24]
run.cmd = "cockroach"
[services.crdb_v24.download]
bin_name = "cockroach-v24"
[services.crdb_v24.download.platform.linux-x86_64]
url = "https://example.com/v24.tgz"
sha256 = "0000000000000000000000000000000000000000000000000000000000000002"
path = "cockroach-v24/cockroach"
"#;
    let config: Config = toml.parse().unwrap();
    assert!(
        config.validate(TEST_PLATFORM).is_ok(),
        "explicit bin_names should resolve the collision"
    );
}

#[test]
fn validate_download_missing_current_platform_warns() {
    // TEST_PLATFORM is LinuxX86_64. Provide only macos entries — should warn.
    let toml = r#"
[services.tool]
run.cmd = "tool"

[services.tool.download.platform.macos-aarch64]
url = "https://example.com/tool-mac.tar.gz"
sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
"#;
    let config: Config = toml.parse().unwrap();
    let warnings = config.validate(TEST_PLATFORM).unwrap();
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("no download entry for current platform")
                && w.contains("linux-x86_64")),
        "expected platform warning, got: {warnings:?}"
    );
}
