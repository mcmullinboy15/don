//! Installing the daemon as a per-user service.
//!
//! A daemon you have to remember to start is a daemon that isn't running when
//! you open the browser, so `don daemon install` writes a systemd user unit
//! or a launchd agent and asks the platform to start it now and on login.
//!
//! Both are installed as *user* services, never system-wide. The daemon
//! proxies to project sockets that are chmod'd to their owner, so it is only
//! ever useful running as the person whose projects those are — and asking
//! for root to get a dev tool's dashboard would be a poor trade.

use super::paths::DaemonPaths;
use std::path::{Path, PathBuf};

/// Label/filename the service is installed under.
const SYSTEMD_UNIT_NAME: &str = "don.service";
const LAUNCHD_LABEL: &str = "com.pjtatlow.don";

/// Errors installing or removing the service.
#[derive(Debug, thiserror::Error)]
pub enum InstallError {
    #[error(
        "don doesn't know how to install a service on {os}.\n  \
         Run `don daemon` yourself, or wire it into whatever supervisor you use."
    )]
    UnsupportedPlatform { os: &'static str },
    #[error("cannot work out where to install the service: $HOME is not set to an absolute path")]
    NoHome,
    #[error("failed to locate the don binary: {0}")]
    NoExecutable(#[source] std::io::Error),
    #[error("failed to write '{}': {source}", path.display())]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to remove '{}': {source}", path.display())]
    Remove {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("`{command}` failed: {message}")]
    Command { command: String, message: String },
}

/// The per-user service manager to install into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceManager {
    /// systemd user units (Linux).
    Systemd,
    /// launchd LaunchAgents (macOS).
    Launchd,
}

impl ServiceManager {
    /// The manager for the platform this binary was built for.
    pub fn for_platform() -> Result<Self, InstallError> {
        if cfg!(target_os = "macos") {
            Ok(Self::Launchd)
        } else if cfg!(target_os = "linux") {
            Ok(Self::Systemd)
        } else {
            Err(InstallError::UnsupportedPlatform {
                os: std::env::consts::OS,
            })
        }
    }

    /// Human-readable name, for messages.
    fn label(self) -> &'static str {
        match self {
            Self::Systemd => "systemd user service",
            Self::Launchd => "launchd agent",
        }
    }
}

/// Everything needed to install, computed before anything is written.
///
/// Separating the plan from the act keeps rendering pure (and testable) and
/// means a bad path fails before a half-written unit file exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallPlan {
    pub manager: ServiceManager,
    /// Where the unit/plist goes.
    pub unit_path: PathBuf,
    /// Its full contents.
    pub contents: String,
}

/// Build an install plan for the current platform.
pub fn plan(paths: &DaemonPaths, port: u16) -> Result<InstallPlan, InstallError> {
    let manager = ServiceManager::for_platform()?;
    let exe = std::env::current_exe().map_err(InstallError::NoExecutable)?;
    let home = home_dir().ok_or(InstallError::NoHome)?;
    Ok(plan_with(manager, &exe, paths, port, &home))
}

/// Build an install plan from explicit inputs.
pub fn plan_with(
    manager: ServiceManager,
    exe: &Path,
    paths: &DaemonPaths,
    port: u16,
    home: &Path,
) -> InstallPlan {
    match manager {
        ServiceManager::Systemd => InstallPlan {
            manager,
            unit_path: home
                .join(".config")
                .join("systemd")
                .join("user")
                .join(SYSTEMD_UNIT_NAME),
            contents: render_systemd(exe, paths, port),
        },
        ServiceManager::Launchd => InstallPlan {
            manager,
            unit_path: home
                .join("Library")
                .join("LaunchAgents")
                .join(format!("{LAUNCHD_LABEL}.plist")),
            contents: render_launchd(exe, paths, port),
        },
    }
}

/// Write the unit and ask the platform to start it.
///
/// Returns lines to show the user. Writing the file is the part that must
/// succeed; if the supervisor command fails we still report what was written
/// and how to start it by hand, because a unit on disk the user can activate
/// beats an error and no trace of what happened.
pub fn install(plan: &InstallPlan) -> Result<Vec<String>, InstallError> {
    if let Some(parent) = plan.unit_path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| InstallError::Write {
            path: plan.unit_path.clone(),
            source,
        })?;
    }
    std::fs::write(&plan.unit_path, &plan.contents).map_err(|source| InstallError::Write {
        path: plan.unit_path.clone(),
        source,
    })?;

    let mut messages = vec![format!(
        "wrote {} to {}",
        plan.manager.label(),
        plan.unit_path.display()
    )];

    match plan.manager {
        ServiceManager::Systemd => {
            run("systemctl", &["--user", "daemon-reload"])?;
            run(
                "systemctl",
                &["--user", "enable", "--now", SYSTEMD_UNIT_NAME],
            )?;
            messages.push("enabled and started the service".to_string());
            messages.push(
                "note: run `loginctl enable-linger $USER` if you want the daemon \
                 to survive logging out"
                    .to_string(),
            );
        }
        ServiceManager::Launchd => {
            let target = format!("gui/{}", current_uid());
            // `bootout` first so re-installing picks up a changed unit
            // instead of failing with "service already loaded".
            let _ = run(
                "launchctl",
                &["bootout", &target, &path_str(&plan.unit_path)],
            );
            run(
                "launchctl",
                &["bootstrap", &target, &path_str(&plan.unit_path)],
            )?;
            messages.push("loaded the agent; it will start on login".to_string());
        }
    }

    Ok(messages)
}

/// Stop the service and remove its unit file.
///
/// Tolerant by design: a service that was already stopped, or a unit file
/// that was already deleted, still counts as uninstalled.
pub fn uninstall(plan: &InstallPlan) -> Result<Vec<String>, InstallError> {
    let mut messages = Vec::new();

    match plan.manager {
        ServiceManager::Systemd => {
            let _ = run(
                "systemctl",
                &["--user", "disable", "--now", SYSTEMD_UNIT_NAME],
            );
            messages.push("stopped and disabled the service".to_string());
        }
        ServiceManager::Launchd => {
            let target = format!("gui/{}", current_uid());
            let _ = run(
                "launchctl",
                &["bootout", &target, &path_str(&plan.unit_path)],
            );
            messages.push("unloaded the agent".to_string());
        }
    }

    match std::fs::remove_file(&plan.unit_path) {
        Ok(()) => messages.push(format!("removed {}", plan.unit_path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            messages.push(format!("{} was already gone", plan.unit_path.display()));
        }
        Err(source) => {
            return Err(InstallError::Remove {
                path: plan.unit_path.clone(),
                source,
            });
        }
    }

    if plan.manager == ServiceManager::Systemd {
        let _ = run("systemctl", &["--user", "daemon-reload"]);
    }

    Ok(messages)
}

/// Render a systemd user unit.
///
/// `DON_STATE_DIR` is pinned so the service resolves the same state directory
/// the installing shell did — a systemd user service gets a stripped
/// environment, and a daemon writing its socket somewhere else than
/// `don daemon status` looks would be a confusing way to fail.
fn render_systemd(exe: &Path, paths: &DaemonPaths, port: u16) -> String {
    format!(
        "[Unit]\n\
         Description=don — dev environment orchestrator\n\
         Documentation=https://github.com/pjtatlow/don\n\
         \n\
         [Service]\n\
         Type=simple\n\
         ExecStart={exe} daemon --port {port}\n\
         Environment=DON_STATE_DIR={state}\n\
         Restart=on-failure\n\
         RestartSec=2\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n",
        exe = exe.display(),
        state = paths.root().display(),
    )
}

/// Render a launchd LaunchAgent plist.
fn render_launchd(exe: &Path, paths: &DaemonPaths, port: u16) -> String {
    let log = paths.log_file();
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \
         \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
         <plist version=\"1.0\">\n\
         <dict>\n\
         \t<key>Label</key>\n\
         \t<string>{label}</string>\n\
         \t<key>ProgramArguments</key>\n\
         \t<array>\n\
         \t\t<string>{exe}</string>\n\
         \t\t<string>daemon</string>\n\
         \t\t<string>--port</string>\n\
         \t\t<string>{port}</string>\n\
         \t</array>\n\
         \t<key>EnvironmentVariables</key>\n\
         \t<dict>\n\
         \t\t<key>DON_STATE_DIR</key>\n\
         \t\t<string>{state}</string>\n\
         \t</dict>\n\
         \t<key>RunAtLoad</key>\n\
         \t<true/>\n\
         \t<key>KeepAlive</key>\n\
         \t<dict>\n\
         \t\t<key>SuccessfulExit</key>\n\
         \t\t<false/>\n\
         \t</dict>\n\
         \t<key>StandardOutPath</key>\n\
         \t<string>{log}</string>\n\
         \t<key>StandardErrorPath</key>\n\
         \t<string>{log}</string>\n\
         </dict>\n\
         </plist>\n",
        label = LAUNCHD_LABEL,
        exe = xml_escape(&exe.display().to_string()),
        state = xml_escape(&paths.root().display().to_string()),
        log = xml_escape(&log.display().to_string()),
    )
}

/// Escape the five XML metacharacters. Paths can legally contain `&` and
/// angle brackets, and an unescaped one would produce a plist launchd
/// refuses to parse.
fn xml_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            other => out.push(other),
        }
    }
    out
}

fn path_str(path: &Path) -> String {
    path.display().to_string()
}

fn current_uid() -> u32 {
    // SAFETY: `getuid` takes no arguments, cannot fail, and has no
    // preconditions — it just reads the calling process's real uid.
    unsafe { libc::getuid() }
}

fn home_dir() -> Option<PathBuf> {
    let home = PathBuf::from(std::env::var_os("HOME")?);
    home.is_absolute().then_some(home)
}

/// Run a supervisor command, turning a non-zero exit into an error that
/// quotes what the tool actually said.
fn run(program: &str, args: &[&str]) -> Result<(), InstallError> {
    let command = format!("{program} {}", args.join(" "));
    let output = std::process::Command::new(program)
        .args(args)
        .output()
        .map_err(|e| InstallError::Command {
            command: command.clone(),
            message: match e.kind() {
                std::io::ErrorKind::NotFound => {
                    format!("{program} is not installed or not on PATH")
                }
                _ => e.to_string(),
            },
        })?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    Err(InstallError::Command {
        command,
        message: if stderr.is_empty() {
            format!("exited with {}", output.status)
        } else {
            stderr
        },
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn paths() -> DaemonPaths {
        DaemonPaths::with_root(PathBuf::from("/home/u/.local/state/don"))
    }

    #[test]
    fn plans_land_in_the_platform_location() {
        struct Case {
            name: &'static str,
            manager: ServiceManager,
            home: &'static str,
            expect_path: &'static str,
        }

        let cases = vec![
            Case {
                name: "systemd user unit",
                manager: ServiceManager::Systemd,
                home: "/home/u",
                expect_path: "/home/u/.config/systemd/user/don.service",
            },
            Case {
                name: "launchd agent",
                manager: ServiceManager::Launchd,
                home: "/Users/u",
                expect_path: "/Users/u/Library/LaunchAgents/com.pjtatlow.don.plist",
            },
        ];

        for case in cases {
            let plan = plan_with(
                case.manager,
                Path::new("/usr/local/bin/don"),
                &paths(),
                3666,
                Path::new(case.home),
            );
            assert_eq!(
                plan.unit_path,
                PathBuf::from(case.expect_path),
                "case: {}",
                case.name
            );
            assert!(!plan.contents.is_empty(), "case: {}", case.name);
        }
    }

    #[test]
    fn systemd_unit_pins_binary_port_and_state_dir() {
        let unit = render_systemd(Path::new("/opt/don/bin/don"), &paths(), 4200);

        struct Case {
            name: &'static str,
            needle: &'static str,
        }

        let cases = vec![
            Case {
                name: "execs the resolved binary with the chosen port",
                needle: "ExecStart=/opt/don/bin/don daemon --port 4200",
            },
            Case {
                name: "pins the state dir so the service and the shell agree",
                needle: "Environment=DON_STATE_DIR=/home/u/.local/state/don",
            },
            Case {
                name: "restarts on failure",
                needle: "Restart=on-failure",
            },
            Case {
                name: "starts on login",
                needle: "WantedBy=default.target",
            },
        ];

        for case in cases {
            assert!(
                unit.contains(case.needle),
                "case: {} — unit was:\n{unit}",
                case.name
            );
        }
    }

    #[test]
    fn launchd_plist_pins_binary_port_and_state_dir() {
        let plist = render_launchd(Path::new("/opt/don/bin/don"), &paths(), 4200);

        for needle in [
            "<string>com.pjtatlow.don</string>",
            "<string>/opt/don/bin/don</string>",
            "<string>daemon</string>",
            "<string>--port</string>",
            "<string>4200</string>",
            "<key>DON_STATE_DIR</key>",
            "<string>/home/u/.local/state/don</string>",
            "<string>/home/u/.local/state/don/logs/daemon.log</string>",
            "<key>RunAtLoad</key>",
        ] {
            assert!(
                plist.contains(needle),
                "expected {needle:?} in plist:\n{plist}"
            );
        }
        assert!(plist.starts_with("<?xml"), "plist needs an XML declaration");
    }

    #[test]
    fn plist_escapes_paths_that_would_break_the_xml() {
        // A directory named with an ampersand is legal and would otherwise
        // produce a plist launchd refuses to parse.
        let awkward = DaemonPaths::with_root(PathBuf::from("/home/a & b/<don>"));
        let plist = render_launchd(Path::new("/bin/don"), &awkward, 3666);
        assert!(plist.contains("/home/a &amp; b/&lt;don&gt;"), "{plist}");
        assert!(
            !plist.contains("/home/a & b/<don>"),
            "raw metacharacters must not survive"
        );
    }

    #[test]
    fn xml_escape_covers_every_metacharacter() {
        struct Case {
            input: &'static str,
            expect: &'static str,
        }

        let cases = vec![
            Case {
                input: "plain",
                expect: "plain",
            },
            Case {
                input: "a & b",
                expect: "a &amp; b",
            },
            Case {
                input: "<tag>",
                expect: "&lt;tag&gt;",
            },
            Case {
                input: "say \"hi\"",
                expect: "say &quot;hi&quot;",
            },
            Case {
                input: "it's",
                expect: "it&apos;s",
            },
            Case {
                input: "&<>\"'",
                expect: "&amp;&lt;&gt;&quot;&apos;",
            },
        ];

        for case in cases {
            assert_eq!(xml_escape(case.input), case.expect, "input: {}", case.input);
        }
    }

    #[test]
    fn install_writes_the_unit_and_uninstall_removes_it() {
        // Exercise the filesystem half without invoking systemctl/launchctl:
        // a plan whose supervisor command will fail still has to leave the
        // unit on disk so the user can start it by hand.
        let tmp = tempfile::tempdir().unwrap();
        let plan = InstallPlan {
            manager: ServiceManager::Systemd,
            unit_path: tmp.path().join("nested").join("don.service"),
            contents: "[Unit]\nDescription=test\n".to_string(),
        };

        let _ = install(&plan);
        assert!(plan.unit_path.exists(), "the unit file must be written");
        assert_eq!(
            std::fs::read_to_string(&plan.unit_path).unwrap(),
            plan.contents
        );

        let messages = uninstall(&plan).unwrap();
        assert!(!plan.unit_path.exists(), "the unit file must be removed");
        assert!(
            messages.iter().any(|m| m.contains("removed")),
            "messages: {messages:?}"
        );

        // Uninstalling twice is not an error.
        let messages = uninstall(&plan).unwrap();
        assert!(
            messages.iter().any(|m| m.contains("already gone")),
            "messages: {messages:?}"
        );
    }
}
