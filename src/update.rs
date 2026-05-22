use std::time::Duration;

const CRATES_IO_API_BASE: &str = "https://crates.io/api/v1/crates";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UpdateAvailable {
    pub(crate) current_version: String,
    pub(crate) latest_version: String,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum UpdateCheckError {
    #[error("failed to build update-check client: {0}")]
    Client(reqwest::Error),
    #[error("failed to query crates.io: {0}")]
    Request(reqwest::Error),
    #[error("failed to parse crates.io response: {0}")]
    Json(serde_json::Error),
}

#[derive(Debug, serde::Deserialize)]
struct CratesIoResponse {
    #[serde(rename = "crate")]
    crate_info: CratesIoCrate,
}

#[derive(Debug, serde::Deserialize)]
struct CratesIoCrate {
    max_stable_version: Option<String>,
}

pub(crate) async fn check_crates_io(
    crate_name: &str,
    current_version: &str,
    timeout: Duration,
) -> Result<Option<UpdateAvailable>, UpdateCheckError> {
    let client = reqwest::Client::builder()
        .timeout(timeout)
        .user_agent(format!(
            "don/{} ({})",
            env!("CARGO_PKG_VERSION"),
            env!("CARGO_PKG_REPOSITORY")
        ))
        .build()
        .map_err(UpdateCheckError::Client)?;

    let body = client
        .get(format!("{CRATES_IO_API_BASE}/{crate_name}"))
        .send()
        .await
        .map_err(UpdateCheckError::Request)?
        .error_for_status()
        .map_err(UpdateCheckError::Request)?
        .text()
        .await
        .map_err(UpdateCheckError::Request)?;
    let response: CratesIoResponse = serde_json::from_str(&body).map_err(UpdateCheckError::Json)?;

    Ok(response
        .crate_info
        .max_stable_version
        .filter(|latest| is_newer_version(latest, current_version))
        .map(|latest_version| UpdateAvailable {
            current_version: current_version.to_string(),
            latest_version,
        }))
}

pub(crate) fn is_newer_version(candidate: &str, current: &str) -> bool {
    let Some(candidate) = ParsedVersion::parse(candidate) else {
        return false;
    };
    let Some(current) = ParsedVersion::parse(current) else {
        return false;
    };
    candidate.is_newer_than(&current)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ParsedVersion {
    major: u64,
    minor: u64,
    patch: u64,
    prerelease: bool,
}

impl ParsedVersion {
    fn parse(raw: &str) -> Option<Self> {
        let raw = raw.strip_prefix('v').unwrap_or(raw);
        let core = raw.split_once('+').map_or(raw, |(core, _)| core);
        let (core, prerelease) = match core.split_once('-') {
            Some((core, _)) => (core, true),
            None => (core, false),
        };
        let mut parts = core.split('.');
        let major = parse_numeric_part(parts.next()?)?;
        let minor = parse_numeric_part(parts.next()?)?;
        let patch = parse_numeric_part(parts.next()?)?;
        if parts.next().is_some() {
            return None;
        }
        Some(Self {
            major,
            minor,
            patch,
            prerelease,
        })
    }

    fn is_newer_than(&self, other: &Self) -> bool {
        (self.major, self.minor, self.patch) > (other.major, other.minor, other.patch)
            || ((self.major, self.minor, self.patch) == (other.major, other.minor, other.patch)
                && other.prerelease
                && !self.prerelease)
    }
}

fn parse_numeric_part(raw: &str) -> Option<u64> {
    if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    raw.parse().ok()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn detects_newer_versions() {
        struct Case {
            name: &'static str,
            candidate: &'static str,
            current: &'static str,
            want: bool,
        }
        let cases = [
            Case {
                name: "patch increase",
                candidate: "0.4.2",
                current: "0.4.1",
                want: true,
            },
            Case {
                name: "minor increase",
                candidate: "0.5.0",
                current: "0.4.9",
                want: true,
            },
            Case {
                name: "major increase",
                candidate: "1.0.0",
                current: "0.99.99",
                want: true,
            },
            Case {
                name: "same version",
                candidate: "0.4.1",
                current: "0.4.1",
                want: false,
            },
            Case {
                name: "older version",
                candidate: "0.4.0",
                current: "0.4.1",
                want: false,
            },
            Case {
                name: "v prefix",
                candidate: "v0.4.2",
                current: "0.4.1",
                want: true,
            },
            Case {
                name: "stable beats same prerelease",
                candidate: "0.4.2",
                current: "0.4.2-beta.1",
                want: true,
            },
            Case {
                name: "prerelease does not beat stable",
                candidate: "0.4.2-beta.1",
                current: "0.4.2",
                want: false,
            },
            Case {
                name: "invalid candidate",
                candidate: "latest",
                current: "0.4.1",
                want: false,
            },
            Case {
                name: "invalid current",
                candidate: "0.4.2",
                current: "dev",
                want: false,
            },
        ];

        for case in cases {
            assert_eq!(
                is_newer_version(case.candidate, case.current),
                case.want,
                "{}",
                case.name
            );
        }
    }
}
