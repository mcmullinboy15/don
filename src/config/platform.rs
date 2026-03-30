use serde::Deserialize;

/// Target platform identified by OS and CPU architecture.
///
/// Used to select platform-specific downloads and apply service overrides.
/// Platform keys use Rust's `std::env::consts` naming conventions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Platform {
    LinuxX86_64,
    LinuxAarch64,
    MacosX86_64,
    MacosAarch64,
}

impl Platform {
    /// Returns the platform matching the current machine, or `None` if unsupported.
    pub fn current() -> Option<Self> {
        Self::from_os_arch(std::env::consts::OS, std::env::consts::ARCH)
    }

    fn from_os_arch(os: &str, arch: &str) -> Option<Self> {
        match (os, arch) {
            ("linux", "x86_64") => Some(Self::LinuxX86_64),
            ("linux", "aarch64") => Some(Self::LinuxAarch64),
            ("macos", "x86_64") => Some(Self::MacosX86_64),
            ("macos", "aarch64") => Some(Self::MacosAarch64),
            _ => None,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::LinuxX86_64 => "linux-x86_64",
            Self::LinuxAarch64 => "linux-aarch64",
            Self::MacosX86_64 => "macos-x86_64",
            Self::MacosAarch64 => "macos-aarch64",
        }
    }

    const ALL: &[Self] = &[
        Self::LinuxX86_64,
        Self::LinuxAarch64,
        Self::MacosX86_64,
        Self::MacosAarch64,
    ];
}

impl std::fmt::Display for Platform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Platform {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        for p in Self::ALL {
            if p.as_str() == s {
                return Ok(*p);
            }
        }
        Err(serde::de::Error::custom(format!(
            "unknown platform '{s}', expected one of: {}",
            Self::ALL
                .iter()
                .map(|p| p.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )))
    }
}
