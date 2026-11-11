//! Version and build metadata.

use std::fmt;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone)]
pub struct BuildInfo {
    pub version: &'static str,
    pub git_hash: Option<&'static str>,
    pub build_timestamp: &'static str,
    pub rustc_version: &'static str,
    pub target: &'static str,
    pub profile: &'static str,
}

impl BuildInfo {
    pub const fn new() -> Self {
        Self {
            version: VERSION,
            git_hash: option_env!("GIT_HASH"),
            build_timestamp: env!("BUILD_TIMESTAMP"),
            rustc_version: env!("RUSTC_VERSION"),
            target: env!("TARGET"),
            profile: env!("PROFILE"),
        }
    }

    pub fn short_version(&self) -> String {
        format!("orcher-common v{}", self.version)
    }

    pub fn detailed_version(&self) -> String {
        let mut parts = vec![format!("orcher-common v{}", self.version)];

        if let Some(hash) = self.git_hash {
            parts.push(format!("git:{}", &hash[..8.min(hash.len())]));
        }

        parts.push(format!("built:{}", self.build_timestamp));
        parts.push(format!("rustc:{}", self.rustc_version));
        parts.push(format!("target:{}", self.target));

        parts.join(" ")
    }

    pub fn is_dev_build(&self) -> bool {
        self.profile == "debug" || self.version.contains("dev") || self.version.contains("alpha")
    }

    pub fn is_release_build(&self) -> bool {
        self.profile == "release" && !self.is_dev_build()
    }
}

impl fmt::Display for BuildInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.detailed_version())
    }
}

impl Default for BuildInfo {
    fn default() -> Self {
        Self::new()
    }
}

pub fn build_info() -> BuildInfo {
    BuildInfo::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_constant() {
        assert!(!VERSION.is_empty());
        assert!(VERSION.contains('.'));
    }

    #[test]
    fn test_build_info_creation() {
        let info = BuildInfo::new();
        assert_eq!(info.version, VERSION);
        assert!(!info.build_timestamp.is_empty());
        assert!(!info.rustc_version.is_empty());
        assert!(!info.target.is_empty());
        assert!(!info.profile.is_empty());
    }

    #[test]
    fn test_build_type_detection() {
        let info = BuildInfo::new();
        // At least one should be true
        assert!(info.is_dev_build() || info.is_release_build());
    }
}
