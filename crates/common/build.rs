use std::env;
use std::process::Command;

fn main() {
    let profile = env::var("PROFILE").unwrap_or_else(|_| "unknown".to_string());
    let is_release = profile == "release";

    // Only capture git hash in release builds to avoid constant recompilation
    if is_release {
        if let Ok(output) = Command::new("git").args(["rev-parse", "HEAD"]).output() {
            if output.status.success() {
                let git_hash = String::from_utf8_lossy(&output.stdout);
                let git_hash = git_hash.trim();
                if !git_hash.is_empty() {
                    println!("cargo:rustc-env=GIT_HASH={}", git_hash);
                } else {
                    println!("cargo:rustc-env=GIT_HASH=unknown");
                }
            } else {
                println!("cargo:rustc-env=GIT_HASH=unknown");
            }
        } else {
            println!("cargo:rustc-env=GIT_HASH=unknown");
        }
    } else {
        // Use static value for dev builds
        println!("cargo:rustc-env=GIT_HASH=dev");
    }

    // Use static timestamp for dev builds to avoid recompilation
    if is_release {
        let build_timestamp = jiff::Timestamp::now()
            .strftime("%Y-%m-%d %H:%M:%S UTC")
            .to_string();
        println!("cargo:rustc-env=BUILD_TIMESTAMP={}", build_timestamp);
    } else {
        println!("cargo:rustc-env=BUILD_TIMESTAMP=dev-build");
    }

    // Capture Rust compiler version
    if let Ok(output) = Command::new("rustc").args(["--version"]).output() {
        if output.status.success() {
            let rustc_version = String::from_utf8_lossy(&output.stdout);
            let rustc_version = rustc_version.trim();
            println!("cargo:rustc-env=RUSTC_VERSION={}", rustc_version);
        }
    } else {
        println!("cargo:rustc-env=RUSTC_VERSION=unknown");
    }

    // Capture target triple
    let target = env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=TARGET={}", target);

    // Capture profile
    println!("cargo:rustc-env=PROFILE={}", profile);

    // Only monitor git changes in release builds to reduce recompilation triggers
    if is_release {
        println!("cargo:rerun-if-changed=.git/HEAD");

        // Also rerun if any git refs change (for branch switches)
        if let Ok(entries) = std::fs::read_dir(".git/refs/heads") {
            for entry in entries.flatten() {
                if let Ok(path) = entry.path().into_os_string().into_string() {
                    println!("cargo:rerun-if-changed={}", path);
                }
            }
        }
    }

    // Always rerun if this build script changes
    println!("cargo:rerun-if-changed=build.rs");
}
