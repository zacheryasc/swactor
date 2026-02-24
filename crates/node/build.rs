use std::process::Command;

fn git(args: &[&str]) -> String {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}

fn main() {
    let hash = git(&["rev-parse", "--short", "HEAD"]);
    let branch = git(&["rev-parse", "--abbrev-ref", "HEAD"]);
    let dirty = git(&["status", "--porcelain"]);
    let suffix = if dirty.is_empty() { "" } else { "-dirty" };

    println!("cargo:rustc-env=SWACTOR_GIT_HASH={hash}{suffix}");
    println!("cargo:rustc-env=SWACTOR_GIT_BRANCH={branch}");

    // Rebuild when HEAD changes (new commit, branch switch)
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/index");
}
