//! Fixture-owned SSH interception; production bootstrappers use ordinary SSH.

use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;

const ADAPTER: &str = include_str!("ssh_faults.py");

pub(super) fn install(command: &mut Command, state_dir: &Path) -> Result<(), String> {
    let path = std::env::var_os("PATH").ok_or("SSH fixture requires PATH")?;
    let paths = std::env::split_paths(&path).collect::<Vec<_>>();
    let real_ssh = paths
        .iter()
        .map(|directory| directory.join("ssh"))
        .find(|candidate| {
            fs::metadata(candidate).is_ok_and(|metadata| {
                metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
            })
        })
        .ok_or("SSH fixture requires an installed ssh executable")?
        .canonicalize()
        .map_err(|error| format!("resolve real SSH executable: {error}"))?;
    let directory = state_dir.join("harness-ssh");
    fs::create_dir_all(&directory)
        .map_err(|error| format!("create fixture SSH adapter directory: {error}"))?;
    let mut staged = tempfile::NamedTempFile::new_in(&directory)
        .map_err(|error| format!("stage fixture SSH adapter: {error}"))?;
    staged
        .write_all(ADAPTER.as_bytes())
        .map_err(|error| format!("write fixture SSH adapter: {error}"))?;
    staged
        .as_file()
        .set_permissions(fs::Permissions::from_mode(0o700))
        .map_err(|error| format!("make fixture SSH adapter executable: {error}"))?;
    staged
        .persist(directory.join("ssh"))
        .map_err(|error| format!("install fixture SSH adapter: {error}"))?;
    let path = std::env::join_paths(std::iter::once(directory).chain(paths))
        .map_err(|error| format!("build fixture SSH search path: {error}"))?;
    command
        .env("PATH", path)
        .env("MYELIN_HARNESS_REAL_SSH", real_ssh);
    Ok(())
}
