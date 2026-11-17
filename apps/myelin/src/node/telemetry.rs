//! Durable producer identity, shared by node and orchestration telemetry.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;

pub(crate) fn allocate_telemetry_lifetime(
    state_root: &Path,
    node_id: u64,
    run_id: u64,
) -> Result<u64, String> {
    let directory = state_root.join(format!("node-{node_id}"));
    fs::create_dir_all(&directory)
        .map_err(|error| format!("create telemetry state {}: {error}", directory.display()))?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join("generation.lock"))
        .map_err(|error| format!("open telemetry generation lock: {error}"))?;
    lock.lock()
        .map_err(|error| format!("lock telemetry generation: {error}"))?;
    let path = directory.join("generation");
    let previous = match fs::read_to_string(&path) {
        Ok(value) => value
            .trim()
            .parse::<u64>()
            .map_err(|error| format!("read telemetry generation {}: {error}", path.display()))?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => {
            return Err(format!(
                "read telemetry generation {}: {error}",
                path.display()
            ));
        }
    };
    let next = previous
        .max(run_id)
        .checked_add(1)
        .ok_or_else(|| format!("telemetry generation exhausted for node {node_id}"))?;
    let pending = directory.join("generation.next");
    let mut file = File::create(&pending)
        .map_err(|error| format!("create telemetry generation {}: {error}", pending.display()))?;
    file.write_all(next.to_string().as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|error| {
            format!(
                "persist telemetry generation {}: {error}",
                pending.display()
            )
        })?;
    fs::rename(&pending, &path)
        .map_err(|error| format!("commit telemetry generation {}: {error}", path.display()))?;
    File::open(&directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("sync telemetry state {}: {error}", directory.display()))?;
    Ok(next)
}
