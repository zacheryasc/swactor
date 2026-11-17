use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LeaseOwnership {
    pub label: String,
    pub contract_id: Option<u64>,
    pub cancelled: bool,
    pub destroyed: bool,
}

#[derive(Clone)]
pub(super) struct LeaseJournal {
    directory: PathBuf,
}

impl LeaseJournal {
    pub fn new(directory: &Path) -> Self {
        Self {
            directory: directory.join("lease-ownership"),
        }
    }

    fn path(&self, label: &str) -> PathBuf {
        self.directory
            .join(format!("{}.json", blake3::hash(label.as_bytes()).to_hex()))
    }

    pub fn load(&self, label: &str) -> Result<Option<LeaseOwnership>, String> {
        let mut file = match File::open(self.path(label)) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(format!("open VastAI ownership journal: {error}")),
        };
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|error| format!("read VastAI ownership journal: {error}"))?;
        let ownership: LeaseOwnership = serde_json::from_slice(&bytes)
            .map_err(|error| format!("decode VastAI ownership journal: {error}"))?;
        if ownership.label != label || ownership.contract_id == Some(0) {
            return Err("VastAI ownership journal identity mismatch".to_owned());
        }
        file.sync_all()
            .map_err(|error| format!("sync loaded VastAI ownership: {error}"))?;
        self.sync_directory()?;
        Ok(Some(ownership))
    }

    fn ensure_directory(&self) -> Result<(), String> {
        match fs::create_dir(&self.directory) {
            Ok(()) => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    fs::set_permissions(&self.directory, fs::Permissions::from_mode(0o700))
                        .map_err(|error| format!("secure VastAI ownership directory: {error}"))?;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(format!("create VastAI ownership directory: {error}")),
        }
        if !fs::symlink_metadata(&self.directory)
            .map_err(|error| error.to_string())?
            .is_dir()
        {
            return Err("VastAI ownership directory is not a real directory".to_owned());
        }
        File::open(
            self.directory
                .parent()
                .expect("journal has a state directory"),
        )
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("sync VastAI ownership directory creation: {error}"))
    }

    fn sync_directory(&self) -> Result<(), String> {
        #[cfg(test)]
        if FAIL_DIRECTORY_SYNC.with(|fail| fail.replace(false)) {
            return Err("injected ownership directory sync failure".to_owned());
        }
        File::open(&self.directory)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| format!("sync VastAI ownership directory: {error}"))
    }

    pub fn acquire_lock(&self) -> Result<File, String> {
        self.ensure_directory()?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options
            .open(self.directory.join("transactions.lock"))
            .map_err(|error| format!("open VastAI ownership lock: {error}"))?;
        file.lock()
            .map_err(|error| format!("lock VastAI ownership transaction: {error}"))?;
        Ok(file)
    }

    pub fn reserve_creation(&self, label: &str) -> Result<(), String> {
        let _guard = self.acquire_lock()?;
        if let Some(record) = self.load(label)? {
            if record.contract_id.is_none() && record.destroyed && !record.cancelled {
                return self.store(&LeaseOwnership {
                    label: label.to_owned(),
                    contract_id: None,
                    cancelled: false,
                    destroyed: false,
                });
            }
            return Err(format!(
                "VastAI label {label} already has owned or cancelled creation"
            ));
        }
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        // Exclusive creation is the ownership boundary even between processes.
        // No provider request is sent until the intent and directory are durable.
        let mut file = options
            .open(self.path(label))
            .map_err(|error| format!("reserve VastAI create ownership for {label}: {error}"))?;
        let bytes = serde_json::to_vec(&LeaseOwnership {
            label: label.to_owned(),
            contract_id: None,
            cancelled: false,
            destroyed: false,
        })
        .map_err(|error| format!("encode VastAI create intent: {error}"))?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|error| format!("commit VastAI create intent: {error}"))?;
        File::open(&self.directory)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| format!("sync VastAI create intent: {error}"))
    }

    pub fn store(&self, ownership: &LeaseOwnership) -> Result<(), String> {
        self.ensure_directory()?;
        let path = self.path(&ownership.label);
        let temporary = path.with_extension("pending");
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temporary)
            .map_err(|error| format!("open VastAI ownership transaction: {error}"))?;
        let bytes = serde_json::to_vec(ownership)
            .map_err(|error| format!("encode VastAI ownership transaction: {error}"))?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|error| format!("commit VastAI ownership transaction: {error}"))?;
        fs::rename(&temporary, path)
            .map_err(|error| format!("publish VastAI ownership transaction: {error}"))?;
        self.sync_directory()
    }

    pub fn reject_creation(&self, label: &str) -> Result<(), String> {
        let _guard = self.acquire_lock()?;
        let mut record = self
            .load(label)?
            .ok_or("VastAI create rejection has no durable intent")?;
        if record.contract_id.is_some() {
            return Err("cannot reject creation of a known VastAI contract".to_owned());
        }
        // Only a typed definitive provider rejection proves there was no lease.
        // Preserve cancellation so a competing owner cannot rent after Stop.
        record.destroyed = true;
        self.store(&record)
    }
}

#[cfg(test)]
thread_local! {
    static FAIL_DIRECTORY_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_does_not_accept_ownership_without_reestablishing_failed_commit_durability() {
        let directory = tempfile::tempdir().unwrap();
        let journal = LeaseJournal::new(directory.path());
        journal.reserve_creation("lease").unwrap();
        let mut ownership = journal.load("lease").unwrap().unwrap();
        ownership.contract_id = Some(73);
        FAIL_DIRECTORY_SYNC.with(|fail| fail.set(true));
        assert!(journal.store(&ownership).is_err());
        FAIL_DIRECTORY_SYNC.with(|fail| fail.set(true));
        assert!(
            journal.load("lease").is_err(),
            "visible renamed data is not proof of durable ownership"
        );
        assert_eq!(
            journal.load("lease").unwrap().unwrap().contract_id,
            Some(73)
        );
    }

    #[test]
    fn rejected_creation_proves_no_lease_but_does_not_erase_cancellation() {
        let directory = tempfile::tempdir().unwrap();
        let journal = LeaseJournal::new(directory.path());
        journal.reserve_creation("lease").unwrap();
        journal.reject_creation("lease").unwrap();
        assert!(journal.load("lease").unwrap().unwrap().destroyed);
        journal.reserve_creation("lease").unwrap();
        let mut ownership = journal.load("lease").unwrap().unwrap();
        ownership.cancelled = true;
        journal.store(&ownership).unwrap();
        journal.reject_creation("lease").unwrap();
        let restored = LeaseJournal::new(directory.path())
            .load("lease")
            .unwrap()
            .unwrap();
        assert!(restored.destroyed && restored.cancelled);
        assert_eq!(restored.contract_id, None);
        assert!(journal.reserve_creation("lease").is_err());
    }
}
