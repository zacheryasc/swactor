use std::path::Path;

use crate::types::FleetState;

impl FleetState {
    pub fn load(path: &Path) -> Result<Self, String> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read fleet state {}: {e}", path.display()))?;
        serde_json::from_str(&raw)
            .map_err(|e| format!("cannot parse fleet state {}: {e}", path.display()))
    }

    pub fn save(&self, path: &Path) -> Result<(), String> {
        let raw = serde_json::to_string_pretty(self)
            .map_err(|e| format!("cannot serialize fleet state: {e}"))?;
        std::fs::write(path, raw)
            .map_err(|e| format!("cannot write fleet state {}: {e}", path.display()))
    }
}
