use std::fs;
use std::path::Path;

use rand::RngExt;

pub fn get_dynamic_port() -> u16 {
    const FIRST_DYNAMIC_PORT: u16 = 49152;
    const LAST_DYNAMIC_PORT: u16 = 65535;
    FIRST_DYNAMIC_PORT + rand::rng().random_range(0..LAST_DYNAMIC_PORT - FIRST_DYNAMIC_PORT)
}

pub fn clean_up_work_dir(work_dir: &Path, log_cleanup: bool) {
    if log_cleanup {
        // Remove created files.
        if fs::remove_dir_all(work_dir).is_err() {
            log::error!("failed removing tmp work dir: {}", work_dir.display());
        }
    } else if let Ok(dir) = fs::read_dir(work_dir) {
        for entry in dir.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            let path = entry.path();
            if meta.is_dir() {
                let _ = fs::remove_dir_all(path);
                continue;
            }
            if path.extension().and_then(|ext| ext.to_str()) != Some("log") {
                let _ = fs::remove_file(path);
            }
        }
    }
}
