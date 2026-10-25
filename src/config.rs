//! Bounded readers for operator-supplied JSON configuration files.

use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

use serde::de::DeserializeOwned;

/// Planning profiles can contain many measurements, but should remain far smaller than this.
pub const MAX_CONFIG_FILE_BYTES: usize = 64 * 1024 * 1024;

/// Reads at most `limit` bytes from a file, checking metadata first and the stream while reading.
pub fn read_file_limited(path: &Path, limit: usize) -> io::Result<Vec<u8>> {
    let file = File::open(path)?;
    if file.metadata()?.len() > limit as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("file exceeds the {limit}-byte limit"),
        ));
    }

    let mut bytes = Vec::new();
    file.take(limit.saturating_add(1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("file exceeds the {limit}-byte limit"),
        ));
    }
    Ok(bytes)
}

/// Reads one JSON configuration file without allowing it to consume unbounded memory.
pub fn read_json_file<T: DeserializeOwned>(path: &Path, what: &str) -> anyhow::Result<T> {
    read_json_file_with_limit(path, what, MAX_CONFIG_FILE_BYTES)
}

fn read_json_file_with_limit<T: DeserializeOwned>(
    path: &Path,
    what: &str,
    limit: usize,
) -> anyhow::Result<T> {
    let bytes = read_file_limited(path, limit)
        .map_err(|error| anyhow::anyhow!("failed to read {what} {}: {error}", path.display()))?;

    serde_json::from_slice(&bytes)
        .map_err(|error| anyhow::anyhow!("invalid {what} JSON in {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_file_reader_enforces_limit_and_names_the_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("profile.json");
        std::fs::write(&path, br#"{"value": 123}"#).unwrap();

        let parsed: serde_json::Value = read_json_file_with_limit(&path, "profile", 32).unwrap();
        assert_eq!(parsed["value"], 123);

        let error = read_json_file_with_limit::<serde_json::Value>(&path, "profile", 8)
            .unwrap_err()
            .to_string();
        assert!(error.contains("profile.json"));
        assert!(error.contains("8-byte limit"));
    }

    #[test]
    fn json_file_reader_rejects_files_over_the_limit() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("growing.json");
        std::fs::write(&path, b"{}").unwrap();

        let error = read_json_file_with_limit::<serde_json::Value>(&path, "profile", 1)
            .unwrap_err()
            .to_string();
        assert!(error.contains("1-byte limit"));
    }
}
