use std::{
    fmt, fs,
    path::{Path, PathBuf},
};

use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};

use crate::error::Result;

#[derive(Debug, Parser)]
#[command(version, about)]
pub struct Cli {
    /// iBroadcast OAuth client id.
    #[arg(long, env = "IBROADCAST_CLIENT_ID", hide_env_values = true)]
    pub client_id: Option<String>,
    /// OAuth client secret, if your iBroadcast app requires one. Never persisted.
    #[arg(long, env = "IBROADCAST_CLIENT_SECRET", hide_env_values = true)]
    pub client_secret: Option<String>,
    #[arg(long)]
    pub download_dir: Option<PathBuf>,
    /// Playback bitrate. Overrides the account preference reported by the server.
    #[arg(long, value_enum)]
    pub bitrate: Option<Bitrate>,
    #[arg(long, default_value = "warn")]
    pub log_level: String,
}

/// On-disk configuration. Every field is optional so that an absent value can
/// fall back to a default without inspecting the raw file text.
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct ConfigFile {
    client_id: Option<String>,
    download_dir: Option<PathBuf>,
    playback_bitrate: Option<Bitrate>,
    download_bitrate: Option<Bitrate>,
    plain_token_file: Option<bool>,
}

/// Resolved runtime configuration.
#[derive(Clone, Debug)]
pub struct Config {
    pub config_dir: PathBuf,
    config_file: PathBuf,
    pub client_id: Option<String>,
    /// Runtime-only; never written to disk.
    pub client_secret: Option<String>,
    pub download_dir: PathBuf,
    /// `None` means "follow the account preference reported by the server".
    pub playback_bitrate: Option<Bitrate>,
    pub download_bitrate: Bitrate,
    pub plain_token_file: bool,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
pub enum Bitrate {
    #[serde(rename = "96")]
    #[value(name = "96")]
    Kbps96,
    #[default]
    #[serde(rename = "128")]
    #[value(name = "128")]
    Kbps128,
    #[serde(rename = "192")]
    #[value(name = "192")]
    Kbps192,
    #[serde(rename = "256")]
    #[value(name = "256")]
    Kbps256,
    #[serde(rename = "320")]
    #[value(name = "320")]
    Kbps320,
    #[serde(rename = "orig")]
    #[value(name = "orig")]
    Original,
}

impl Bitrate {
    pub const VALUES: [Self; 6] = [
        Self::Kbps96,
        Self::Kbps128,
        Self::Kbps192,
        Self::Kbps256,
        Self::Kbps320,
        Self::Original,
    ];

    pub fn as_path_segment(self) -> &'static str {
        match self {
            Self::Kbps96 => "96",
            Self::Kbps128 => "128",
            Self::Kbps192 => "192",
            Self::Kbps256 => "256",
            Self::Kbps320 => "320",
            Self::Original => "orig",
        }
    }

    pub fn from_u64(value: u64) -> Option<Self> {
        match value {
            96 => Some(Self::Kbps96),
            128 => Some(Self::Kbps128),
            192 => Some(Self::Kbps192),
            256 => Some(Self::Kbps256),
            320 => Some(Self::Kbps320),
            _ => None,
        }
    }

    pub fn next(self) -> Self {
        let index = Self::VALUES
            .iter()
            .position(|value| *value == self)
            .unwrap_or_default();
        Self::VALUES[(index + 1) % Self::VALUES.len()]
    }
}

impl fmt::Display for Bitrate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_path_segment())
    }
}

impl Config {
    pub fn load(cli: &Cli) -> Result<Self> {
        let config_dir = dirs::config_dir()
            .unwrap_or_else(fallback_base_dir)
            .join("ibroadcast-tui");
        let config_file = config_dir.join("config.toml");

        let file: ConfigFile = if config_file.exists() {
            toml::from_str(&fs::read_to_string(&config_file)?)?
        } else {
            ConfigFile::default()
        };

        Ok(Self {
            config_dir,
            config_file,
            client_id: cli
                .client_id
                .as_deref()
                .and_then(nonempty_trimmed)
                .map(str::to_owned)
                .or(file.client_id),
            client_secret: cli
                .client_secret
                .as_deref()
                .and_then(nonempty_trimmed)
                .map(str::to_owned),
            download_dir: cli
                .download_dir
                .clone()
                .or(file.download_dir)
                .unwrap_or_else(default_download_dir),
            playback_bitrate: cli.bitrate.or(file.playback_bitrate),
            download_bitrate: file.download_bitrate.unwrap_or(Bitrate::Original),
            plain_token_file: file.plain_token_file.unwrap_or(false),
        })
    }

    pub fn save(&self) -> Result<()> {
        let file = ConfigFile {
            client_id: self.client_id.clone(),
            download_dir: Some(self.download_dir.clone()),
            playback_bitrate: self.playback_bitrate,
            download_bitrate: Some(self.download_bitrate),
            plain_token_file: Some(self.plain_token_file),
        };
        fs::create_dir_all(&self.config_dir)?;
        fs::write(&self.config_file, toml::to_string_pretty(&file)?)?;
        Ok(())
    }
}

fn default_download_dir() -> PathBuf {
    dirs::audio_dir()
        .or_else(dirs::download_dir)
        .unwrap_or_else(fallback_base_dir)
        .join("iBroadcast")
}

fn fallback_base_dir() -> PathBuf {
    std::env::current_dir().unwrap_or_else(|_| Path::new(".").to_path_buf())
}

fn nonempty_trimmed(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then_some(trimmed)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::{Bitrate, ConfigFile};

    #[test]
    fn config_files_written_by_older_versions_still_parse() {
        // Older versions always wrote every field plus a `cache_dir`.
        let text = r#"
            client_id = "abc"
            download_dir = "C:\\Music"
            playback_bitrate = "128"
            download_bitrate = "orig"
            cache_dir = "C:\\Cache"
            plain_token_file = false
        "#;
        let file: ConfigFile = toml::from_str(text).unwrap();
        assert_eq!(file.client_id.as_deref(), Some("abc"));
        assert_eq!(file.playback_bitrate, Some(Bitrate::Kbps128));
        assert_eq!(file.download_bitrate, Some(Bitrate::Original));
    }

    #[test]
    fn missing_fields_stay_unset() {
        let file: ConfigFile = toml::from_str("").unwrap();
        assert_eq!(file.playback_bitrate, None);
        assert_eq!(file.client_id, None);
    }

    #[test]
    fn bitrates_cycle_through_all_values() {
        let mut bitrate = Bitrate::Kbps96;
        for expected in Bitrate::VALUES.into_iter().cycle().skip(1).take(6) {
            bitrate = bitrate.next();
            assert_eq!(bitrate, expected);
        }
    }
}
