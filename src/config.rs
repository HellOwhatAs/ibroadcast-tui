use std::{
    fmt, fs,
    path::{Path, PathBuf},
};

use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};

use crate::{error::Result, queue::PlaybackMode};

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
    playback_mode: Option<PlaybackMode>,
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
    pub playback_mode: PlaybackMode,
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

    /// The bitrates the server stores as complete downloadable files. All
    /// other bitrates exist only as HLS segment streams.
    pub const DOWNLOAD_VALUES: [Self; 2] = [Self::Kbps128, Self::Original];

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

    /// Path segment used when building a streaming URL. The server only
    /// serves complete files at 128 kbps and in the original format; the
    /// transcoded bitrates are published as HLS playlists, exactly like the
    /// official web player requests them.
    pub fn stream_path_segment(self) -> &'static str {
        match self {
            Self::Kbps96 => "hls_96",
            Self::Kbps128 => "128",
            Self::Kbps192 => "hls_192",
            Self::Kbps256 => "hls_256",
            Self::Kbps320 => "hls_320",
            Self::Original => "orig",
        }
    }

    /// True when streaming this bitrate goes through an HLS playlist rather
    /// than a single progressive file.
    pub fn is_hls_stream(self) -> bool {
        !matches!(self, Self::Kbps128 | Self::Original)
    }

    /// Target bandwidth in bits per second, used to pick a variant from an
    /// HLS master playlist.
    pub fn target_bandwidth(self) -> u64 {
        match self {
            Self::Kbps96 => 96_000,
            Self::Kbps128 => 128_000,
            Self::Kbps192 => 192_000,
            Self::Kbps256 => 256_000,
            Self::Kbps320 => 320_000,
            Self::Original => u64::MAX,
        }
    }

    /// Maps a bitrate to one the server can serve as a complete file:
    /// transcoded bitrates fall back to 128 kbps.
    pub fn nearest_download(self) -> Self {
        if Self::DOWNLOAD_VALUES.contains(&self) {
            self
        } else {
            Self::Kbps128
        }
    }

    pub fn next_download(self) -> Self {
        match self {
            Self::Kbps128 => Self::Original,
            _ => Self::Kbps128,
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
            playback_mode: file.playback_mode.unwrap_or_default(),
            download_bitrate: file
                .download_bitrate
                .unwrap_or(Bitrate::Original)
                .nearest_download(),
            plain_token_file: file.plain_token_file.unwrap_or(false),
        })
    }

    pub fn save(&self) -> Result<()> {
        let file = ConfigFile {
            client_id: self.client_id.clone(),
            download_dir: Some(self.download_dir.clone()),
            playback_bitrate: self.playback_bitrate,
            playback_mode: Some(self.playback_mode),
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

    use crate::queue::PlaybackMode;

    use super::{Bitrate, ConfigFile};

    #[test]
    fn config_files_written_by_older_versions_still_parse() {
        // Older versions always wrote every field plus a `cache_dir`.
        let text = r#"
            client_id = "abc"
            download_dir = "C:\\Music"
            playback_bitrate = "128"
            playback_mode = "shuffle"
            download_bitrate = "orig"
            cache_dir = "C:\\Cache"
            plain_token_file = false
        "#;
        let file: ConfigFile = toml::from_str(text).unwrap();
        assert_eq!(file.client_id.as_deref(), Some("abc"));
        assert_eq!(file.playback_bitrate, Some(Bitrate::Kbps128));
        assert_eq!(file.playback_mode, Some(PlaybackMode::Shuffle));
        assert_eq!(file.download_bitrate, Some(Bitrate::Original));
    }

    #[test]
    fn missing_fields_stay_unset() {
        let file: ConfigFile = toml::from_str("").unwrap();
        assert_eq!(file.playback_bitrate, None);
        assert_eq!(file.playback_mode, None);
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

    #[test]
    fn transcoded_bitrates_stream_over_hls() {
        assert!(Bitrate::Kbps96.is_hls_stream());
        assert!(Bitrate::Kbps320.is_hls_stream());
        assert!(!Bitrate::Kbps128.is_hls_stream());
        assert!(!Bitrate::Original.is_hls_stream());
        assert_eq!(Bitrate::Kbps192.stream_path_segment(), "hls_192");
        assert_eq!(Bitrate::Kbps128.stream_path_segment(), "128");
        assert_eq!(Bitrate::Original.stream_path_segment(), "orig");
    }

    #[test]
    fn download_bitrates_are_limited_to_complete_files() {
        assert_eq!(Bitrate::Kbps96.nearest_download(), Bitrate::Kbps128);
        assert_eq!(Bitrate::Kbps320.nearest_download(), Bitrate::Kbps128);
        assert_eq!(Bitrate::Kbps128.nearest_download(), Bitrate::Kbps128);
        assert_eq!(Bitrate::Original.nearest_download(), Bitrate::Original);
        assert_eq!(Bitrate::Kbps128.next_download(), Bitrate::Original);
        assert_eq!(Bitrate::Original.next_download(), Bitrate::Kbps128);
    }
}
