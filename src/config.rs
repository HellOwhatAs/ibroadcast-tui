use std::{
    env, fmt, fs,
    path::{Path, PathBuf},
};

use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};

use crate::error::Result;

#[derive(Debug, Parser)]
#[command(version, about)]
pub struct Cli {
    #[arg(long, env = "IBROADCAST_CLIENT_ID", hide_env_values = true)]
    pub client_id: Option<String>,
    #[arg(long, env = "IBROADCAST_CLIENT_SECRET", hide_env_values = true)]
    pub client_secret: Option<String>,
    #[arg(long)]
    pub download_dir: Option<PathBuf>,
    #[arg(long, value_enum)]
    pub bitrate: Option<Bitrate>,
    #[arg(long, default_value = "warn")]
    pub log_level: String,
}

#[derive(Clone, Debug)]
pub struct ConfigPaths {
    pub config_dir: PathBuf,
    pub config_file: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct AppConfig {
    pub client_id: Option<String>,
    #[serde(skip)]
    pub client_secret: Option<String>,
    pub download_dir: PathBuf,
    pub playback_bitrate: Bitrate,
    pub download_bitrate: Bitrate,
    pub cache_dir: PathBuf,
    pub plain_token_file: bool,
    #[serde(skip)]
    pub playback_bitrate_explicit: bool,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
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

impl Default for AppConfig {
    fn default() -> Self {
        let app_data = dirs::data_dir()
            .unwrap_or_else(fallback_base_dir)
            .join("ibroadcast-tui");
        let cache_dir = dirs::cache_dir()
            .unwrap_or_else(|| app_data.join("cache"))
            .join("ibroadcast-tui");
        let download_dir = dirs::audio_dir()
            .or_else(dirs::download_dir)
            .unwrap_or_else(|| app_data.join("downloads"))
            .join("iBroadcast");

        Self {
            client_id: None,
            client_secret: None,
            download_dir,
            playback_bitrate: Bitrate::Kbps128,
            download_bitrate: Bitrate::Original,
            cache_dir,
            plain_token_file: false,
            playback_bitrate_explicit: false,
        }
    }
}

impl AppConfig {
    pub fn load(cli: &Cli) -> Result<(Self, ConfigPaths)> {
        let config_dir = dirs::config_dir()
            .unwrap_or_else(fallback_base_dir)
            .join("ibroadcast-tui");
        let config_file = config_dir.join("config.toml");

        let (mut config, config_had_playback_bitrate) = if config_file.exists() {
            let text = fs::read_to_string(&config_file)?;
            let mut config: Self = toml::from_str(&text)?;
            let explicit = text.contains("playback_bitrate");
            config.playback_bitrate_explicit = explicit;
            (config, explicit)
        } else {
            (Self::default(), false)
        };
        config.playback_bitrate_explicit = config_had_playback_bitrate;

        if let Some(client_id) = cli
            .client_id
            .as_ref()
            .filter(|value| !value.trim().is_empty())
        {
            config.client_id = Some(client_id.trim().to_owned());
        }
        config.client_secret = cli
            .client_secret
            .as_deref()
            .and_then(nonempty_trimmed)
            .map(str::to_owned)
            .or_else(|| env_secret("IBEOADCAST_CLIENT_SECRET"));
        if let Some(download_dir) = &cli.download_dir {
            config.download_dir = download_dir.clone();
        }
        if let Some(bitrate) = cli.bitrate {
            config.playback_bitrate = bitrate;
            config.playback_bitrate_explicit = true;
        }

        Ok((
            config,
            ConfigPaths {
                config_dir,
                config_file,
            },
        ))
    }

    pub fn save(&self, paths: &ConfigPaths) -> Result<()> {
        fs::create_dir_all(&paths.config_dir)?;
        let text = toml::to_string_pretty(self)?;
        fs::write(&paths.config_file, text)?;
        Ok(())
    }
}

fn fallback_base_dir() -> PathBuf {
    std::env::current_dir().unwrap_or_else(|_| Path::new(".").to_path_buf())
}

fn nonempty_trimmed(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then_some(trimmed)
}

fn env_secret(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .and_then(|value| nonempty_trimmed(&value).map(str::to_owned))
}
