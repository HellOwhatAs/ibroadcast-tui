use std::path::PathBuf;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, AppError>;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("api error: {0}")]
    Api(String),
    #[error("authentication error: {0}")]
    Auth(String),
    #[error("download failed for {path}: {source}")]
    Download {
        path: PathBuf,
        source: Box<AppError>,
    },
    #[error("invalid library response: {0}")]
    Library(String),
    #[error("missing token; log in again")]
    MissingToken,
    #[error("playback error: {0}")]
    Playback(String),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("toml decode error: {0}")]
    TomlDecode(#[from] toml::de::Error),
    #[error("toml encode error: {0}")]
    TomlEncode(#[from] toml::ser::Error),
}

impl AppError {
    pub fn auth_response(status: reqwest::StatusCode, body: &str) -> Self {
        Self::Auth(format!("server returned {status}: {body}"))
    }
}
