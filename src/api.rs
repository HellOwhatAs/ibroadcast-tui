use chrono::Utc;
use reqwest::Client;
use serde_json::{Value, json};

use crate::{
    config::Bitrate,
    error::{AppError, Result},
    library::{Library, Track, value_to_u64},
    oauth::{self, TokenSet},
};

const CLIENT_NAME: &str = "ibroadcast-tui";
const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone, Debug)]
pub struct ApiSettings {
    pub streaming_server: String,
    pub artwork_server: String,
}

impl Default for ApiSettings {
    fn default() -> Self {
        Self {
            streaming_server: "https://streaming.ibroadcast.com".to_owned(),
            artwork_server: "https://artwork.ibroadcast.com".to_owned(),
        }
    }
}

impl ApiSettings {
    fn from_response(value: &Value) -> Self {
        let mut settings = Self::default();
        if let Some(object) = value.get("settings").and_then(Value::as_object) {
            if let Some(server) = object.get("streaming_server").and_then(Value::as_str) {
                settings.streaming_server = server.trim_end_matches('/').to_owned();
            }
            if let Some(server) = object.get("artwork_server").and_then(Value::as_str) {
                settings.artwork_server = server.trim_end_matches('/').to_owned();
            }
        }
        settings
    }

    pub fn merge_from(&mut self, other: Self) {
        if !other.streaming_server.is_empty() {
            self.streaming_server = other.streaming_server;
        }
        if !other.artwork_server.is_empty() {
            self.artwork_server = other.artwork_server;
        }
    }
}

#[derive(Debug)]
pub struct StatusResponse {
    pub user_id: Option<u64>,
    pub settings: ApiSettings,
}

#[derive(Debug)]
pub struct LibraryResponse {
    pub library: Library,
    pub settings: ApiSettings,
}

/// Authenticated JSON API client. Refreshes its token transparently; callers
/// that persist tokens should compare [`ApiClient::token`] before and after
/// calls (see `session::Session`).
#[derive(Debug)]
pub struct ApiClient {
    http: Client,
    client_id: String,
    client_secret: Option<String>,
    token: TokenSet,
    device_name: String,
    user_agent: String,
}

#[derive(Clone, Debug)]
pub struct StreamContext {
    pub streaming_server: String,
    pub access_token: String,
    pub user_id: u64,
    pub platform: String,
    pub version: String,
}

impl ApiClient {
    pub fn new(
        http: Client,
        client_id: String,
        client_secret: Option<String>,
        token: TokenSet,
    ) -> Self {
        let device_name = std::env::var("COMPUTERNAME")
            .or_else(|_| std::env::var("HOSTNAME"))
            .unwrap_or_else(|_| "Terminal".to_owned());

        Self {
            http,
            client_id,
            client_secret,
            token,
            device_name,
            user_agent: format!("{CLIENT_NAME}/{VERSION}"),
        }
    }

    pub fn token(&self) -> &TokenSet {
        &self.token
    }

    pub async fn ensure_token(&mut self) -> Result<()> {
        if self.token.is_expired() {
            self.refresh_token().await?;
        }
        Ok(())
    }

    async fn refresh_token(&mut self) -> Result<()> {
        self.token = oauth::refresh_access_token(
            &self.http,
            &self.client_id,
            self.client_secret.as_deref(),
            &self.token.refresh_token,
        )
        .await?;
        Ok(())
    }

    pub async fn status(&mut self) -> Result<StatusResponse> {
        let value = self
            .json_request("status", "https://api.ibroadcast.com/status")
            .await?;
        Ok(StatusResponse {
            user_id: find_user_id(&value),
            settings: ApiSettings::from_response(&value),
        })
    }

    pub async fn get_bitrate_pref(&mut self) -> Result<Option<Bitrate>> {
        let value = self
            .json_request("getbitratepref", "https://api.ibroadcast.com/getbitratepref")
            .await?;
        Ok(value
            .get("bitrate")
            .and_then(Value::as_u64)
            .and_then(Bitrate::from_u64))
    }

    pub async fn sync_library(&mut self) -> Result<LibraryResponse> {
        let value = self
            .json_request("library", "https://library.ibroadcast.com")
            .await?;
        let library_value = value
            .get("library")
            .ok_or_else(|| AppError::Library("response omitted library".to_owned()))?;
        Ok(LibraryResponse {
            library: Library::from_value(library_value)?,
            settings: ApiSettings::from_response(&value),
        })
    }

    pub fn stream_context(&self, settings: &ApiSettings, user_id: u64) -> StreamContext {
        StreamContext {
            streaming_server: settings.streaming_server.trim_end_matches('/').to_owned(),
            access_token: self.token.access_token.clone(),
            user_id,
            platform: CLIENT_NAME.to_owned(),
            version: VERSION.to_owned(),
        }
    }

    async fn json_request(&mut self, mode: &str, url: &str) -> Result<Value> {
        self.ensure_token().await?;
        let body = self.request_body(mode);
        let mut value = self.post_json(url, &body).await?;

        // The API can report a stale token even when it has not expired
        // locally; refresh once and retry.
        if value.get("authenticated").and_then(Value::as_bool) == Some(false) {
            self.refresh_token().await?;
            value = self.post_json(url, &body).await?;
        }

        if value.get("result").and_then(Value::as_bool) == Some(false) {
            let message = value
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("operation failed");
            return Err(AppError::Api(message.to_owned()));
        }

        Ok(value)
    }

    fn request_body(&self, mode: &str) -> Value {
        json!({
            "client": CLIENT_NAME,
            "version": VERSION,
            "device_name": self.device_name,
            "user_agent": self.user_agent,
            "mode": mode,
        })
    }

    async fn post_json(&self, url: &str, body: &Value) -> Result<Value> {
        let response = self
            .http
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::USER_AGENT, &self.user_agent)
            .bearer_auth(&self.token.access_token)
            .body(serde_json::to_vec(body)?)
            .send()
            .await?;

        let status = response.status();
        let text = response.text().await?;
        if !status.is_success() {
            return Err(AppError::Api(format!("server returned {status}: {text}")));
        }
        Ok(serde_json::from_str(&text)?)
    }
}

impl StreamContext {
    pub fn build_stream_url(&self, track: &Track, bitrate: Bitrate) -> Result<String> {
        let path = track_file_with_bitrate(&track.file, bitrate)?;
        let file_id = file_id_from_track_file(&track.file)?;
        let expires = Utc::now().timestamp_millis();
        Ok(format!(
            "{}{}?Expires={}&Signature={}&file_id={}&user_id={}&platform={}&version={}",
            self.streaming_server,
            path,
            expires,
            urlencoding::encode(&self.access_token),
            file_id,
            self.user_id,
            urlencoding::encode(&self.platform),
            urlencoding::encode(&self.version)
        ))
    }
}

fn track_file_with_bitrate(track_file: &str, bitrate: Bitrate) -> Result<String> {
    let trimmed = track_file.trim_matches('/');
    let mut parts: Vec<&str> = trimmed.split('/').filter(|part| !part.is_empty()).collect();
    if parts.len() < 2 {
        return Err(AppError::Api(format!(
            "invalid track file path: {track_file}"
        )));
    }
    parts[0] = bitrate.as_path_segment();
    Ok(format!("/{}", parts.join("/")))
}

fn file_id_from_track_file(track_file: &str) -> Result<u64> {
    track_file
        .trim_matches('/')
        .rsplit('/')
        .next()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| AppError::Api(format!("invalid track file path: {track_file}")))
}

fn find_user_id(value: &Value) -> Option<u64> {
    [
        value.pointer("/user/id"),
        value.pointer("/user/user_id"),
        value.pointer("/status/user_id"),
        value.pointer("/status/id"),
        value.get("user_id"),
    ]
    .into_iter()
    .flatten()
    .find_map(value_to_u64)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use crate::{config::Bitrate, library::Track};

    use super::{StreamContext, file_id_from_track_file, track_file_with_bitrate};

    #[test]
    fn rewrites_track_file_bitrate() {
        assert_eq!(
            track_file_with_bitrate("/128/d0c/6f4/21127414", Bitrate::Kbps320).unwrap(),
            "/320/d0c/6f4/21127414"
        );
        assert_eq!(
            track_file_with_bitrate("/128/d0c/6f4/21127414", Bitrate::Original).unwrap(),
            "/orig/d0c/6f4/21127414"
        );
    }

    #[test]
    fn extracts_file_id() {
        assert_eq!(
            file_id_from_track_file("/128/d0c/6f4/21127414").unwrap(),
            21127414
        );
    }

    #[test]
    fn builds_stream_url() {
        let context = StreamContext {
            streaming_server: "https://streaming.ibroadcast.com".to_owned(),
            access_token: "abc def".to_owned(),
            user_id: 42,
            platform: "ibroadcast-tui".to_owned(),
            version: "0.1.0".to_owned(),
        };
        let track = Track {
            file: "/128/d0c/6f4/21127414".to_owned(),
            ..Track::default()
        };
        let url = context.build_stream_url(&track, Bitrate::Kbps128).unwrap();
        assert!(url.starts_with("https://streaming.ibroadcast.com/128/d0c/6f4/21127414?"));
        assert!(url.contains("Signature=abc%20def"));
        assert!(url.contains("file_id=21127414"));
        assert!(url.contains("user_id=42"));
    }
}
