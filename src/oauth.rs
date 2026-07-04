use std::time::{Duration, Instant};

use chrono::Utc;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use crate::error::{AppError, Result};

const DEVICE_CODE_URL: &str = "https://oauth.ibroadcast.com/device/code";
const TOKEN_URL: &str = "https://oauth.ibroadcast.com/token";
const REVOKE_URL: &str = "https://oauth.ibroadcast.com/revoke";
const RFC8628_DEVICE_GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:device_code";
const IBROADCAST_DEVICE_GRANT_TYPE: &str = "device_code";

#[derive(Clone, Debug, Deserialize)]
pub struct DeviceCode {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    pub verification_uri_complete: Option<String>,
    pub interval: Option<u64>,
    pub expires_in: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TokenSet {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: i64,
    pub scope: Vec<String>,
}

impl TokenSet {
    pub fn is_expired(&self) -> bool {
        Utc::now().timestamp() >= self.expires_at.saturating_sub(60)
    }

    fn from_response(response: RawTokenResponse, previous_refresh: Option<&str>) -> Result<Self> {
        let refresh_token = response
            .refresh_token
            .or_else(|| previous_refresh.map(str::to_owned))
            .ok_or_else(|| AppError::Auth("token response omitted refresh_token".to_owned()))?;
        Ok(Self {
            access_token: response.access_token,
            refresh_token,
            expires_at: Utc::now().timestamp() + response.expires_in,
            scope: response.scope.map(ScopeField::into_vec).unwrap_or_default(),
        })
    }
}

#[derive(Debug, Deserialize)]
struct RawTokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    expires_in: i64,
    scope: Option<ScopeField>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ScopeField {
    SpaceDelimited(String),
    List(Vec<String>),
}

impl ScopeField {
    fn into_vec(self) -> Vec<String> {
        match self {
            Self::SpaceDelimited(value) => value
                .split_whitespace()
                .filter(|scope| !scope.is_empty())
                .map(str::to_owned)
                .collect(),
            Self::List(values) => values,
        }
    }
}

#[derive(Debug, Deserialize)]
struct OAuthErrorResponse {
    error: Option<String>,
    error_description: Option<String>,
}

pub async fn request_device_code(
    http: &Client,
    client_id: &str,
    scopes: &[&str],
) -> Result<DeviceCode> {
    let response = http
        .get(DEVICE_CODE_URL)
        .query(&[("client_id", client_id), ("scope", &scopes.join(" "))])
        .send()
        .await?;

    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        return Err(AppError::auth_response(status, &text));
    }

    Ok(serde_json::from_str(&text)?)
}

pub async fn poll_for_token(
    http: &Client,
    client_id: &str,
    client_secret: Option<&str>,
    device_code: &str,
    interval_seconds: u64,
    expires_in: Option<u64>,
) -> Result<TokenSet> {
    let started = Instant::now();
    let timeout = expires_in.map(Duration::from_secs);
    let mut interval = interval_seconds.max(1);
    let mut grant_type = RFC8628_DEVICE_GRANT_TYPE;
    let mut fallback_grant_type = Some(IBROADCAST_DEVICE_GRANT_TYPE);
    let mut sleep_before_poll = true;

    loop {
        if timeout.is_some_and(|limit| started.elapsed() >= limit) {
            return Err(AppError::Auth(
                "device code expired before authorization completed".to_owned(),
            ));
        }

        if sleep_before_poll {
            sleep(Duration::from_secs(interval)).await;
        } else {
            sleep_before_poll = true;
        }

        let mut form = vec![
            ("grant_type", grant_type),
            ("client_id", client_id),
            ("device_code", device_code),
        ];
        add_client_secret(&mut form, client_secret);

        let response = http.post(TOKEN_URL).form(&form).send().await?;

        let status = response.status();
        let text = response.text().await?;
        if status.is_success() {
            return TokenSet::from_response(serde_json::from_str(&text)?, None);
        }

        let error = oauth_error_from_text(&text);
        if is_invalid_grant_type(error.as_ref(), &text)
            && let Some(next_grant_type) = fallback_grant_type.take()
        {
            grant_type = next_grant_type;
            sleep_before_poll = false;
            tracing::debug!(
                "OAuth token endpoint rejected device-code grant type; retrying with fallback"
            );
            continue;
        }

        match error.as_ref().and_then(|error| error.error.as_deref()) {
            Some("authorization_pending") => {}
            Some("slow_down") => interval += 5,
            Some("expired_token") => {
                return Err(AppError::Auth(
                    "device code expired; restart login".to_owned(),
                ));
            }
            Some("access_denied") => {
                return Err(AppError::Auth("authorization was denied".to_owned()));
            }
            Some(code) => {
                let description = error
                    .as_ref()
                    .and_then(|error| error.error_description.as_deref())
                    .unwrap_or("");
                if code == "invalid_client" {
                    return Err(AppError::Auth(format!(
                        "{code}: {description}. Check your client_id; if your iBroadcast app requires a secret, set IBROADCAST_CLIENT_SECRET."
                    )));
                }
                return Err(AppError::Auth(format!("{code}: {description}")));
            }
            None => return Err(AppError::auth_response(status, &text)),
        }
    }
}

pub async fn refresh_access_token(
    http: &Client,
    client_id: &str,
    client_secret: Option<&str>,
    refresh_token: &str,
) -> Result<TokenSet> {
    let mut form = vec![
        ("grant_type", "refresh_token"),
        ("client_id", client_id),
        ("refresh_token", refresh_token),
    ];
    add_client_secret(&mut form, client_secret);

    let response = http.post(TOKEN_URL).form(&form).send().await?;

    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        return Err(AppError::auth_response(status, &text));
    }

    TokenSet::from_response(serde_json::from_str(&text)?, Some(refresh_token))
}

pub async fn revoke_token(
    http: &Client,
    client_id: &str,
    client_secret: Option<&str>,
    refresh_token: &str,
) -> Result<()> {
    let mut form = vec![("refresh_token", refresh_token), ("client_id", client_id)];
    add_client_secret(&mut form, client_secret);

    let response = http.post(REVOKE_URL).form(&form).send().await?;

    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        return Err(AppError::auth_response(status, &text));
    }
    Ok(())
}

fn add_client_secret<'a>(form: &mut Vec<(&'static str, &'a str)>, client_secret: Option<&'a str>) {
    if let Some(client_secret) = client_secret
        .map(str::trim)
        .filter(|client_secret| !client_secret.is_empty())
    {
        form.push(("client_secret", client_secret));
    }
}

fn oauth_error_from_text(text: &str) -> Option<OAuthErrorResponse> {
    if let Ok(error) = serde_json::from_str::<OAuthErrorResponse>(text) {
        return Some(error);
    }

    for code in [
        "authorization_pending",
        "slow_down",
        "expired_token",
        "access_denied",
        "invalid_client",
    ] {
        if text.contains(code) {
            return Some(OAuthErrorResponse {
                error: Some(code.to_owned()),
                error_description: None,
            });
        }
    }

    None
}

fn is_invalid_grant_type(error: Option<&OAuthErrorResponse>, raw_text: &str) -> bool {
    let mut text = String::new();
    if let Some(error) = error {
        if let Some(code) = &error.error {
            text.push_str(code);
            text.push(' ');
        }
        if let Some(description) = &error.error_description {
            text.push_str(description);
            text.push(' ');
        }
    }
    text.push_str(raw_text);

    let lower = text.to_ascii_lowercase();
    lower.contains("grant_type") && lower.contains("invalid")
}

#[cfg(test)]
mod tests {
    use super::{
        OAuthErrorResponse, TokenSet, add_client_secret, is_invalid_grant_type,
        oauth_error_from_text,
    };

    #[test]
    fn expired_tokens_use_a_clock_skew() {
        let token = TokenSet {
            access_token: "a".to_owned(),
            refresh_token: "r".to_owned(),
            expires_at: chrono::Utc::now().timestamp() + 30,
            scope: vec![],
        };
        assert!(token.is_expired());
    }

    #[test]
    fn client_secret_is_added_only_when_present() {
        let mut form = vec![("grant_type", "device_code"), ("client_id", "id")];
        add_client_secret(&mut form, Some(" secret "));
        assert_eq!(form.last(), Some(&("client_secret", "secret")));

        let mut form = vec![("grant_type", "device_code"), ("client_id", "id")];
        add_client_secret(&mut form, Some("   "));
        assert!(!form.iter().any(|(key, _)| *key == "client_secret"));
    }

    #[test]
    fn oauth_pending_error_can_be_detected_from_plain_text() {
        let error = oauth_error_from_text("authorization_pending").unwrap();
        assert_eq!(error.error.as_deref(), Some("authorization_pending"));
    }

    #[test]
    fn invalid_grant_type_errors_are_detected_from_description() {
        let error = OAuthErrorResponse {
            error: Some("invalid_request".to_owned()),
            error_description: Some("grant_type is invalid".to_owned()),
        };
        assert!(is_invalid_grant_type(Some(&error), ""));
        assert!(is_invalid_grant_type(
            None,
            r#"{"error":"grant_type is invalid"}"#
        ));
    }
}
