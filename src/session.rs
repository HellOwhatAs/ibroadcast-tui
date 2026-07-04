use reqwest::Client;

use crate::{
    api::{ApiClient, ApiSettings},
    config::Bitrate,
    error::{AppError, Result},
    library::{Library, Track},
    oauth::TokenSet,
    storage::TokenStore,
};

/// An authenticated iBroadcast session.
///
/// Owns the API client, the account settings, and — crucially — the sole
/// responsibility for persisting refreshed tokens. Any operation that may
/// refresh the access token synchronizes the token store afterwards, so no
/// caller has to remember to do it.
#[derive(Debug)]
pub struct Session {
    api: ApiClient,
    settings: ApiSettings,
    user_id: u64,
    client_id: String,
    store: TokenStore,
    persisted_access_token: String,
    /// Cleared at logout so that in-flight background tasks cannot re-persist
    /// a token after it has been deleted from the store.
    persistence_enabled: bool,
}

/// Everything produced by a successful login + library sync.
#[derive(Debug)]
pub struct EstablishedSession {
    pub session: Session,
    pub library: Library,
    /// The account-level playback bitrate preference, if the server reports one.
    pub server_bitrate: Option<Bitrate>,
}

impl Session {
    pub async fn establish(
        http: Client,
        client_id: String,
        client_secret: Option<String>,
        token: TokenSet,
        store: TokenStore,
    ) -> Result<EstablishedSession> {
        let mut persisted = token.access_token.clone();
        let mut api = ApiClient::new(http, client_id.clone(), client_secret, token);

        // Any of these calls can refresh the token internally. Persist after
        // each one — even on failure — so a rotated refresh token is never
        // lost when a later step errors out.
        let status = api.status().await;
        persist_if_changed(&store, &client_id, &api, &mut persisted);
        let status = status?;
        let user_id = status
            .user_id
            .ok_or_else(|| AppError::Api("status response omitted user id".to_owned()))?;

        let server_bitrate = api.get_bitrate_pref().await.ok().flatten();
        persist_if_changed(&store, &client_id, &api, &mut persisted);

        let library_response = api.sync_library().await;
        persist_if_changed(&store, &client_id, &api, &mut persisted);
        let library_response = library_response?;

        let mut settings = status.settings;
        settings.merge_from(library_response.settings);

        Ok(EstablishedSession {
            session: Self {
                api,
                settings,
                user_id,
                client_id,
                store,
                persisted_access_token: persisted,
                persistence_enabled: true,
            },
            library: library_response.library,
            server_bitrate,
        })
    }

    /// Builds a signed streaming URL, refreshing and persisting the token first
    /// if needed.
    pub async fn stream_url(&mut self, track: &Track, bitrate: Bitrate) -> Result<String> {
        let refreshed = self.api.ensure_token().await;
        self.sync_token_store();
        refreshed?;
        self.api
            .stream_context(&self.settings, self.user_id)
            .build_stream_url(track, bitrate)
    }

    pub fn refresh_token(&self) -> &str {
        &self.api.token().refresh_token
    }

    /// Stops this session from writing tokens to the store. Called at logout,
    /// before the stored token is deleted and revoked.
    pub fn disable_persistence(&mut self) {
        self.persistence_enabled = false;
    }

    fn sync_token_store(&mut self) {
        if !self.persistence_enabled {
            return;
        }
        persist_if_changed(
            &self.store,
            &self.client_id,
            &self.api,
            &mut self.persisted_access_token,
        );
    }
}

fn persist_if_changed(
    store: &TokenStore,
    client_id: &str,
    api: &ApiClient,
    persisted_access_token: &mut String,
) {
    let current = &api.token().access_token;
    if current == persisted_access_token {
        return;
    }
    match store.save(client_id, api.token()) {
        Ok(_) => *persisted_access_token = current.clone(),
        Err(err) => tracing::warn!("failed to persist refreshed token: {err}"),
    }
}
