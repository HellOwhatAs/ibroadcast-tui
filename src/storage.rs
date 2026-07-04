use std::{
    fs,
    path::{Path, PathBuf},
};

use keyring::Entry;

use crate::{downloads::sanitize_component, error::Result, oauth::TokenSet};

const KEYRING_SERVICE: &str = "ibroadcast-tui";

#[derive(Clone, Debug)]
pub enum TokenPersistence {
    Keyring,
    KeyringWithPlainBackup(PathBuf),
    PlainFile(PathBuf),
}

#[derive(Clone, Debug)]
pub struct TokenStore {
    config_dir: PathBuf,
    prefer_plain_file: bool,
}

impl TokenStore {
    pub fn new(config_dir: PathBuf, prefer_plain_file: bool) -> Self {
        Self {
            config_dir,
            prefer_plain_file,
        }
    }

    pub fn load(&self, client_id: &str) -> Result<Option<TokenSet>> {
        if !self.prefer_plain_file
            && let Ok(entry) = Entry::new(KEYRING_SERVICE, client_id)
            && let Ok(secret) = entry.get_password()
        {
            return Ok(Some(serde_json::from_str(&secret)?));
        }

        let path = self.plain_path(client_id);
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(serde_json::from_str(&fs::read_to_string(path)?)?))
    }

    pub fn save(&self, client_id: &str, token: &TokenSet) -> Result<TokenPersistence> {
        let secret = serde_json::to_string_pretty(token)?;
        let plain_file = self.write_plain_token(client_id, &secret);

        if !self.prefer_plain_file
            && let Ok(entry) = Entry::new(KEYRING_SERVICE, client_id)
            && entry.set_password(&secret).is_ok()
            && entry.get_password().is_ok_and(|value| value == secret)
        {
            return Ok(match plain_file {
                Ok(path) => TokenPersistence::KeyringWithPlainBackup(path),
                Err(_) => TokenPersistence::Keyring,
            });
        }

        Ok(TokenPersistence::PlainFile(plain_file?))
    }

    fn write_plain_token(&self, client_id: &str, secret: &str) -> Result<PathBuf> {
        let path = self.plain_path(client_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, secret)?;
        Ok(path)
    }

    pub fn delete(&self, client_id: &str) {
        if let Ok(entry) = Entry::new(KEYRING_SERVICE, client_id) {
            let _ = entry.delete_credential();
        }
        let _ = fs::remove_file(self.plain_path(client_id));
    }

    fn plain_path(&self, client_id: &str) -> PathBuf {
        self.config_dir
            .join("tokens")
            .join(format!("{}.json", safe_token_file_name(client_id)))
    }
}

fn safe_token_file_name(client_id: &str) -> String {
    let sanitized = sanitize_component(client_id);
    Path::new(&sanitized)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("token")
        .to_owned()
}
