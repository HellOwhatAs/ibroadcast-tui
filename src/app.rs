use std::{
    collections::HashSet,
    fs, io,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyEventKind,
        KeyModifiers,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, Borders, Clear, List, ListItem, ListState, Paragraph, Row, Table, TableState, Tabs,
        Wrap,
    },
};
use reqwest::Client;
use tokio::sync::mpsc;

use crate::{
    api::{ApiClient, ApiSettings},
    config::{AppConfig, Bitrate, ConfigPaths},
    download::{
        DownloadStatus, DownloadTask, build_download_path, download_to_file, extension_from_mime,
        stream_to_buffer,
    },
    error::{AppError, Result},
    library::{Library, Track},
    oauth::{self, DeviceCode, TokenSet},
    playback::PlaybackController,
    progressive::ProgressiveBuffer,
    queue::PlaybackQueue,
    storage::{TokenPersistence, TokenStore},
};

const SCOPES: &[&str] = &["user.library:read", "user.account:read"];

pub struct App {
    config: AppConfig,
    paths: ConfigPaths,
    token_store: TokenStore,
    http: Client,
    tx: mpsc::UnboundedSender<BackendEvent>,
    rx: mpsc::UnboundedReceiver<BackendEvent>,
    phase: Phase,
    client_id_input: String,
    search_input: String,
    search_mode: bool,
    active_view: View,
    selected: usize,
    queue_selected: usize,
    filtered_track_ids: Vec<u64>,
    downloaded_track_ids: HashSet<u64>,
    library: Option<Library>,
    api: Option<ApiClient>,
    settings: ApiSettings,
    user_id: Option<u64>,
    playback: Option<PlaybackController>,
    playback_bitrate: Bitrate,
    playback_download: Option<tokio::task::JoinHandle<()>>,
    playback_warning: Option<String>,
    queue: PlaybackQueue,
    downloads: Vec<DownloadTask>,
    next_download_id: u64,
    status_line: String,
    should_quit: bool,
}

#[derive(Clone, Debug)]
enum Phase {
    NeedClientId,
    RequestingDeviceCode,
    Authorizing(DeviceCode),
    LoadingLibrary,
    LoggingOut,
    Ready,
    Error(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum View {
    Library,
    Queue,
}

#[derive(Debug)]
enum BackendEvent {
    DeviceCode(Result<DeviceCode>),
    Token(Result<TokenSet>),
    Session(Box<Result<LoadedSession>>),
    DownloadFinished {
        task_id: u64,
        result: Result<PathBuf>,
    },
    Logout {
        client_id: String,
        result: Result<()>,
    },
}

#[derive(Debug)]
struct LoadedSession {
    api: ApiClient,
    library: Library,
    user_id: u64,
    settings: ApiSettings,
    preferred_bitrate: Option<Bitrate>,
}

impl App {
    pub fn new(config: AppConfig, paths: ConfigPaths, token_store: TokenStore) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let playback_bitrate = config.playback_bitrate;
        let playback = PlaybackController::new().ok();
        let playback_warning = if playback.is_none() {
            Some("No audio output device available; browsing and downloads still work".to_owned())
        } else {
            None
        };

        Self {
            config,
            paths,
            token_store,
            http: Client::new(),
            tx,
            rx,
            phase: Phase::NeedClientId,
            client_id_input: String::new(),
            search_input: String::new(),
            search_mode: false,
            active_view: View::Library,
            selected: 0,
            queue_selected: 0,
            filtered_track_ids: Vec::new(),
            downloaded_track_ids: HashSet::new(),
            library: None,
            api: None,
            settings: ApiSettings::default(),
            user_id: None,
            playback,
            playback_bitrate,
            playback_download: None,
            playback_warning,
            queue: PlaybackQueue::default(),
            downloads: Vec::new(),
            next_download_id: 1,
            status_line: String::new(),
            should_quit: false,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        self.bootstrap();

        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        let result = self.run_loop(&mut terminal).await;

        disable_raw_mode()?;
        execute!(
            terminal.backend_mut(),
            LeaveAlternateScreen,
            DisableMouseCapture
        )?;
        terminal.show_cursor()?;

        result
    }

    fn bootstrap(&mut self) {
        if let Some(client_id) = self.config.client_id.clone() {
            match self.token_store.load(&client_id) {
                Ok(Some(token)) => self.start_session_load(client_id, token),
                Ok(None) => self.start_authorization(client_id),
                Err(err) => self.phase = Phase::Error(err.to_string()),
            }
        } else {
            self.phase = Phase::NeedClientId;
            self.status_line = "Enter your iBroadcast OAuth client_id".to_owned();
        }
    }

    async fn run_loop(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    ) -> Result<()> {
        let mut last_tick = Instant::now();
        while !self.should_quit {
            terminal.draw(|frame| self.draw(frame))?;

            while let Ok(event) = self.rx.try_recv() {
                self.handle_backend_event(event);
            }

            if event::poll(Duration::from_millis(50))?
                && let Event::Key(key) = event::read()?
                && key.kind == KeyEventKind::Press
            {
                self.handle_key(key).await?;
            }

            if last_tick.elapsed() >= Duration::from_millis(250) {
                last_tick = Instant::now();
            }
        }
        Ok(())
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Result<()> {
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.should_quit = true;
            return Ok(());
        }

        if self.search_mode {
            self.handle_search_key(key);
            return Ok(());
        }

        match &self.phase {
            Phase::NeedClientId => self.handle_client_id_key(key),
            Phase::Error(_) => self.handle_error_key(key),
            Phase::Ready => self.handle_ready_key(key).await,
            Phase::Authorizing(_) | Phase::RequestingDeviceCode | Phase::LoadingLibrary => {
                if key.code == KeyCode::Char('q') {
                    self.should_quit = true;
                }
                Ok(())
            }
            Phase::LoggingOut => {
                if key.code == KeyCode::Char('q') {
                    self.should_quit = true;
                }
                Ok(())
            }
        }
    }

    fn handle_client_id_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Enter if !self.client_id_input.trim().is_empty() => {
                let client_id = self.client_id_input.trim().to_owned();
                self.config.client_id = Some(client_id.clone());
                self.config.save(&self.paths)?;
                self.start_authorization(client_id);
            }
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Char(ch) => self.client_id_input.push(ch),
            KeyCode::Backspace => {
                self.client_id_input.pop();
            }
            KeyCode::Esc => self.should_quit = true,
            _ => {}
        }
        Ok(())
    }

    fn handle_error_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Char('l') => {
                if let Some(client_id) = self.config.client_id.clone() {
                    self.token_store.delete(&client_id);
                    self.start_authorization(client_id);
                } else {
                    self.phase = Phase::NeedClientId;
                }
            }
            KeyCode::Char('q') | KeyCode::Esc => self.should_quit = true,
            _ => {}
        }
        Ok(())
    }

    fn handle_search_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Enter => {
                self.apply_search();
                self.search_mode = false;
            }
            KeyCode::Esc => self.search_mode = false,
            KeyCode::Backspace => {
                self.search_input.pop();
            }
            KeyCode::Char(ch) => self.search_input.push(ch),
            _ => {}
        }
    }

    async fn handle_ready_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Tab => self.next_view(),
            KeyCode::Char('/') => {
                self.search_mode = true;
                self.search_input.clear();
            }
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::Enter => self.activate_selected().await?,
            KeyCode::Char('a') => self.add_selected_to_queue(false),
            KeyCode::Char('A') => self.add_selected_to_queue(true),
            KeyCode::Delete | KeyCode::Char('x') => self.delete_or_remove_selected().await?,
            KeyCode::Char('[') | KeyCode::Char('<') => self.move_queue_item(-1),
            KeyCode::Char(']') | KeyCode::Char('>') => self.move_queue_item(1),
            KeyCode::Char('C') => self.clear_queue(),
            KeyCode::Char('b') => self.cycle_playback_bitrate(),
            KeyCode::Char('B') => self.cycle_download_bitrate(),
            KeyCode::Char(' ') => self.toggle_pause(),
            KeyCode::Char('n') => self.play_next().await?,
            KeyCode::Char('p') => self.play_previous().await?,
            KeyCode::Char('d') => self.download_selected_track(false).await?,
            KeyCode::Char('D') => self.download_selected_track(true).await?,
            KeyCode::Char('L') => self.start_logout(),
            KeyCode::Char('+') | KeyCode::Char('=') => self.adjust_volume(0.05),
            KeyCode::Char('-') => self.adjust_volume(-0.05),
            _ => {}
        }
        Ok(())
    }

    fn start_authorization(&mut self, client_id: String) {
        self.phase = Phase::RequestingDeviceCode;
        self.status_line = "Requesting device code...".to_owned();
        let tx = self.tx.clone();
        let http = self.http.clone();
        tokio::spawn(async move {
            let result = oauth::request_device_code(&http, &client_id, SCOPES).await;
            let _ = tx.send(BackendEvent::DeviceCode(result));
        });
    }

    fn start_token_poll(&mut self, client_id: String, device_code: DeviceCode) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let client_secret = self.config.client_secret.clone();
        tokio::spawn(async move {
            let result = oauth::poll_for_token(
                &http,
                &client_id,
                client_secret.as_deref(),
                &device_code.device_code,
                device_code.interval.unwrap_or(5),
                device_code.expires_in,
            )
            .await;
            let _ = tx.send(BackendEvent::Token(result));
        });
    }

    fn start_session_load(&mut self, client_id: String, token: TokenSet) {
        self.phase = Phase::LoadingLibrary;
        self.status_line = "Loading iBroadcast library...".to_owned();
        let tx = self.tx.clone();
        let config = self.config.clone();
        tokio::spawn(async move {
            let mut api = ApiClient::new(client_id, token, &config);
            let result = async {
                let status = api.status().await?;
                let user_id = status
                    .user_id
                    .ok_or_else(|| AppError::Api("status response omitted user id".to_owned()))?;
                let preferred_bitrate = api.get_bitrate_pref().await.ok().flatten();
                let mut settings = status.settings;
                let library_response = api.sync_library().await?;
                settings.merge_from(library_response.settings);
                Ok(LoadedSession {
                    api,
                    library: library_response.library,
                    user_id,
                    settings,
                    preferred_bitrate,
                })
            }
            .await;
            let _ = tx.send(BackendEvent::Session(Box::new(result)));
        });
    }

    fn start_logout(&mut self) {
        let Some(client_id) = self.config.client_id.clone() else {
            self.phase = Phase::NeedClientId;
            return;
        };

        let refresh_token = self
            .api
            .as_ref()
            .map(|api| api.token().refresh_token.clone());
        self.token_store.delete(&client_id);
        if let Some(player) = self.playback.as_mut() {
            player.stop();
        }
        self.abort_playback_download();
        self.api = None;
        self.library = None;
        self.filtered_track_ids.clear();
        self.user_id = None;
        self.queue = PlaybackQueue::default();
        self.phase = Phase::LoggingOut;
        self.status_line = "Logged out locally; revoking token...".to_owned();

        let tx = self.tx.clone();
        let http = self.http.clone();
        let client_secret = self.config.client_secret.clone();
        tokio::spawn(async move {
            let result = if let Some(refresh_token) = refresh_token {
                oauth::revoke_token(&http, &client_id, client_secret.as_deref(), &refresh_token)
                    .await
            } else {
                Ok(())
            };
            let _ = tx.send(BackendEvent::Logout { client_id, result });
        });
    }

    fn handle_backend_event(&mut self, event: BackendEvent) {
        match event {
            BackendEvent::DeviceCode(Ok(device_code)) => {
                if let Some(client_id) = self.config.client_id.clone() {
                    self.start_token_poll(client_id, device_code.clone());
                    self.phase = Phase::Authorizing(device_code);
                    self.status_line = "Waiting for browser authorization...".to_owned();
                } else {
                    self.phase = Phase::NeedClientId;
                }
            }
            BackendEvent::DeviceCode(Err(err)) => self.phase = Phase::Error(err.to_string()),
            BackendEvent::Token(Ok(token)) => {
                if let Some(client_id) = self.config.client_id.clone() {
                    match self.token_store.save(&client_id, &token) {
                        Ok(TokenPersistence::Keyring) => {
                            self.status_line = "Token saved to system keyring".to_owned();
                        }
                        Ok(TokenPersistence::KeyringWithPlainBackup(path)) => {
                            self.status_line = format!(
                                "Token saved to keyring; fallback file at {}",
                                path.display()
                            );
                        }
                        Ok(TokenPersistence::PlainFile(path)) => {
                            self.status_line =
                                format!("Keyring unavailable; token saved to {}", path.display());
                        }
                        Err(err) => {
                            self.phase = Phase::Error(err.to_string());
                            return;
                        }
                    }
                    self.start_session_load(client_id, token);
                } else {
                    self.phase = Phase::NeedClientId;
                }
            }
            BackendEvent::Token(Err(err)) => self.phase = Phase::Error(err.to_string()),
            BackendEvent::Session(result) => match *result {
                Ok(mut session) => {
                    if session.api.take_refreshed()
                        && let Some(client_id) = self.config.client_id.as_deref()
                    {
                        let _ = self.token_store.save(client_id, session.api.token());
                    }
                    if !self.config.playback_bitrate_explicit {
                        self.playback_bitrate =
                            session.preferred_bitrate.unwrap_or(Bitrate::Kbps128);
                    }
                    self.filtered_track_ids = session.library.sorted_track_ids();
                    self.selected = 0;
                    self.user_id = Some(session.user_id);
                    self.settings = session.settings;
                    self.api = Some(session.api);
                    let count = session.library.tracks.len();
                    self.library = Some(session.library);
                    self.refresh_downloaded_tracks();
                    self.phase = Phase::Ready;
                    self.status_line = format!("Loaded {count} tracks");
                    if let Some(warning) = &self.playback_warning {
                        self.status_line = format!("{}; {warning}", self.status_line);
                    }
                }
                Err(err) => self.phase = Phase::Error(err.to_string()),
            },
            BackendEvent::DownloadFinished { task_id, result } => {
                let status = match result {
                    Ok(path) => {
                        self.status_line = format!("Downloaded {}", path.display());
                        DownloadStatus::Finished(path)
                    }
                    Err(err) => {
                        self.status_line = err.to_string();
                        DownloadStatus::Failed(err.to_string())
                    }
                };
                if let Some(task) = self.downloads.iter_mut().find(|task| task.id == task_id) {
                    if let DownloadStatus::Finished(_) = &status {
                        self.downloaded_track_ids.insert(task.track_id);
                    }
                    task.status = status;
                }
            }
            BackendEvent::Logout { client_id, result } => {
                self.client_id_input = client_id.clone();
                self.config.client_id = Some(client_id);
                self.phase = Phase::NeedClientId;
                self.status_line = match result {
                    Ok(()) => "Logged out. Press Enter to authorize again.".to_owned(),
                    Err(err) => format!(
                        "Logged out locally. Token revoke failed: {err}. Press Enter to authorize again."
                    ),
                };
            }
        }
    }

    fn apply_search(&mut self) {
        if let Some(library) = &self.library {
            self.filtered_track_ids = library.search_track_ids(&self.search_input);
            self.selected = 0;
            self.active_view = View::Library;
            self.status_line = format!(
                "{} matches for '{}'",
                self.filtered_track_ids.len(),
                self.search_input
            );
        }
    }

    fn next_view(&mut self) {
        self.active_view = match self.active_view {
            View::Library => View::Queue,
            View::Queue => View::Library,
        };
    }

    fn move_selection(&mut self, delta: isize) {
        let len = match self.active_view {
            View::Library => self.filtered_track_ids.len(),
            View::Queue => self.queue.tracks().len(),
        };
        if len == 0 {
            return;
        }
        let selected = match self.active_view {
            View::Library => &mut self.selected,
            View::Queue => &mut self.queue_selected,
        };
        let next = (*selected as isize + delta).clamp(0, len.saturating_sub(1) as isize);
        *selected = next as usize;
    }

    async fn activate_selected(&mut self) -> Result<()> {
        match self.active_view {
            View::Library => {
                if let Some(track_id) = self.selected_library_track_id() {
                    let queue_index = self.queue.enqueue(track_id);
                    self.queue_selected = queue_index;
                    self.queue.play_index(queue_index);
                    self.start_playback_current().await?;
                }
            }
            View::Queue => {
                if self.queue.play_index(self.queue_selected).is_some() {
                    self.start_playback_current().await?;
                }
            }
        }
        Ok(())
    }

    fn add_selected_to_queue(&mut self, all_visible: bool) {
        if self.active_view != View::Library {
            return;
        }

        let added = if all_visible {
            self.queue
                .enqueue_many(self.filtered_track_ids.iter().copied())
        } else if let Some(track_id) = self.selected_library_track_id() {
            self.queue.enqueue(track_id);
            1
        } else {
            0
        };

        self.status_line = if added == 0 {
            "No tracks to add".to_owned()
        } else {
            format!("Added {added} track(s) to queue")
        };
    }

    async fn delete_or_remove_selected(&mut self) -> Result<()> {
        match self.active_view {
            View::Library => self.delete_selected_local_file(),
            View::Queue => self.remove_queue_selected().await,
        }
    }

    fn delete_selected_local_file(&mut self) -> Result<()> {
        let Some(track_id) = self.selected_library_track_id() else {
            self.status_line = "No track selected".to_owned();
            return Ok(());
        };

        if self.track_is_downloading(track_id) {
            self.status_line = "Download is still running; wait before deleting".to_owned();
            return Ok(());
        }

        let Some(path) = self.local_download_path(track_id) else {
            self.status_line = "Track is not available locally".to_owned();
            return Ok(());
        };

        if !path.exists() {
            self.downloaded_track_ids.remove(&track_id);
            self.status_line = "Track is not available locally".to_owned();
            return Ok(());
        }

        if self.queue.current_track() == Some(track_id) {
            if let Some(player) = self.playback.as_mut() {
                player.stop();
            }
            self.abort_playback_download();
        }

        fs::remove_file(&path)?;
        self.downloaded_track_ids.remove(&track_id);
        remove_empty_download_dirs(&path, &self.config.download_dir);
        self.status_line = format!("Deleted local file: {}", path.display());
        Ok(())
    }

    async fn remove_queue_selected(&mut self) -> Result<()> {
        if self.active_view != View::Queue || self.queue.is_empty() {
            return Ok(());
        }

        let selected = self.queue_selected.min(self.queue.len() - 1);
        let removed_current = self.queue.current_index() == Some(selected);
        let removed = self.queue.remove(selected);
        if self.queue_selected >= self.queue.len() {
            self.queue_selected = self.queue.len().saturating_sub(1);
        }

        if removed_current {
            if self.queue.current_track().is_some() {
                self.start_playback_current().await?;
            } else {
                if let Some(player) = self.playback.as_mut() {
                    player.stop();
                }
                self.abort_playback_download();
                self.status_line = "Queue is empty".to_owned();
            }
        } else if removed.is_some() {
            self.status_line = "Removed track from queue".to_owned();
        }
        Ok(())
    }

    fn move_queue_item(&mut self, delta: isize) {
        if self.active_view != View::Queue || self.queue.is_empty() {
            return;
        }

        let selected = self.queue_selected.min(self.queue.len() - 1);
        let moved = if delta < 0 {
            self.queue.move_up(selected)
        } else {
            self.queue.move_down(selected)
        };
        if let Some(index) = moved {
            self.queue_selected = index;
            self.status_line = "Moved queue item".to_owned();
        }
    }

    fn clear_queue(&mut self) {
        if self.active_view != View::Queue {
            return;
        }
        self.queue.clear();
        self.queue_selected = 0;
        if let Some(player) = self.playback.as_mut() {
            player.stop();
        }
        self.abort_playback_download();
        self.status_line = "Queue cleared".to_owned();
    }

    fn cycle_playback_bitrate(&mut self) {
        self.playback_bitrate = self.playback_bitrate.next();
        self.config.playback_bitrate = self.playback_bitrate;
        self.config.playback_bitrate_explicit = true;
        self.status_line = match self.config.save(&self.paths) {
            Ok(()) => format!(
                "Playback bitrate set to {} for future streams",
                self.playback_bitrate
            ),
            Err(err) => format!(
                "Playback bitrate set to {}, but config save failed: {err}",
                self.playback_bitrate
            ),
        };
    }

    fn cycle_download_bitrate(&mut self) {
        self.config.download_bitrate = self.config.download_bitrate.next();
        self.status_line = match self.config.save(&self.paths) {
            Ok(()) => format!("Download bitrate set to {}", self.config.download_bitrate),
            Err(err) => format!(
                "Download bitrate set to {}, but config save failed: {err}",
                self.config.download_bitrate
            ),
        };
    }

    async fn play_next(&mut self) -> Result<()> {
        if self.queue.next().is_some() {
            self.start_playback_current().await?;
        }
        Ok(())
    }

    async fn play_previous(&mut self) -> Result<()> {
        if self.queue.previous().is_some() {
            self.start_playback_current().await?;
        }
        Ok(())
    }

    fn toggle_pause(&mut self) {
        let Some(player) = self.playback.as_mut() else {
            self.status_line = "No audio output device available".to_owned();
            return;
        };
        let paused = player.toggle_pause();
        self.queue.set_paused(paused);
        self.status_line = if paused {
            "Paused".to_owned()
        } else {
            "Playing".to_owned()
        };
    }

    fn adjust_volume(&mut self, delta: f32) {
        let Some(player) = self.playback.as_mut() else {
            self.status_line = "No audio output device available".to_owned();
            return;
        };
        let volume = (player.volume() + delta).clamp(0.0, 1.0);
        player.set_volume(volume);
        self.status_line = format!("Volume {:.0}%", volume * 100.0);
    }

    async fn start_playback_current(&mut self) -> Result<()> {
        let Some(track_id) = self.queue.current_track() else {
            return Ok(());
        };
        if let Some(path) = self.local_download_path(track_id) {
            if path.exists() {
                self.downloaded_track_ids.insert(track_id);
                return self.play_local_track(track_id, path);
            }
            self.downloaded_track_ids.remove(&track_id);
        }

        let (track, url) = self
            .prepare_track_stream(track_id, self.playback_bitrate)
            .await?;

        self.abort_playback_download();
        let label = self
            .library
            .as_ref()
            .map(|library| library.track_label(track_id))
            .unwrap_or_else(|| format!("Track {track_id}"));
        self.status_line = format!("Streaming {label}");

        let buffer = ProgressiveBuffer::new(None);
        let reader = buffer.reader();
        let http = self.http.clone();
        let download_buffer = buffer.clone();
        let handle = tokio::spawn(async move {
            if let Err(err) = stream_to_buffer(&http, &url, download_buffer.clone()).await {
                download_buffer.fail(err.to_string());
            }
        });
        self.playback_download = Some(handle);

        let Some(player) = self.playback.as_mut() else {
            self.status_line = "No audio output device available".to_owned();
            return Ok(());
        };
        match player.play_stream(
            reader,
            None,
            &track.mime_type,
            extension_from_mime(&track.mime_type),
        ) {
            Ok(()) => {
                self.status_line = format!("Playing {label}");
            }
            Err(err) => self.status_line = err.to_string(),
        }
        Ok(())
    }

    fn play_local_track(&mut self, track_id: u64, path: PathBuf) -> Result<()> {
        self.abort_playback_download();
        let label = self
            .library
            .as_ref()
            .map(|library| library.track_label(track_id))
            .unwrap_or_else(|| format!("Track {track_id}"));

        let Some(player) = self.playback.as_mut() else {
            self.status_line = "No audio output device available".to_owned();
            return Ok(());
        };

        let mime_type = self
            .library
            .as_ref()
            .and_then(|library| library.tracks.get(&track_id))
            .map(|track| track.mime_type.as_str())
            .filter(|mime_type| !mime_type.trim().is_empty())
            .unwrap_or("application/octet-stream")
            .to_owned();
        let extension_hint = path
            .extension()
            .and_then(|extension| extension.to_str())
            .filter(|extension| !extension.trim().is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| extension_from_mime(&mime_type).to_owned());

        match player.play_file(&path, &mime_type, &extension_hint) {
            Ok(()) => {
                self.queue.set_paused(false);
                self.status_line = format!("Playing local file: {label}");
            }
            Err(err) => self.status_line = err.to_string(),
        }
        Ok(())
    }

    async fn download_selected_track(&mut self, all_visible: bool) -> Result<()> {
        let track_ids: Vec<u64> = match (self.active_view, all_visible) {
            (View::Library, true) => self.filtered_track_ids.clone(),
            (View::Library, false) => self
                .filtered_track_ids
                .get(self.selected)
                .copied()
                .into_iter()
                .collect(),
            (View::Queue, true) => self.queue.tracks().to_vec(),
            (View::Queue, false) => self
                .queue
                .tracks()
                .get(self.queue_selected)
                .copied()
                .into_iter()
                .collect(),
        };

        let mut queued = 0usize;
        let mut skipped = 0usize;
        for track_id in track_ids {
            if self.track_is_downloading(track_id) || self.track_is_local(track_id) {
                skipped += 1;
                continue;
            }
            let (track, url, path) = self
                .prepare_track_transfer(track_id, self.config.download_bitrate)
                .await?;
            let task_id = self.next_download_id;
            self.next_download_id += 1;
            self.downloads.push(DownloadTask {
                id: task_id,
                track_id,
                title: track.title.clone(),
                status: DownloadStatus::Running,
            });
            let tx = self.tx.clone();
            let http = self.http.clone();
            tokio::spawn(async move {
                let result = download_to_file(&http, &url, &path)
                    .await
                    .map(|()| path.clone())
                    .map_err(|err| AppError::download_path(path.clone(), err));
                let _ = tx.send(BackendEvent::DownloadFinished { task_id, result });
            });
            queued += 1;
        }
        self.status_line = match (queued, skipped) {
            (0, 0) => "No tracks selected for download".to_owned(),
            (0, _) => format!("No new downloads queued; skipped {skipped} local/running track(s)"),
            (_, 0) => format!("Queued {queued} download(s)"),
            _ => format!("Queued {queued} download(s), skipped {skipped} local/running track(s)"),
        };
        Ok(())
    }

    async fn prepare_track_transfer(
        &mut self,
        track_id: u64,
        bitrate: Bitrate,
    ) -> Result<(Track, String, PathBuf)> {
        let track = self
            .library
            .as_ref()
            .and_then(|library| library.tracks.get(&track_id))
            .cloned()
            .ok_or_else(|| AppError::Library(format!("unknown track id {track_id}")))?;
        let user_id = self
            .user_id
            .ok_or_else(|| AppError::Api("missing user id".to_owned()))?;
        let api = self.api.as_mut().ok_or(AppError::MissingToken)?;
        api.ensure_token().await?;
        if api.take_refreshed()
            && let Some(client_id) = self.config.client_id.as_deref()
        {
            let _ = self.token_store.save(client_id, api.token());
        }
        let context = api.stream_context(&self.settings, user_id);
        let url = context.build_stream_url(&track, bitrate)?;
        let library = self
            .library
            .as_ref()
            .ok_or_else(|| AppError::Library("library is not loaded".to_owned()))?;
        let path = build_download_path(&self.config.download_dir, library, &track);
        Ok((track, url, path))
    }

    async fn prepare_track_stream(
        &mut self,
        track_id: u64,
        bitrate: Bitrate,
    ) -> Result<(Track, String)> {
        let track = self
            .library
            .as_ref()
            .and_then(|library| library.tracks.get(&track_id))
            .cloned()
            .ok_or_else(|| AppError::Library(format!("unknown track id {track_id}")))?;
        let user_id = self
            .user_id
            .ok_or_else(|| AppError::Api("missing user id".to_owned()))?;
        let api = self.api.as_mut().ok_or(AppError::MissingToken)?;
        api.ensure_token().await?;
        if api.take_refreshed()
            && let Some(client_id) = self.config.client_id.as_deref()
        {
            let _ = self.token_store.save(client_id, api.token());
        }
        let context = api.stream_context(&self.settings, user_id);
        let url = context.build_stream_url(&track, bitrate)?;
        Ok((track, url))
    }

    fn selected_library_track_id(&self) -> Option<u64> {
        self.filtered_track_ids
            .get(
                self.selected
                    .min(self.filtered_track_ids.len().saturating_sub(1)),
            )
            .copied()
    }

    fn refresh_downloaded_tracks(&mut self) {
        self.downloaded_track_ids.clear();
        let Some(library) = &self.library else {
            return;
        };
        self.downloaded_track_ids = library
            .tracks
            .iter()
            .filter_map(|(track_id, track)| {
                let path = build_download_path(&self.config.download_dir, library, track);
                path.exists().then_some(*track_id)
            })
            .collect();
    }

    fn local_download_path(&self, track_id: u64) -> Option<PathBuf> {
        let library = self.library.as_ref()?;
        let track = library.tracks.get(&track_id)?;
        Some(build_download_path(
            &self.config.download_dir,
            library,
            track,
        ))
    }

    fn track_is_local(&self, track_id: u64) -> bool {
        self.downloaded_track_ids.contains(&track_id)
            && self
                .local_download_path(track_id)
                .is_some_and(|path| path.exists())
    }

    fn track_is_downloading(&self, track_id: u64) -> bool {
        self.downloads
            .iter()
            .any(|task| task.track_id == track_id && matches!(task.status, DownloadStatus::Running))
    }

    fn abort_playback_download(&mut self) {
        if let Some(handle) = self.playback_download.take() {
            handle.abort();
        }
    }

    fn draw(&mut self, frame: &mut Frame<'_>) {
        match &self.phase {
            Phase::NeedClientId => self.draw_client_id(frame),
            Phase::RequestingDeviceCode => self.draw_message(
                frame,
                "Requesting device code",
                "Contacting oauth.ibroadcast.com...",
            ),
            Phase::Authorizing(device_code) => self.draw_authorizing(frame, device_code),
            Phase::LoadingLibrary => self.draw_message(
                frame,
                "Loading library",
                "Synchronizing your iBroadcast library...",
            ),
            Phase::LoggingOut => self.draw_message(
                frame,
                "Logging out",
                "Revoking token and clearing session...",
            ),
            Phase::Ready => self.draw_ready(frame),
            Phase::Error(message) => self.draw_error(frame, message),
        }
    }

    fn draw_client_id(&self, frame: &mut Frame<'_>) {
        let text = vec![
            Line::from("Enter your iBroadcast OAuth client_id"),
            Line::from("Create one in media.ibroadcast.com: side menu -> Apps -> developer"),
            Line::from("No code is required; copy the generated client_id here."),
            Line::from(""),
            Line::from(self.client_id_input.as_str()),
            Line::from(""),
            Line::from(self.status_line.as_str()),
            Line::from(""),
            Line::from("Enter to continue, q to quit"),
        ];
        frame.render_widget(
            centered_paragraph("Login", text),
            centered_rect(78, 13, frame.area()),
        );
    }

    fn draw_authorizing(&self, frame: &mut Frame<'_>, device_code: &DeviceCode) {
        let mut lines = vec![
            Line::from("Open this URL and approve the app:"),
            Line::from(device_code.verification_uri.as_str()),
            Line::from(""),
            Line::from(vec![
                Span::raw("Code: "),
                Span::styled(
                    &device_code.user_code,
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
            ]),
        ];
        if let Some(complete) = &device_code.verification_uri_complete {
            lines.push(Line::from(""));
            lines.push(Line::from("Direct URL:"));
            lines.push(Line::from(complete.as_str()));
        }
        lines.push(Line::from(""));
        lines.push(Line::from("Waiting for authorization... q quits"));
        frame.render_widget(
            centered_paragraph("Authorize iBroadcast", lines),
            centered_rect(80, 13, frame.area()),
        );
    }

    fn draw_message(&self, frame: &mut Frame<'_>, title: &'static str, message: &'static str) {
        frame.render_widget(
            centered_paragraph(title, vec![Line::from(message)]),
            centered_rect(70, 7, frame.area()),
        );
    }

    fn draw_error(&self, frame: &mut Frame<'_>, message: &str) {
        let lines = vec![
            Line::from("Something failed:"),
            Line::from(""),
            Line::from(message),
            Line::from(""),
            Line::from("Press l to log in again, q to quit"),
        ];
        frame.render_widget(
            centered_paragraph("Error", lines),
            centered_rect(80, 11, frame.area()),
        );
    }

    fn draw_ready(&mut self, frame: &mut Frame<'_>) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(5),
                Constraint::Length(5),
                Constraint::Length(1),
            ])
            .split(frame.area());

        let selected_tab = match self.active_view {
            View::Library => 0,
            View::Queue => 1,
        };
        let tabs = Tabs::new(vec!["Library", "Queue"])
            .select(selected_tab)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("iBroadcast TUI"),
            )
            .highlight_style(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            );
        frame.render_widget(tabs, chunks[0]);

        match self.active_view {
            View::Library => self.draw_library(frame, chunks[1]),
            View::Queue => self.draw_queue(frame, chunks[1]),
        }

        let now_playing = self
            .queue
            .current_track()
            .and_then(|id| self.library.as_ref().map(|library| library.track_label(id)))
            .unwrap_or_else(|| "Nothing playing".to_owned());
        let status = Paragraph::new(vec![
            Line::from(self.status_line.as_str()),
            Line::from(format!("Now: {now_playing}")),
            Line::from(format!(
                "Playback bitrate: {} | Download bitrate: {}",
                self.playback_bitrate, self.config.download_bitrate
            )),
            Line::from(format!(
                "Local files: {} | Downloads running: {}",
                self.downloaded_track_ids.len(),
                self.downloads
                    .iter()
                    .filter(|task| matches!(task.status, DownloadStatus::Running))
                    .count()
            )),
        ])
        .block(Block::default().borders(Borders::ALL).title("Status"));
        frame.render_widget(status, chunks[2]);

        let help = if self.search_mode {
            format!("Search: {}", self.search_input)
        } else {
            "/ search | Enter play | a/A add | d/D download | x delete local/queue | [/ ] move | b/B bitrate | L logout | q quit".to_owned()
        };
        frame.render_widget(Paragraph::new(help), chunks[3]);

        if self.search_mode {
            self.draw_search_popup(frame);
        }
    }

    fn draw_library(&mut self, frame: &mut Frame<'_>, area: Rect) {
        let Some(library) = &self.library else {
            return;
        };
        let downloaded_track_ids = &self.downloaded_track_ids;
        let downloads = &self.downloads;
        let download_dir = &self.config.download_dir;
        let rows = self.filtered_track_ids.iter().filter_map(|track_id| {
            let track = library.tracks.get(track_id)?;
            let state = track_storage_label(
                *track_id,
                track,
                library,
                download_dir,
                downloaded_track_ids,
                downloads,
            );
            Some(Row::new(vec![
                state.to_owned(),
                track.track.to_string(),
                track.title.clone(),
                library.artist_name(track.artist_id).to_owned(),
                library.album_name(track.album_id).to_owned(),
                track.duration_label(),
            ]))
        });
        let mut state = TableState::default();
        if !self.filtered_track_ids.is_empty() {
            state.select(Some(self.selected.min(self.filtered_track_ids.len() - 1)));
        }
        let table = Table::new(
            rows,
            [
                Constraint::Length(11),
                Constraint::Length(5),
                Constraint::Percentage(30),
                Constraint::Percentage(25),
                Constraint::Percentage(25),
                Constraint::Length(7),
            ],
        )
        .header(
            Row::new(vec!["State", "#", "Title", "Artist", "Album", "Time"])
                .style(Style::default().fg(Color::Yellow)),
        )
        .block(Block::default().borders(Borders::ALL).title(format!(
            "Library ({}/{})",
            self.filtered_track_ids.len(),
            library.tracks.len()
        )))
        .row_highlight_style(Style::default().bg(Color::DarkGray));
        frame.render_stateful_widget(table, area, &mut state);
    }

    fn draw_queue(&mut self, frame: &mut Frame<'_>, area: Rect) {
        let Some(library) = &self.library else {
            return;
        };
        let items = self
            .queue
            .tracks()
            .iter()
            .enumerate()
            .map(|(index, track_id)| {
                let marker = if self.queue.current_index() == Some(index) {
                    ">"
                } else {
                    " "
                };
                ListItem::new(format!("{marker} {}", library.track_label(*track_id)))
            });
        let mut state = ListState::default();
        if !self.queue.tracks().is_empty() {
            state.select(Some(self.queue_selected.min(self.queue.tracks().len() - 1)));
        }
        let list = List::new(items)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Playback Queue"),
            )
            .highlight_style(Style::default().bg(Color::DarkGray));
        frame.render_stateful_widget(list, area, &mut state);
    }

    fn draw_search_popup(&self, frame: &mut Frame<'_>) {
        let area = centered_rect(60, 5, frame.area());
        frame.render_widget(Clear, area);
        let paragraph = Paragraph::new(self.search_input.as_str())
            .block(Block::default().borders(Borders::ALL).title("Search"))
            .wrap(Wrap { trim: true });
        frame.render_widget(paragraph, area);
    }
}

fn centered_paragraph<'a>(title: &'static str, lines: Vec<Line<'a>>) -> Paragraph<'a> {
    Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title(title))
        .alignment(Alignment::Center)
        .wrap(Wrap { trim: true })
}

fn centered_rect(width_percent: u16, height: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(0),
            Constraint::Length(height),
            Constraint::Min(0),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - width_percent) / 2),
            Constraint::Percentage(width_percent),
            Constraint::Percentage((100 - width_percent) / 2),
        ])
        .split(vertical[1])[1]
}

fn track_storage_label(
    track_id: u64,
    track: &Track,
    library: &Library,
    download_dir: &Path,
    downloaded_track_ids: &HashSet<u64>,
    downloads: &[DownloadTask],
) -> &'static str {
    if downloads
        .iter()
        .any(|task| task.track_id == track_id && matches!(task.status, DownloadStatus::Running))
    {
        return "downloading";
    }

    if downloaded_track_ids.contains(&track_id)
        && build_download_path(download_dir, library, track).exists()
    {
        return "local";
    }

    if downloads
        .iter()
        .rev()
        .any(|task| task.track_id == track_id && matches!(task.status, DownloadStatus::Failed(_)))
    {
        return "failed";
    }

    ""
}

fn remove_empty_download_dirs(file_path: &Path, root: &Path) {
    let Ok(root) = root.canonicalize() else {
        return;
    };
    let mut current = file_path.parent().and_then(|path| path.canonicalize().ok());

    while let Some(dir) = current {
        if dir == root || !dir.starts_with(&root) {
            break;
        }
        if fs::remove_dir(&dir).is_err() {
            break;
        }
        current = dir.parent().map(Path::to_path_buf);
    }
}
