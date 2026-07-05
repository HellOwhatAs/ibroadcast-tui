use std::{fs, io, path::PathBuf, sync::Arc, time::Duration};

use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Frame, Terminal, backend::CrosstermBackend};
use reqwest::Client;
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinHandle,
};

use crate::{
    config::{Bitrate, Config},
    downloads::{
        DownloadManager, build_download_path, download_to_file, extension_from_mime,
        remove_empty_download_dirs, stream_to_buffer,
    },
    error::{AppError, Result},
    hls::stream_hls_to_buffer,
    library::{Library, Track},
    oauth::{self, DeviceCode, TokenSet},
    player::{AudioOutput, StreamSource, decode_progressive_stream},
    progressive::ProgressiveBuffer,
    queue::PlaybackQueue,
    session::{EstablishedSession, Session},
    storage::{TokenPersistence, TokenStore},
    ui::{self, View},
};

const SCOPES: &[&str] = &["user.library:read", "user.account:read"];
const NO_AUDIO_DEVICE: &str = "No audio output device available";

pub struct App {
    config: Config,
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
    session: Option<SessionCtx>,
    audio: Option<AudioOutput>,
    audio_warning: Option<String>,
    playback: PlaybackPhase,
    /// Incremented whenever playback intent changes; stale stream events are
    /// discarded by comparing it.
    playback_generation: u64,
    stream_task: Option<StreamTask>,
    queue: PlaybackQueue,
    downloads: DownloadManager,
    status_line: String,
    should_quit: bool,
}

/// The background task feeding the progressive buffer for the current stream.
///
/// The buffer handle is kept so cancellation can wake any reader blocked on
/// it: aborting the task alone would leave the audio thread waiting on the
/// buffer's condvar forever.
struct StreamTask {
    handle: JoinHandle<()>,
    buffer: ProgressiveBuffer,
}

/// State that exists only while logged in with a synced library.
struct SessionCtx {
    session: Arc<Mutex<Session>>,
    library: Library,
    server_bitrate: Option<Bitrate>,
    filtered_track_ids: Vec<u64>,
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
enum PlaybackPhase {
    /// Nothing playing and nothing requested.
    Idle,
    /// A stream URL is being resolved in the background.
    Loading,
    /// A track is loaded into the audio output (playing or paused).
    Active,
}

/// User intents in the ready screen, decoupled from the physical key map.
#[derive(Clone, Copy, Debug)]
enum Action {
    Quit,
    NextView,
    OpenSearch,
    MoveSelection(isize),
    Activate,
    AddToQueue { all_visible: bool },
    DeleteOrRemove,
    MoveQueueItem(isize),
    ClearQueue,
    CyclePlaybackMode,
    CyclePlaybackBitrate,
    CycleDownloadBitrate,
    TogglePause,
    NextTrack,
    PreviousTrack,
    Download { all_visible: bool },
    Logout,
    AdjustVolume(f32),
}

enum BackendEvent {
    DeviceCode(Result<DeviceCode>),
    Token(Result<TokenSet>),
    Session(Box<Result<EstablishedSession>>),
    /// A signed stream URL was resolved for the current playback intent.
    StreamResolved {
        generation: u64,
        track: Box<Track>,
        bitrate: Bitrate,
        result: Result<String>,
    },
    /// The stream's container format was probed and a decoder built.
    StreamDecoded {
        generation: u64,
        label: String,
        result: Result<StreamSource>,
    },
    /// The network transfer feeding the current stream failed mid-track.
    StreamInterrupted {
        generation: u64,
        error: String,
    },
    DownloadFinished {
        task_id: u64,
        result: Result<PathBuf>,
    },
    LoggedOut {
        client_id: String,
        result: Result<()>,
    },
}

impl App {
    pub fn new(config: Config, token_store: TokenStore) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let audio = AudioOutput::new().ok();
        let audio_warning = audio.is_none().then(|| {
            "No audio output device available; browsing and downloads still work".to_owned()
        });

        Self {
            config,
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
            session: None,
            audio,
            audio_warning,
            playback: PlaybackPhase::Idle,
            playback_generation: 0,
            stream_task: None,
            queue: PlaybackQueue::default(),
            downloads: DownloadManager::default(),
            status_line: String::new(),
            should_quit: false,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        self.bootstrap();
        let mut terminal = TerminalGuard::new()?;
        self.run_loop(&mut terminal.terminal).await
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
        while !self.should_quit {
            terminal.draw(|frame| self.render(frame))?;

            while let Ok(event) = self.rx.try_recv() {
                self.handle_backend_event(event);
            }

            if event::poll(Duration::from_millis(50))?
                && let Event::Key(key) = event::read()?
                && key.kind == KeyEventKind::Press
            {
                self.handle_key(key);
            }

            self.tick();
        }
        Ok(())
    }

    /// Advances playback when the current track has finished.
    fn tick(&mut self) {
        if self.playback != PlaybackPhase::Active {
            return;
        }
        if !self.audio.as_ref().is_some_and(AudioOutput::is_finished) {
            return;
        }
        if self.queue.next().is_some() {
            self.start_playback_current();
        } else {
            self.stop_playback();
            self.status_line = "Queue finished".to_owned();
        }
    }

    // ---- input --------------------------------------------------------

    fn handle_key(&mut self, key: KeyEvent) {
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.should_quit = true;
            return;
        }

        if self.search_mode {
            self.handle_search_key(key);
            return;
        }

        match &self.phase {
            Phase::NeedClientId => self.handle_client_id_key(key),
            Phase::Error(_) => self.handle_error_key(key),
            Phase::Ready => {
                if let Some(action) = action_for_key(key) {
                    self.apply_action(action);
                }
            }
            Phase::RequestingDeviceCode
            | Phase::Authorizing(_)
            | Phase::LoadingLibrary
            | Phase::LoggingOut => {
                if key.code == KeyCode::Char('q') {
                    self.should_quit = true;
                }
            }
        }
    }

    fn handle_client_id_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Enter if !self.client_id_input.trim().is_empty() => {
                let client_id = self.client_id_input.trim().to_owned();
                self.config.client_id = Some(client_id.clone());
                if let Err(err) = self.config.save() {
                    self.status_line = format!("Warning: could not save config: {err}");
                }
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
    }

    fn handle_error_key(&mut self, key: KeyEvent) {
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

    fn apply_action(&mut self, action: Action) {
        match action {
            Action::Quit => self.should_quit = true,
            Action::NextView => {
                self.active_view = match self.active_view {
                    View::Library => View::Queue,
                    View::Queue => View::Library,
                };
            }
            Action::OpenSearch => {
                self.search_mode = true;
                self.search_input.clear();
            }
            Action::MoveSelection(delta) => self.move_selection(delta),
            Action::Activate => self.activate_selected(),
            Action::AddToQueue { all_visible } => self.add_to_queue(all_visible),
            Action::DeleteOrRemove => match self.active_view {
                View::Library => self.delete_selected_local_file(),
                View::Queue => self.remove_queue_selected(),
            },
            Action::MoveQueueItem(delta) => self.move_queue_item(delta),
            Action::ClearQueue => self.clear_queue(),
            Action::CyclePlaybackMode => self.cycle_playback_mode(),
            Action::CyclePlaybackBitrate => self.cycle_playback_bitrate(),
            Action::CycleDownloadBitrate => self.cycle_download_bitrate(),
            Action::TogglePause => self.toggle_pause(),
            Action::NextTrack => {
                if self.queue.next().is_some() {
                    self.start_playback_current();
                }
            }
            Action::PreviousTrack => {
                if self.queue.previous().is_some() {
                    self.start_playback_current();
                }
            }
            Action::Download { all_visible } => self.download_tracks(all_visible),
            Action::Logout => self.start_logout(),
            Action::AdjustVolume(delta) => self.adjust_volume(delta),
        }
    }

    // ---- browsing and queue -------------------------------------------

    fn move_selection(&mut self, delta: isize) {
        let len = match self.active_view {
            View::Library => self
                .session
                .as_ref()
                .map_or(0, |ctx| ctx.filtered_track_ids.len()),
            View::Queue => self.queue.len(),
        };
        if len == 0 {
            return;
        }
        let selected = match self.active_view {
            View::Library => &mut self.selected,
            View::Queue => &mut self.queue_selected,
        };
        *selected = (*selected as isize + delta).clamp(0, len as isize - 1) as usize;
    }

    fn selected_library_track_id(&self) -> Option<u64> {
        let ids = &self.session.as_ref()?.filtered_track_ids;
        ids.get(self.selected.min(ids.len().checked_sub(1)?))
            .copied()
    }

    fn activate_selected(&mut self) {
        match self.active_view {
            View::Library => {
                if let Some(track_id) = self.selected_library_track_id() {
                    // Tracks are queued at most once; activating an already
                    // queued track jumps to its existing entry.
                    let (index, _) = self.queue.enqueue(track_id);
                    self.queue_selected = index;
                    self.queue.play_index(index);
                    self.start_playback_current();
                }
            }
            View::Queue => {
                if self.queue.play_index(self.queue_selected).is_some() {
                    self.start_playback_current();
                }
            }
        }
    }

    fn add_to_queue(&mut self, all_visible: bool) {
        if self.active_view != View::Library {
            return;
        }

        let (candidates, added) = if all_visible {
            let Some(ctx) = &self.session else { return };
            (
                ctx.filtered_track_ids.len(),
                self.queue
                    .enqueue_many(ctx.filtered_track_ids.iter().copied()),
            )
        } else if let Some(track_id) = self.selected_library_track_id() {
            (1, usize::from(self.queue.enqueue(track_id).1))
        } else {
            (0, 0)
        };

        self.status_line = if candidates == 0 {
            "No tracks to add".to_owned()
        } else if added == 0 {
            "Already in queue".to_owned()
        } else if added < candidates {
            format!(
                "Added {added} track(s) to queue ({} already queued)",
                candidates - added
            )
        } else {
            format!("Added {added} track(s) to queue")
        };
    }

    fn delete_selected_local_file(&mut self) {
        let Some(track_id) = self.selected_library_track_id() else {
            self.status_line = "No track selected".to_owned();
            return;
        };
        if self.downloads.is_running(track_id) {
            self.status_line = "Download is still running; wait before deleting".to_owned();
            return;
        }
        if !self.downloads.is_local(track_id) {
            self.status_line = "Track is not available locally".to_owned();
            return;
        }
        let Some(ctx) = &self.session else { return };
        let Some(track) = ctx.library.tracks.get(&track_id) else {
            return;
        };
        let path = build_download_path(&self.config.download_dir, &ctx.library, track);

        if self.queue.current_track() == Some(track_id) {
            self.stop_playback();
        }

        match remove_file_with_retry(&path) {
            Ok(()) => {
                self.downloads.mark_not_local(track_id);
                remove_empty_download_dirs(&path, &self.config.download_dir);
                self.status_line = format!("Deleted local file: {}", path.display());
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                // Deleted externally; fix up the index.
                self.downloads.mark_not_local(track_id);
                self.status_line = "Track is not available locally".to_owned();
            }
            Err(err) => self.status_line = format!("Delete failed: {err}"),
        }
    }

    fn remove_queue_selected(&mut self) {
        if self.queue.is_empty() {
            return;
        }

        let selected = self.queue_selected.min(self.queue.len() - 1);
        let removed_current = self.queue.current_index() == Some(selected);
        let removed = self.queue.remove(selected);
        if self.queue_selected >= self.queue.len() {
            self.queue_selected = self.queue.len().saturating_sub(1);
        }

        if removed_current && self.playback != PlaybackPhase::Idle {
            if self.queue.current_track().is_some() {
                self.start_playback_current();
            } else {
                self.stop_playback();
                self.status_line = "Queue is empty".to_owned();
            }
        } else if removed.is_some() {
            self.status_line = "Removed track from queue".to_owned();
        }
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
        self.stop_playback();
        self.status_line = "Queue cleared".to_owned();
    }

    fn apply_search(&mut self) {
        let Some(ctx) = self.session.as_mut() else {
            return;
        };
        ctx.filtered_track_ids = ctx.library.search_track_ids(&self.search_input);
        self.selected = 0;
        self.active_view = View::Library;
        self.status_line = format!(
            "{} matches for '{}'",
            ctx.filtered_track_ids.len(),
            self.search_input
        );
    }

    // ---- settings ------------------------------------------------------

    /// The bitrate used for streaming: explicit user choice, else the account
    /// preference reported by the server, else the default.
    fn effective_playback_bitrate(&self) -> Bitrate {
        self.config
            .playback_bitrate
            .or_else(|| self.session.as_ref().and_then(|ctx| ctx.server_bitrate))
            .unwrap_or_default()
    }

    fn cycle_playback_bitrate(&mut self) {
        let next = self.effective_playback_bitrate().next();
        self.config.playback_bitrate = Some(next);
        self.status_line = match self.config.save() {
            Ok(()) => format!("Playback bitrate set to {next} for future streams"),
            Err(err) => format!("Playback bitrate set to {next}, but config save failed: {err}"),
        };
    }

    fn cycle_download_bitrate(&mut self) {
        // Downloads only cycle through the bitrates the server stores as
        // complete files; the others exist solely as HLS streams.
        self.config.download_bitrate = self.config.download_bitrate.next_download();
        self.status_line = match self.config.save() {
            Ok(()) => format!("Download bitrate set to {}", self.config.download_bitrate),
            Err(err) => format!(
                "Download bitrate set to {}, but config save failed: {err}",
                self.config.download_bitrate
            ),
        };
    }

    fn cycle_playback_mode(&mut self) {
        let mode = self.queue.cycle_playback_mode();
        self.status_line = format!("Playback mode set to {mode}");
    }

    // ---- playback -------------------------------------------------------

    /// Starts playing the queue's current track: from the local file when one
    /// exists, otherwise by resolving a stream URL in the background.
    fn start_playback_current(&mut self) {
        self.cancel_stream_task();
        let Some(track_id) = self.queue.current_track() else {
            self.stop_playback();
            return;
        };
        let Some(ctx) = &self.session else {
            self.status_line = "Not logged in".to_owned();
            return;
        };
        let Some(track) = ctx.library.tracks.get(&track_id).cloned() else {
            self.status_line = format!("Unknown track id {track_id}");
            return;
        };
        let label = ctx.library.track_label(track_id);
        let session = Arc::clone(&ctx.session);
        let local_path = self
            .downloads
            .is_local(track_id)
            .then(|| build_download_path(&self.config.download_dir, &ctx.library, &track));

        if let Some(path) = local_path {
            if path.exists() {
                self.play_local(&track, &label, path);
                return;
            }
            // Deleted externally; fall through to streaming.
            self.downloads.mark_not_local(track_id);
        }

        let Some(audio) = self.audio.as_mut() else {
            self.playback = PlaybackPhase::Idle;
            self.status_line = NO_AUDIO_DEVICE.to_owned();
            return;
        };
        // Silence the previous track while the new one is being resolved, so
        // a resolution failure cannot leave stale audio playing.
        audio.stop();

        self.playback_generation += 1;
        let generation = self.playback_generation;
        self.playback = PlaybackPhase::Loading;
        self.status_line = format!("Fetching stream for {label}");

        let bitrate = self.effective_playback_bitrate();
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let result = session.lock().await.stream_url(&track, bitrate).await;
            let _ = tx.send(BackendEvent::StreamResolved {
                generation,
                track: Box::new(track),
                bitrate,
                result,
            });
        });
    }

    fn play_local(&mut self, track: &Track, label: &str, path: PathBuf) {
        self.playback_generation += 1; // invalidate any pending stream resolution
        let Some(audio) = self.audio.as_mut() else {
            self.playback = PlaybackPhase::Idle;
            self.status_line = NO_AUDIO_DEVICE.to_owned();
            return;
        };

        let mime_type = if track.mime_type.trim().is_empty() {
            "application/octet-stream"
        } else {
            &track.mime_type
        };
        let extension_hint = path
            .extension()
            .and_then(|extension| extension.to_str())
            .filter(|extension| !extension.trim().is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| extension_from_mime(mime_type).to_owned());

        match audio.play_file(&path, mime_type, &extension_hint) {
            Ok(()) => {
                self.playback = PlaybackPhase::Active;
                self.status_line = format!("Playing local file: {label}");
            }
            Err(err) => {
                self.playback = PlaybackPhase::Idle;
                self.status_line = format!("Playback failed: {err}");
            }
        }
    }

    /// Hooks a resolved stream URL up to the audio output: spawns the network
    /// transfer into the progressive buffer, and probes/decodes the container
    /// on a blocking thread (probing blocks until enough bytes arrive, so it
    /// must stay off the UI event loop).
    ///
    /// Transcoded bitrates arrive as HLS playlists whose segments are demuxed
    /// into a raw ADTS AAC stream; 128 kbps and the original format are plain
    /// progressive files.
    fn begin_stream(&mut self, generation: u64, track: Track, bitrate: Bitrate, url: String) {
        let label = self
            .session
            .as_ref()
            .map(|ctx| ctx.library.track_label(track.id))
            .unwrap_or_else(|| format!("Track {}", track.id));

        let buffer = ProgressiveBuffer::new(None);
        let reader = buffer.reader();
        let http = self.http.clone();
        let feeder_buffer = buffer.clone();
        let feeder_tx = self.tx.clone();
        let handle = tokio::spawn(async move {
            let result = if bitrate.is_hls_stream() {
                stream_hls_to_buffer(
                    &http,
                    &url,
                    bitrate.target_bandwidth(),
                    feeder_buffer.clone(),
                )
                .await
            } else {
                stream_to_buffer(&http, &url, feeder_buffer.clone()).await
            };
            if let Err(err) = result {
                let error = err.to_string();
                feeder_buffer.fail(error.clone());
                let _ = feeder_tx.send(BackendEvent::StreamInterrupted { generation, error });
            }
        });
        self.stream_task = Some(StreamTask { handle, buffer });

        let (mime_type, extension_hint) = if bitrate.is_hls_stream() {
            // The HLS feeder emits a raw ADTS AAC stream, whatever the
            // track's own container is.
            ("audio/aac".to_owned(), "aac".to_owned())
        } else {
            (
                track.mime_type.clone(),
                extension_from_mime(&track.mime_type).to_owned(),
            )
        };
        let tx = self.tx.clone();
        tokio::task::spawn_blocking(move || {
            let result = decode_progressive_stream(reader, &mime_type, &extension_hint);
            let _ = tx.send(BackendEvent::StreamDecoded {
                generation,
                label,
                result,
            });
        });
    }

    fn toggle_pause(&mut self) {
        let Some(audio) = self.audio.as_mut() else {
            self.status_line = NO_AUDIO_DEVICE.to_owned();
            return;
        };
        self.status_line = match audio.toggle_pause() {
            Some(true) => "Paused".to_owned(),
            Some(false) => "Playing".to_owned(),
            None => "Nothing playing".to_owned(),
        };
    }

    fn adjust_volume(&mut self, delta: f32) {
        let Some(audio) = self.audio.as_mut() else {
            self.status_line = NO_AUDIO_DEVICE.to_owned();
            return;
        };
        let volume = (audio.volume() + delta).clamp(0.0, 1.0);
        audio.set_volume(volume);
        self.status_line = format!("Volume {:.0}%", volume * 100.0);
    }

    fn stop_playback(&mut self) {
        self.playback = PlaybackPhase::Idle;
        self.playback_generation += 1; // invalidate any pending stream events
        self.cancel_stream_task();
        if let Some(audio) = self.audio.as_mut() {
            audio.stop();
        }
    }

    fn cancel_stream_task(&mut self) {
        if let Some(task) = self.stream_task.take() {
            task.handle.abort();
            // Wake any reader blocked on the buffer; without this the audio
            // thread could wait on the buffer's condvar forever.
            task.buffer.fail("playback cancelled");
        }
    }

    // ---- downloads ------------------------------------------------------

    fn download_tracks(&mut self, all_visible: bool) {
        let track_ids: Vec<u64> = match (self.active_view, all_visible) {
            (View::Library, true) => self
                .session
                .as_ref()
                .map(|ctx| ctx.filtered_track_ids.clone())
                .unwrap_or_default(),
            (View::Library, false) => self.selected_library_track_id().into_iter().collect(),
            (View::Queue, true) => self.queue.tracks().to_vec(),
            (View::Queue, false) => self
                .queue
                .tracks()
                .get(self.queue_selected)
                .copied()
                .into_iter()
                .collect(),
        };
        let Some(ctx) = &self.session else {
            return;
        };

        let bitrate = self.config.download_bitrate;
        let mut queued = 0usize;
        let mut skipped = 0usize;
        for track_id in track_ids {
            if self.downloads.is_running(track_id) || self.downloads.is_local(track_id) {
                skipped += 1;
                continue;
            }
            let Some(track) = ctx.library.tracks.get(&track_id).cloned() else {
                skipped += 1;
                continue;
            };
            let path = build_download_path(&self.config.download_dir, &ctx.library, &track);
            let task_id = self.downloads.begin(track_id);
            let session = Arc::clone(&ctx.session);
            let http = self.http.clone();
            let tx = self.tx.clone();
            tokio::spawn(async move {
                let result = download_track(session, http, track, bitrate, path).await;
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
    }

    // ---- authentication lifecycle ---------------------------------------

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
        let http = self.http.clone();
        let client_secret = self.config.client_secret.clone();
        let store = self.token_store.clone();
        tokio::spawn(async move {
            let result = Session::establish(http, client_id, client_secret, token, store).await;
            let _ = tx.send(BackendEvent::Session(Box::new(result)));
        });
    }

    fn start_logout(&mut self) {
        let Some(client_id) = self.config.client_id.clone() else {
            self.phase = Phase::NeedClientId;
            return;
        };

        self.stop_playback();
        let session = self.session.take();
        self.queue.clear();
        self.queue_selected = 0;
        self.selected = 0;
        self.downloads.clear();
        self.search_mode = false;
        self.phase = Phase::LoggingOut;
        self.status_line = "Logged out locally; revoking token...".to_owned();

        let tx = self.tx.clone();
        let http = self.http.clone();
        let client_secret = self.config.client_secret.clone();
        let store = self.token_store.clone();
        tokio::spawn(async move {
            let result = match session {
                Some(ctx) => {
                    // Stop the session from re-persisting a refreshed token
                    // (in-flight downloads share it) before deleting the
                    // stored one, then revoke.
                    let refresh_token = {
                        let mut session = ctx.session.lock().await;
                        session.disable_persistence();
                        session.refresh_token().to_owned()
                    };
                    store.delete(&client_id);
                    oauth::revoke_token(&http, &client_id, client_secret.as_deref(), &refresh_token)
                        .await
                }
                None => {
                    store.delete(&client_id);
                    Ok(())
                }
            };
            let _ = tx.send(BackendEvent::LoggedOut { client_id, result });
        });
    }

    // ---- backend events ---------------------------------------------------

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
                let Some(client_id) = self.config.client_id.clone() else {
                    self.phase = Phase::NeedClientId;
                    return;
                };
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
            }
            BackendEvent::Token(Err(err)) => self.phase = Phase::Error(err.to_string()),
            BackendEvent::Session(result) => match *result {
                Ok(established) => self.enter_ready(established),
                Err(err) => self.phase = Phase::Error(err.to_string()),
            },
            BackendEvent::StreamResolved {
                generation,
                track,
                bitrate,
                result,
            } => {
                // Discard events for playback intents that were superseded.
                if generation != self.playback_generation || self.playback != PlaybackPhase::Loading
                {
                    return;
                }
                match result {
                    Ok(url) => self.begin_stream(generation, *track, bitrate, url),
                    Err(err) => {
                        self.playback = PlaybackPhase::Idle;
                        self.status_line = format!("Stream failed: {err}");
                    }
                }
            }
            BackendEvent::StreamDecoded {
                generation,
                label,
                result,
            } => {
                if generation != self.playback_generation || self.playback != PlaybackPhase::Loading
                {
                    return;
                }
                match result {
                    Ok(source) => {
                        let Some(audio) = self.audio.as_mut() else {
                            self.cancel_stream_task();
                            self.playback = PlaybackPhase::Idle;
                            self.status_line = NO_AUDIO_DEVICE.to_owned();
                            return;
                        };
                        audio.play_source(source);
                        self.playback = PlaybackPhase::Active;
                        self.status_line = format!("Playing {label}");
                    }
                    Err(err) => {
                        self.cancel_stream_task();
                        self.playback = PlaybackPhase::Idle;
                        self.status_line = format!("Playback failed: {err}");
                    }
                }
            }
            BackendEvent::StreamInterrupted { generation, error } => {
                // The decoder will end early and tick() advances the queue;
                // just make sure the failure is visible.
                if generation == self.playback_generation {
                    self.status_line = format!("Stream error: {error}");
                }
            }
            BackendEvent::DownloadFinished { task_id, result } => {
                // Ignore completions from a session that was logged out.
                if self.downloads.complete(task_id, &result) {
                    self.status_line = match result {
                        Ok(path) => format!("Downloaded {}", path.display()),
                        Err(err) => err.to_string(),
                    };
                }
            }
            BackendEvent::LoggedOut { client_id, result } => {
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

    fn enter_ready(&mut self, established: EstablishedSession) {
        let ctx = SessionCtx {
            session: Arc::new(Mutex::new(established.session)),
            filtered_track_ids: established.library.sorted_track_ids(),
            server_bitrate: established.server_bitrate,
            library: established.library,
        };
        let count = ctx.library.tracks.len();
        self.downloads.clear();
        self.downloads
            .rescan(&ctx.library, &self.config.download_dir);
        self.session = Some(ctx);
        self.selected = 0;
        self.queue_selected = 0;
        self.active_view = View::Library;
        self.search_input.clear();
        self.phase = Phase::Ready;
        self.status_line = format!("Loaded {count} tracks");
        if let Some(warning) = &self.audio_warning {
            self.status_line = format!("{}; {warning}", self.status_line);
        }
    }

    // ---- rendering ---------------------------------------------------------

    fn render(&self, frame: &mut Frame<'_>) {
        match &self.phase {
            Phase::NeedClientId => {
                ui::login_screen(frame, &self.client_id_input, &self.status_line);
            }
            Phase::RequestingDeviceCode => ui::message_screen(
                frame,
                "Requesting device code",
                "Contacting oauth.ibroadcast.com...",
            ),
            Phase::Authorizing(device_code) => ui::authorizing_screen(frame, device_code),
            Phase::LoadingLibrary => ui::message_screen(
                frame,
                "Loading library",
                "Synchronizing your iBroadcast library...",
            ),
            Phase::LoggingOut => ui::message_screen(
                frame,
                "Logging out",
                "Revoking token and clearing session...",
            ),
            Phase::Error(message) => ui::error_screen(frame, message),
            Phase::Ready => {
                let Some(ctx) = &self.session else {
                    return;
                };
                ui::ready_screen(
                    frame,
                    &ui::ReadyScreen {
                        library: &ctx.library,
                        filtered_track_ids: &ctx.filtered_track_ids,
                        selected: self.selected,
                        queue: &self.queue,
                        queue_selected: self.queue_selected,
                        active_view: self.active_view,
                        downloads: &self.downloads,
                        status_line: &self.status_line,
                        now_playing: self
                            .queue
                            .current_track()
                            .map(|track_id| ctx.library.track_label(track_id)),
                        playback_bitrate: self.effective_playback_bitrate(),
                        playback_mode: self.queue.playback_mode(),
                        download_bitrate: self.config.download_bitrate,
                        search_input: self.search_mode.then_some(self.search_input.as_str()),
                    },
                );
            }
        }
    }
}

/// Removes a file, retrying briefly: right after stopping playback the audio
/// thread may still hold the file open for a few callbacks (notably on
/// Windows, where that blocks deletion).
fn remove_file_with_retry(path: &std::path::Path) -> io::Result<()> {
    let mut attempts = 0;
    loop {
        match fs::remove_file(path) {
            Err(err) if err.kind() == io::ErrorKind::PermissionDenied && attempts < 5 => {
                attempts += 1;
                std::thread::sleep(Duration::from_millis(50));
            }
            result => return result,
        }
    }
}

/// Resolves a stream URL and downloads it to disk; used by spawned tasks.
async fn download_track(
    session: Arc<Mutex<Session>>,
    http: Client,
    track: Track,
    bitrate: Bitrate,
    path: PathBuf,
) -> Result<PathBuf> {
    let result = async {
        let url = session.lock().await.stream_url(&track, bitrate).await?;
        download_to_file(&http, &url, &path).await
    }
    .await;
    match result {
        Ok(()) => Ok(path),
        Err(err) => Err(AppError::download_path(path, err)),
    }
}

fn action_for_key(key: KeyEvent) -> Option<Action> {
    Some(match key.code {
        KeyCode::Char('q') => Action::Quit,
        KeyCode::Tab => Action::NextView,
        KeyCode::Char('/') => Action::OpenSearch,
        KeyCode::Down | KeyCode::Char('j') => Action::MoveSelection(1),
        KeyCode::Up | KeyCode::Char('k') => Action::MoveSelection(-1),
        KeyCode::Enter => Action::Activate,
        KeyCode::Char('a') => Action::AddToQueue { all_visible: false },
        KeyCode::Char('A') => Action::AddToQueue { all_visible: true },
        KeyCode::Delete | KeyCode::Char('x') => Action::DeleteOrRemove,
        KeyCode::Char('[') | KeyCode::Char('<') => Action::MoveQueueItem(-1),
        KeyCode::Char(']') | KeyCode::Char('>') => Action::MoveQueueItem(1),
        KeyCode::Char('C') => Action::ClearQueue,
        KeyCode::Char('m') => Action::CyclePlaybackMode,
        KeyCode::Char('b') => Action::CyclePlaybackBitrate,
        KeyCode::Char('B') => Action::CycleDownloadBitrate,
        KeyCode::Char(' ') => Action::TogglePause,
        KeyCode::Char('n') => Action::NextTrack,
        KeyCode::Char('p') => Action::PreviousTrack,
        KeyCode::Char('d') => Action::Download { all_visible: false },
        KeyCode::Char('D') => Action::Download { all_visible: true },
        KeyCode::Char('L') => Action::Logout,
        KeyCode::Char('+') | KeyCode::Char('=') => Action::AdjustVolume(0.05),
        KeyCode::Char('-') => Action::AdjustVolume(-0.05),
        _ => return None,
    })
}

/// Puts the terminal into raw/alternate-screen mode and guarantees it is
/// restored on drop, including during unwinding after a panic.
struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<io::Stdout>>,
}

impl TerminalGuard {
    fn new() -> Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        if let Err(err) = execute!(stdout, EnterAlternateScreen) {
            let _ = disable_raw_mode();
            return Err(err.into());
        }
        match Terminal::new(CrosstermBackend::new(stdout)) {
            Ok(terminal) => Ok(Self { terminal }),
            Err(err) => {
                let _ = execute!(io::stdout(), LeaveAlternateScreen);
                let _ = disable_raw_mode();
                Err(err.into())
            }
        }
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}
