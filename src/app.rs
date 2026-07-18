use std::{
    fs, io,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

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
    audio::{AudioWorker, AudioWorkerEvent, DefaultOutputProbe},
    config::{Bitrate, Config},
    downloads::{
        DownloadManager, build_download_path, download_to_file, extension_from_mime,
        remove_empty_download_dirs, stream_to_buffer,
    },
    error::{AppError, Result},
    hls::stream_hls_to_buffer,
    library::{Library, Track},
    oauth::{self, DeviceCode, TokenSet},
    player::{
        AudioOutput, AudioStreamEvent, SinkEpoch, StreamSource, decode_file_from,
        decode_progressive_stream,
    },
    progressive::ProgressiveBuffer,
    queue::PlaybackQueue,
    session::{EstablishedSession, Session},
    storage::{TokenPersistence, TokenStore},
    ui::{self, View},
};

const SCOPES: &[&str] = &["user.library:read", "user.account:read"];
const AUDIO_PROBE_INTERVAL: Duration = Duration::from_millis(750);
const AUDIO_RETRY_MAX: Duration = Duration::from_secs(5);
const AUDIO_STABLE_RESET_AFTER: Duration = Duration::from_secs(10);

pub struct App {
    config: Config,
    token_store: TokenStore,
    http: Client,
    tx: mpsc::UnboundedSender<BackendEvent>,
    rx: mpsc::UnboundedReceiver<BackendEvent>,
    audio_worker: AudioWorker,
    audio_rx: mpsc::UnboundedReceiver<AudioWorkerEvent>,
    phase: Phase,
    client_id_input: String,
    search_input: String,
    search_mode: bool,
    active_view: View,
    selected: usize,
    queue_selected: usize,
    session: Option<SessionCtx>,
    audio: AudioState,
    audio_warning: Option<String>,
    audio_volume: f32,
    audio_failure_streak: u32,
    audio_ready_since: Option<Instant>,
    next_audio_open_attempt: u64,
    next_audio_probe: u64,
    audio_probe_in_flight: Option<(u64, Option<u64>)>,
    next_audio_probe_at: Instant,
    desired_playback: DesiredPlayback,
    playback: PlaybackPhase,
    /// Incremented whenever playback intent changes; stale stream events are
    /// discarded by comparing it.
    playback_generation: u64,
    /// Stable resume point retained while the physical output is replaced.
    /// It is cleared by every explicit track/stop intent.
    playback_checkpoint: Option<PlaybackCheckpoint>,
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
    generation: u64,
    track_id: u64,
    label: String,
    mime_type: String,
    extension_hint: String,
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
    /// A track is desired, but there is no healthy output yet.
    WaitingForAudio,
    /// A stream URL is being resolved in the background.
    Loading,
    /// A track is loaded into the audio output (playing or paused).
    Active,
}

enum AudioState {
    Opening { attempt_id: u64, retry_count: u32 },
    Ready(AudioOutput),
    Unavailable { retry_at: Instant, retry_count: u32 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DesiredPlayback {
    Stopped,
    Track { track_id: u64, paused: bool },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DesiredPlaybackAction {
    Play(u64),
    Pause,
    TogglePause,
    Stop,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PlaybackCheckpoint {
    generation: u64,
    track_id: u64,
    position: Duration,
}

impl DesiredPlayback {
    const fn track_id(self) -> Option<u64> {
        match self {
            Self::Stopped => None,
            Self::Track { track_id, .. } => Some(track_id),
        }
    }

    const fn paused(self) -> bool {
        matches!(self, Self::Track { paused: true, .. })
    }
}

fn reduce_desired_playback(
    current: DesiredPlayback,
    action: DesiredPlaybackAction,
) -> DesiredPlayback {
    match action {
        DesiredPlaybackAction::Play(track_id) => DesiredPlayback::Track {
            track_id,
            paused: false,
        },
        DesiredPlaybackAction::Pause => match current {
            DesiredPlayback::Stopped => DesiredPlayback::Stopped,
            DesiredPlayback::Track { track_id, .. } => DesiredPlayback::Track {
                track_id,
                paused: true,
            },
        },
        DesiredPlaybackAction::TogglePause => match current {
            DesiredPlayback::Stopped => DesiredPlayback::Stopped,
            DesiredPlayback::Track { track_id, paused } => DesiredPlayback::Track {
                track_id,
                paused: !paused,
            },
        },
        DesiredPlaybackAction::Stop => DesiredPlayback::Stopped,
    }
}

/// Captures an active track once and keeps that exact point through rapid
/// output changes. A replacement output has no player yet, so its apparent
/// zero position must never overwrite the original checkpoint.
fn checkpoint_after_audio_loss(
    existing: Option<PlaybackCheckpoint>,
    phase: PlaybackPhase,
    desired: DesiredPlayback,
    generation: u64,
    position: Option<Duration>,
) -> Option<PlaybackCheckpoint> {
    let track_id = desired.track_id()?;
    if let Some(checkpoint) = existing
        && checkpoint.generation == generation
        && checkpoint.track_id == track_id
    {
        return Some(checkpoint);
    }
    if phase != PlaybackPhase::Active {
        return None;
    }
    position.map(|position| PlaybackCheckpoint {
        generation,
        track_id,
        position,
    })
}

#[allow(clippy::too_many_arguments)]
fn decoded_source_is_current(
    event_generation: u64,
    event_track_id: u64,
    event_sink_epoch: Option<SinkEpoch>,
    event_position_base: Duration,
    playback_generation: u64,
    phase: PlaybackPhase,
    desired: DesiredPlayback,
    checkpoint: Option<PlaybackCheckpoint>,
    current_sink_epoch: Option<SinkEpoch>,
) -> bool {
    if event_generation != playback_generation || desired.track_id() != Some(event_track_id) {
        return false;
    }

    if let Some(event_sink_epoch) = event_sink_epoch {
        phase == PlaybackPhase::WaitingForAudio
            && current_sink_epoch == Some(event_sink_epoch)
            && checkpoint.is_some_and(|checkpoint| {
                checkpoint.generation == event_generation
                    && checkpoint.track_id == event_track_id
                    && checkpoint.position == event_position_base
            })
    } else {
        phase == PlaybackPhase::Loading && event_position_base.is_zero()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AudioCandidate {
    Accept,
    DefaultChanged,
    Failed,
    NoDefault,
}

fn classify_audio_candidate(
    output: &AudioOutput,
    observed_default: &DefaultOutputProbe,
) -> AudioCandidate {
    let observed_default = match observed_default {
        DefaultOutputProbe::Available(device_id) => Ok(Some(device_id)),
        DefaultOutputProbe::Unavailable => Ok(None),
        DefaultOutputProbe::Failed(_) => Err(()),
    };
    classify_audio_candidate_state(output.is_healthy(), output.device_id(), observed_default)
}

/// Pure candidate classifier used by the rodio adapter and race tests.
///
/// An identity query error is deliberately represented by `Err(())`, while a
/// successful query that found no default device is `Ok(None)`.
fn classify_audio_candidate_state<T: PartialEq>(
    healthy: bool,
    opened_device: Option<&T>,
    observed_default: std::result::Result<Option<&T>, ()>,
) -> AudioCandidate {
    if !healthy {
        return AudioCandidate::Failed;
    }

    match observed_default {
        Ok(None) => AudioCandidate::NoDefault,
        Err(()) => AudioCandidate::Accept,
        Ok(Some(current)) => match opened_device {
            Some(opened) if opened != current => AudioCandidate::DefaultChanged,
            // An identity lookup failure must not discard an output that was
            // successfully opened from the default device moments earlier.
            Some(_) | None => AudioCandidate::Accept,
        },
    }
}

fn open_attempt_retry_count(state: &AudioState, attempt_id: u64) -> Option<u32> {
    match state {
        AudioState::Opening {
            attempt_id: current,
            retry_count,
        } if *current == attempt_id => Some(*retry_count),
        AudioState::Opening { .. } | AudioState::Ready(_) | AudioState::Unavailable { .. } => None,
    }
}

fn should_advance_queue(
    phase: PlaybackPhase,
    desired: DesiredPlayback,
    current_track: Option<u64>,
    output_healthy: bool,
    track_finished: bool,
) -> bool {
    phase == PlaybackPhase::Active
        && desired.track_id() == current_track
        && current_track.is_some()
        && output_healthy
        && track_finished
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProbeEventDisposition {
    Ignore,
    ConsumeStale,
    Apply,
}

fn classify_probe_event(
    in_flight: Option<(u64, Option<u64>)>,
    probe_id: u64,
    sink_epoch: Option<u64>,
    current_sink_epoch: Option<u64>,
) -> ProbeEventDisposition {
    if in_flight != Some((probe_id, sink_epoch)) {
        ProbeEventDisposition::Ignore
    } else if sink_epoch != current_sink_epoch {
        ProbeEventDisposition::ConsumeStale
    } else {
        ProbeEventDisposition::Apply
    }
}

fn audio_retry_delay(retry_count: u32) -> Duration {
    let exponent = retry_count.saturating_sub(1).min(4);
    let millis = (500_u64 << exponent).min(AUDIO_RETRY_MAX.as_millis() as u64);
    Duration::from_millis(millis)
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
    /// A decoder was prepared for initial playback or checkpoint recovery.
    StreamDecoded {
        generation: u64,
        track_id: u64,
        /// Present only when rebuilding on a replacement physical output.
        sink_epoch: Option<SinkEpoch>,
        position_base: Duration,
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
        let (audio_worker, audio_rx) = AudioWorker::start();
        let mut queue = PlaybackQueue::default();
        queue.set_playback_mode(config.playback_mode);

        let mut app = Self {
            config,
            token_store,
            http: Client::new(),
            tx,
            rx,
            audio_worker,
            audio_rx,
            phase: Phase::NeedClientId,
            client_id_input: String::new(),
            search_input: String::new(),
            search_mode: false,
            active_view: View::Library,
            selected: 0,
            queue_selected: 0,
            session: None,
            audio: AudioState::Unavailable {
                retry_at: Instant::now(),
                retry_count: 0,
            },
            audio_warning: Some("Opening audio output...".to_owned()),
            audio_volume: 0.8,
            audio_failure_streak: 0,
            audio_ready_since: None,
            next_audio_open_attempt: 0,
            next_audio_probe: 0,
            audio_probe_in_flight: None,
            next_audio_probe_at: Instant::now(),
            desired_playback: DesiredPlayback::Stopped,
            playback: PlaybackPhase::Idle,
            playback_generation: 0,
            playback_checkpoint: None,
            stream_task: None,
            queue,
            downloads: DownloadManager::default(),
            status_line: String::new(),
            should_quit: false,
        };
        app.begin_audio_open(0, None, "Opening audio output...");
        app
    }

    fn begin_audio_open(&mut self, retry_count: u32, previous: Option<AudioOutput>, message: &str) {
        self.next_audio_open_attempt = self.next_audio_open_attempt.wrapping_add(1).max(1);
        let attempt_id = self.next_audio_open_attempt;
        self.audio = AudioState::Opening {
            attempt_id,
            retry_count,
        };
        self.audio_ready_since = None;
        self.audio_warning = Some(message.to_owned());
        if self.desired_playback.track_id().is_some() {
            self.playback = PlaybackPhase::WaitingForAudio;
        }
        self.audio_worker.open(attempt_id, previous);
    }

    /// Freezes logical playback before a physical output is torn down.
    ///
    /// Returning `true` means an active source can be rebuilt from the retained
    /// local file or progressive buffer, so its generation and feeder must stay
    /// alive. Returning `false` leaves the older full-invalidation path in
    /// charge (for example when the device changes while a URL is still being
    /// resolved).
    fn checkpoint_playback_for_audio_recovery(&mut self) -> bool {
        let position = match &mut self.audio {
            AudioState::Ready(output) => {
                output.set_paused(true);
                output.position()
            }
            AudioState::Opening { .. } | AudioState::Unavailable { .. } => None,
        };
        self.playback_checkpoint = checkpoint_after_audio_loss(
            self.playback_checkpoint,
            self.playback,
            self.desired_playback,
            self.playback_generation,
            position,
        );

        if let Some(checkpoint) = self.playback_checkpoint {
            self.desired_playback =
                reduce_desired_playback(self.desired_playback, DesiredPlaybackAction::Pause);
            if let Some(task) = self.stream_task.as_ref()
                && task.generation == checkpoint.generation
                && task.track_id == checkpoint.track_id
            {
                // Wake the old device's decoder without failing the shared
                // buffer or stopping its network feeder. The replacement
                // output will create a reader in the new reader epoch.
                task.buffer.cancel_current_readers();
            }
            self.playback = PlaybackPhase::WaitingForAudio;
            true
        } else {
            false
        }
    }

    fn begin_audio_recovery(&mut self, message: String) {
        if matches!(self.audio, AudioState::Opening { .. }) {
            return;
        }

        let can_resume_in_place = self.checkpoint_playback_for_audio_recovery();

        let old_state = std::mem::replace(
            &mut self.audio,
            AudioState::Unavailable {
                retry_at: Instant::now(),
                retry_count: 0,
            },
        );
        let (previous, retry_count) = match old_state {
            AudioState::Ready(mut output) => {
                output.stop();
                (Some(output), 0)
            }
            AudioState::Unavailable { retry_count, .. } => (None, retry_count),
            AudioState::Opening { .. } => unreachable!("opening state handled above"),
        };

        if !can_resume_in_place {
            self.invalidate_playback_pipeline_for_audio_loss();
        }
        self.begin_audio_open(retry_count, previous, &message);
    }

    fn begin_audio_failure_recovery(&mut self, message: String) {
        if matches!(self.audio, AudioState::Opening { .. }) {
            return;
        }
        let can_resume_in_place = self.checkpoint_playback_for_audio_recovery();
        if self
            .audio_ready_since
            .is_some_and(|since| since.elapsed() >= AUDIO_STABLE_RESET_AFTER)
        {
            self.audio_failure_streak = 0;
        }
        self.audio_failure_streak = self.audio_failure_streak.saturating_add(1);

        let old_state = std::mem::replace(
            &mut self.audio,
            AudioState::Unavailable {
                retry_at: Instant::now(),
                retry_count: self.audio_failure_streak.saturating_sub(1),
            },
        );
        let previous = match old_state {
            AudioState::Ready(mut output) => {
                output.stop();
                Some(output)
            }
            AudioState::Unavailable { .. } => None,
            AudioState::Opening { .. } => unreachable!("opening state handled above"),
        };
        if !can_resume_in_place {
            self.invalidate_playback_pipeline_for_audio_loss();
        }

        if self.audio_failure_streak == 1 {
            self.begin_audio_open(0, previous, &message);
        } else {
            if let Some(output) = previous {
                self.audio_worker.dispose(output);
            }
            self.set_audio_unavailable(self.audio_failure_streak.saturating_sub(1), message);
        }
    }

    fn invalidate_playback_pipeline_for_audio_loss(&mut self) {
        self.playback_generation = self.playback_generation.wrapping_add(1);
        self.cancel_stream_task();
        self.playback = if self.desired_playback.track_id().is_some() {
            PlaybackPhase::WaitingForAudio
        } else {
            PlaybackPhase::Idle
        };
    }

    fn set_audio_unavailable(&mut self, retry_count: u32, message: String) {
        let delay = audio_retry_delay(retry_count);
        self.audio_failure_streak = self.audio_failure_streak.max(retry_count);
        self.audio_ready_since = None;
        self.audio = AudioState::Unavailable {
            retry_at: Instant::now() + delay,
            retry_count,
        };
        self.audio_warning = Some(format!(
            "{message}; retrying audio output in {:.1}s",
            delay.as_secs_f32()
        ));
        self.playback = if self.desired_playback.track_id().is_some() {
            PlaybackPhase::WaitingForAudio
        } else {
            PlaybackPhase::Idle
        };
        self.next_audio_probe_at = Instant::now() + AUDIO_PROBE_INTERVAL;
    }

    fn handle_audio_worker_event(&mut self, event: AudioWorkerEvent) {
        match event {
            AudioWorkerEvent::OpenFinished {
                attempt_id,
                result,
                observed_default,
            } => self.handle_audio_open_finished(attempt_id, result, observed_default),
            AudioWorkerEvent::ProbeFinished {
                probe_id,
                sink_epoch,
                result,
            } => {
                self.handle_audio_probe_finished(probe_id, sink_epoch, result);
            }
            AudioWorkerEvent::Stream(event) => self.handle_audio_stream_event(event),
        }
    }

    fn handle_audio_open_finished(
        &mut self,
        attempt_id: u64,
        result: Result<AudioOutput>,
        observed_default: DefaultOutputProbe,
    ) {
        let Some(retry_count) = open_attempt_retry_count(&self.audio, attempt_id) else {
            if let Ok(output) = result {
                self.audio_worker.dispose(output);
            }
            return;
        };

        match result {
            Ok(mut output) => match classify_audio_candidate(&output, &observed_default) {
                AudioCandidate::Accept => {
                    output.set_volume(self.audio_volume);
                    self.audio = AudioState::Ready(output);
                    self.audio_warning = None;
                    self.audio_ready_since = Some(Instant::now());
                    self.next_audio_probe_at = Instant::now() + AUDIO_PROBE_INTERVAL;
                    if self.playback_checkpoint.is_some() {
                        self.restore_playback_checkpoint();
                    } else if self.desired_playback.track_id().is_some() {
                        self.resume_desired_track();
                    } else {
                        self.playback = PlaybackPhase::Idle;
                    }
                }
                AudioCandidate::DefaultChanged => {
                    self.audio = AudioState::Unavailable {
                        retry_at: Instant::now(),
                        retry_count,
                    };
                    self.begin_audio_open(
                        retry_count,
                        Some(output),
                        "Default audio output changed; reconnecting...",
                    );
                }
                AudioCandidate::Failed => {
                    self.audio_worker.dispose(output);
                    self.set_audio_unavailable(
                        retry_count.saturating_add(1),
                        "Audio output failed while opening".to_owned(),
                    );
                }
                AudioCandidate::NoDefault => {
                    self.audio_worker.dispose(output);
                    self.set_audio_unavailable(
                        retry_count.saturating_add(1),
                        "No default audio output device available".to_owned(),
                    );
                }
            },
            Err(error) => self.set_audio_unavailable(
                retry_count.saturating_add(1),
                format!("Audio output unavailable: {error}"),
            ),
        }
    }

    fn handle_audio_probe_finished(
        &mut self,
        probe_id: u64,
        sink_epoch: Option<u64>,
        result: DefaultOutputProbe,
    ) {
        let current_epoch = match &self.audio {
            AudioState::Ready(output) => Some(output.sink_epoch()),
            AudioState::Opening { .. } | AudioState::Unavailable { .. } => None,
        };
        match classify_probe_event(
            self.audio_probe_in_flight,
            probe_id,
            sink_epoch,
            current_epoch,
        ) {
            ProbeEventDisposition::Ignore => return,
            ProbeEventDisposition::ConsumeStale => {
                self.audio_probe_in_flight = None;
                self.next_audio_probe_at = Instant::now() + AUDIO_PROBE_INTERVAL;
                return;
            }
            ProbeEventDisposition::Apply => {
                self.audio_probe_in_flight = None;
                self.next_audio_probe_at = Instant::now() + AUDIO_PROBE_INTERVAL;
            }
        }

        let ready_candidate = match &self.audio {
            AudioState::Ready(output) => Some(classify_audio_candidate(output, &result)),
            AudioState::Opening { .. } | AudioState::Unavailable { .. } => None,
        };
        if let Some(candidate) = ready_candidate {
            match candidate {
                AudioCandidate::Accept => {
                    if let DefaultOutputProbe::Failed(error) = result {
                        tracing::warn!("could not identify default audio output: {error}");
                    }
                }
                AudioCandidate::DefaultChanged => self.begin_audio_recovery(
                    "Default audio output changed; reconnecting...".to_owned(),
                ),
                AudioCandidate::NoDefault => self.begin_audio_recovery(
                    "No default audio output device; waiting for one...".to_owned(),
                ),
                AudioCandidate::Failed => self.begin_audio_failure_recovery(
                    "Audio output failed; reconnecting...".to_owned(),
                ),
            }
            return;
        }

        match (&self.audio, &result) {
            (AudioState::Unavailable { .. }, DefaultOutputProbe::Available(_)) => {
                self.begin_audio_recovery("Audio output detected; reconnecting...".to_owned());
            }
            (AudioState::Unavailable { .. }, DefaultOutputProbe::Failed(error)) => {
                tracing::warn!("could not identify default audio output: {error}");
            }
            _ => {}
        }
    }

    fn handle_audio_stream_event(&mut self, event: AudioStreamEvent) {
        let is_current = matches!(
            &self.audio,
            AudioState::Ready(output) if output.sink_epoch() == event.sink_epoch
        );
        if !is_current {
            return;
        }

        if event.error.is_fatal() {
            tracing::warn!(
                sink_epoch = event.sink_epoch,
                error = %event.error,
                "audio output failed"
            );
            self.begin_audio_failure_recovery(format!("{}; reconnecting...", event.error));
        } else {
            tracing::warn!(
                sink_epoch = event.sink_epoch,
                error = %event.error,
                "non-fatal audio output warning"
            );
        }
    }

    fn tick_audio_lifecycle(&mut self) {
        let now = Instant::now();
        if matches!(self.audio, AudioState::Ready(_))
            && self
                .audio_ready_since
                .is_some_and(|since| now.duration_since(since) >= AUDIO_STABLE_RESET_AFTER)
        {
            self.audio_failure_streak = 0;
        }
        if let AudioState::Unavailable {
            retry_at,
            retry_count,
        } = &self.audio
            && now >= *retry_at
        {
            self.begin_audio_open(*retry_count, None, "Retrying audio output...");
            return;
        }

        if matches!(self.audio, AudioState::Opening { .. })
            || self.audio_probe_in_flight.is_some()
            || now < self.next_audio_probe_at
        {
            return;
        }

        self.next_audio_probe = self.next_audio_probe.wrapping_add(1).max(1);
        let probe_id = self.next_audio_probe;
        let sink_epoch = match &self.audio {
            AudioState::Ready(output) => Some(output.sink_epoch()),
            AudioState::Opening { .. } | AudioState::Unavailable { .. } => None,
        };
        self.audio_probe_in_flight = Some((probe_id, sink_epoch));
        self.next_audio_probe_at = now + AUDIO_PROBE_INTERVAL;
        self.audio_worker.probe(probe_id, sink_epoch);
    }

    pub async fn run(mut self) -> Result<()> {
        self.bootstrap();
        let mut terminal = match TerminalGuard::new() {
            Ok(terminal) => terminal,
            Err(error) => {
                self.shutdown_audio().await;
                return Err(error);
            }
        };
        let result = self.run_loop(&mut terminal.terminal).await;
        // Restore the user's terminal before waiting for a platform audio open
        // or teardown that happened to be in flight at exit.
        drop(terminal);
        self.shutdown_audio().await;
        result
    }

    async fn shutdown_audio(&mut self) {
        self.stop_requested_playback();
        let state = std::mem::replace(
            &mut self.audio,
            AudioState::Unavailable {
                retry_at: Instant::now(),
                retry_count: 0,
            },
        );
        let current = match state {
            AudioState::Ready(output) => Some(output),
            AudioState::Opening { .. } | AudioState::Unavailable { .. } => None,
        };
        let mut acknowledged = self.audio_worker.shutdown(current);

        loop {
            tokio::select! {
                result = &mut acknowledged => {
                    if result.is_err() {
                        tracing::warn!("audio worker stopped before acknowledging shutdown");
                    }
                    break;
                }
                event = self.audio_rx.recv() => match event {
                    Some(AudioWorkerEvent::OpenFinished {
                        result: Ok(output),
                        ..
                    }) => self.audio_worker.dispose(output),
                    Some(_) => {}
                    None => {
                        let _ = acknowledged.await;
                        break;
                    }
                }
            }
        }

        // No successful candidate can remain leased after acknowledgement,
        // but drain already queued non-owning events before the receiver drops.
        while let Ok(event) = self.audio_rx.try_recv() {
            if let AudioWorkerEvent::OpenFinished {
                result: Ok(output), ..
            } = event
            {
                self.audio_worker.dispose(output);
            }
        }
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
            while let Ok(event) = self.audio_rx.try_recv() {
                self.handle_audio_worker_event(event);
            }

            if event::poll(Duration::from_millis(50))?
                && let Event::Key(key) = event::read()?
                && key.kind == KeyEventKind::Press
            {
                self.handle_key(key);
            }

            self.tick_audio_lifecycle();
            self.tick();
        }
        Ok(())
    }

    /// Advances playback when the current track has finished.
    fn tick(&mut self) {
        let (output_healthy, track_finished) = match &self.audio {
            AudioState::Ready(output) => (output.is_healthy(), output.is_finished()),
            AudioState::Opening { .. } | AudioState::Unavailable { .. } => (false, false),
        };
        if !should_advance_queue(
            self.playback,
            self.desired_playback,
            self.queue.current_track(),
            output_healthy,
            track_finished,
        ) {
            return;
        }
        if self.queue.next().is_some() {
            self.request_playback_current();
        } else {
            self.stop_requested_playback();
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
                    self.request_playback_current();
                }
            }
            Action::PreviousTrack => {
                if self.queue.previous().is_some() {
                    self.request_playback_current();
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
                    self.request_playback_current();
                }
            }
            View::Queue => {
                if self.queue.play_index(self.queue_selected).is_some() {
                    self.request_playback_current();
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

        if self.desired_playback.track_id() == Some(track_id) {
            self.stop_requested_playback();
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
        let removed = self.queue.remove(selected);
        if self.queue_selected >= self.queue.len() {
            self.queue_selected = self.queue.len().saturating_sub(1);
        }

        if removed.is_some() && removed == self.desired_playback.track_id() {
            if self.queue.current_track().is_some() {
                self.request_playback_current();
            } else {
                self.stop_requested_playback();
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
        self.stop_requested_playback();
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
        self.config.playback_mode = mode;
        self.status_line = match self.config.save() {
            Ok(()) => format!("Playback mode set to {mode}"),
            Err(err) => format!("Playback mode set to {mode}, but config save failed: {err}"),
        };
    }

    // ---- playback -------------------------------------------------------

    /// Rebuilds the current source on the newly opened output without
    /// restarting its transfer. The prepared source is tagged with both the
    /// playback generation and sink epoch so rapid device changes cannot
    /// attach it to the wrong output.
    fn restore_playback_checkpoint(&mut self) {
        let Some(checkpoint) = self.playback_checkpoint else {
            return;
        };
        if checkpoint.generation != self.playback_generation
            || self.desired_playback.track_id() != Some(checkpoint.track_id)
        {
            self.playback_checkpoint = None;
            self.resume_desired_track();
            return;
        }

        let sink_epoch = match &self.audio {
            AudioState::Ready(output) if output.is_healthy() => output.sink_epoch(),
            AudioState::Ready(_) | AudioState::Opening { .. } | AudioState::Unavailable { .. } => {
                self.playback = PlaybackPhase::WaitingForAudio;
                return;
            }
        };

        let retained_stream = self.stream_task.as_ref().and_then(|task| {
            (task.generation == checkpoint.generation && task.track_id == checkpoint.track_id).then(
                || {
                    (
                        task.buffer.reader(),
                        task.label.clone(),
                        task.mime_type.clone(),
                        task.extension_hint.clone(),
                    )
                },
            )
        });
        if let Some((reader, label, mime_type, extension_hint)) = retained_stream {
            let tx = self.tx.clone();
            tokio::task::spawn_blocking(move || {
                let result = decode_progressive_stream(
                    reader,
                    &mime_type,
                    &extension_hint,
                    checkpoint.position,
                );
                let _ = tx.send(BackendEvent::StreamDecoded {
                    generation: checkpoint.generation,
                    track_id: checkpoint.track_id,
                    sink_epoch: Some(sink_epoch),
                    position_base: checkpoint.position,
                    label,
                    result,
                });
            });
            self.playback = PlaybackPhase::WaitingForAudio;
            self.status_line = "Audio output changed; restoring paused playback...".to_owned();
            return;
        }

        let local_source = self.session.as_ref().and_then(|ctx| {
            let track = ctx.library.tracks.get(&checkpoint.track_id)?;
            let path = build_download_path(&self.config.download_dir, &ctx.library, track);
            if !path.exists() {
                return None;
            }
            let mime_type = if track.mime_type.trim().is_empty() {
                "application/octet-stream".to_owned()
            } else {
                track.mime_type.clone()
            };
            let extension_hint = path
                .extension()
                .and_then(|extension| extension.to_str())
                .filter(|extension| !extension.trim().is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| extension_from_mime(&mime_type).to_owned());
            Some((
                path,
                ctx.library.track_label(checkpoint.track_id),
                mime_type,
                extension_hint,
            ))
        });
        let Some((path, label, mime_type, extension_hint)) = local_source else {
            self.stop_requested_playback();
            self.status_line =
                "Cannot resume after audio output change: retained source is unavailable"
                    .to_owned();
            return;
        };

        let tx = self.tx.clone();
        tokio::task::spawn_blocking(move || {
            let result = decode_file_from(&path, &mime_type, &extension_hint, checkpoint.position);
            let _ = tx.send(BackendEvent::StreamDecoded {
                generation: checkpoint.generation,
                track_id: checkpoint.track_id,
                sink_epoch: Some(sink_epoch),
                position_base: checkpoint.position,
                label,
                result,
            });
        });
        self.playback = PlaybackPhase::WaitingForAudio;
        self.status_line = "Audio output changed; restoring paused playback...".to_owned();
    }

    /// Records a new user playback intent for the queue's current track.
    fn request_playback_current(&mut self) {
        let Some(track_id) = self.queue.current_track() else {
            self.stop_requested_playback();
            return;
        };
        self.desired_playback =
            reduce_desired_playback(self.desired_playback, DesiredPlaybackAction::Play(track_id));
        self.start_desired_track();
    }

    /// Restarts the latest desired track after recovering an audio output.
    /// Unlike a user activation this preserves the desired pause state.
    fn resume_desired_track(&mut self) {
        if self.desired_playback.track_id().is_some() {
            self.start_desired_track();
        }
    }

    /// Starts the physical playback pipeline for the latest desired track.
    fn start_desired_track(&mut self) {
        self.playback_checkpoint = None;
        self.playback_generation = self.playback_generation.wrapping_add(1);
        let generation = self.playback_generation;
        self.cancel_stream_task();

        let DesiredPlayback::Track { track_id, paused } = self.desired_playback else {
            self.playback = PlaybackPhase::Idle;
            return;
        };
        let Some(ctx) = &self.session else {
            self.stop_requested_playback();
            self.status_line = "Not logged in".to_owned();
            return;
        };
        let Some(track) = ctx.library.tracks.get(&track_id).cloned() else {
            self.stop_requested_playback();
            self.status_line = format!("Unknown track id {track_id}");
            return;
        };
        let label = ctx.library.track_label(track_id);
        let session = Arc::clone(&ctx.session);
        let local_path = self
            .downloads
            .is_local(track_id)
            .then(|| build_download_path(&self.config.download_dir, &ctx.library, &track));

        let output_healthy = matches!(
            &self.audio,
            AudioState::Ready(output) if output.is_healthy()
        );
        if !output_healthy {
            self.playback = PlaybackPhase::WaitingForAudio;
            if matches!(&self.audio, AudioState::Ready(_)) {
                self.begin_audio_failure_recovery(
                    "Audio output failed; reconnecting...".to_owned(),
                );
            }
            return;
        }

        if let AudioState::Ready(output) = &mut self.audio {
            // Silence the previous track while a new URL is resolved.
            output.stop();
        }

        if let Some(path) = local_path {
            if path.exists() {
                self.play_local(&track, &label, path, paused);
                return;
            }
            // Deleted externally; fall through to streaming.
            self.downloads.mark_not_local(track_id);
        }

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

    fn play_local(&mut self, track: &Track, label: &str, path: PathBuf, paused: bool) {
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

        let result = match &mut self.audio {
            AudioState::Ready(output) if output.is_healthy() => {
                output.play_file(&path, mime_type, &extension_hint, paused)
            }
            _ => {
                self.playback = PlaybackPhase::WaitingForAudio;
                return;
            }
        };

        match result {
            Ok(()) => {
                self.playback = PlaybackPhase::Active;
                self.status_line = format!("Playing local file: {label}");
            }
            Err(err) => {
                let output_failed = matches!(
                    &self.audio,
                    AudioState::Ready(output) if !output.is_healthy()
                );
                if output_failed {
                    self.begin_audio_failure_recovery(format!("{err}; reconnecting..."));
                    return;
                }
                self.stop_requested_playback();
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
        self.stream_task = Some(StreamTask {
            handle,
            buffer,
            generation,
            track_id: track.id,
            label: label.clone(),
            mime_type: mime_type.clone(),
            extension_hint: extension_hint.clone(),
        });

        let track_id = track.id;
        let tx = self.tx.clone();
        tokio::task::spawn_blocking(move || {
            let result =
                decode_progressive_stream(reader, &mime_type, &extension_hint, Duration::ZERO);
            let _ = tx.send(BackendEvent::StreamDecoded {
                generation,
                track_id,
                sink_epoch: None,
                position_base: Duration::ZERO,
                label,
                result,
            });
        });
    }

    fn toggle_pause(&mut self) {
        let DesiredPlayback::Track { .. } = self.desired_playback else {
            self.status_line = "Nothing playing".to_owned();
            return;
        };
        self.desired_playback =
            reduce_desired_playback(self.desired_playback, DesiredPlaybackAction::TogglePause);
        let paused = self.desired_playback.paused();
        if let AudioState::Ready(output) = &mut self.audio
            && output.is_healthy()
            && self.playback == PlaybackPhase::Active
        {
            output.set_paused(paused);
        }
        self.status_line = if paused {
            "Paused".to_owned()
        } else {
            "Playing".to_owned()
        };
    }

    fn adjust_volume(&mut self, delta: f32) {
        self.audio_volume = (self.audio_volume + delta).clamp(0.0, 1.0);
        if let AudioState::Ready(output) = &mut self.audio {
            output.set_volume(self.audio_volume);
        }
        self.status_line = format!("Volume {:.0}%", self.audio_volume * 100.0);
    }

    fn stop_requested_playback(&mut self) {
        self.desired_playback =
            reduce_desired_playback(self.desired_playback, DesiredPlaybackAction::Stop);
        self.playback = PlaybackPhase::Idle;
        self.playback_checkpoint = None;
        self.playback_generation = self.playback_generation.wrapping_add(1);
        self.cancel_stream_task();
        if let AudioState::Ready(output) = &mut self.audio {
            output.stop();
        }
    }

    fn cancel_stream_task(&mut self) {
        if let Some(task) = self.stream_task.take() {
            task.handle.abort();
            task.buffer.cancel_current_readers();
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
        self.stop_requested_playback();
        let Some(client_id) = self.config.client_id.clone() else {
            self.session = None;
            self.queue.clear();
            self.phase = Phase::NeedClientId;
            return;
        };

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
                    Ok(url) if self.desired_playback.track_id() == Some(track.id) => {
                        self.begin_stream(generation, *track, bitrate, url)
                    }
                    Ok(_) => (),
                    Err(err) => {
                        self.stop_requested_playback();
                        self.status_line = format!("Stream failed: {err}");
                    }
                }
            }
            BackendEvent::StreamDecoded {
                generation,
                track_id,
                sink_epoch,
                position_base,
                label,
                result,
            } => {
                let restoring = sink_epoch.is_some();
                let current_sink_epoch = match &self.audio {
                    AudioState::Ready(output) => Some(output.sink_epoch()),
                    AudioState::Opening { .. } | AudioState::Unavailable { .. } => None,
                };
                let event_is_current = decoded_source_is_current(
                    generation,
                    track_id,
                    sink_epoch,
                    position_base,
                    self.playback_generation,
                    self.playback,
                    self.desired_playback,
                    self.playback_checkpoint,
                    current_sink_epoch,
                );
                if !event_is_current {
                    return;
                }
                match result {
                    Ok(source) => {
                        let paused = self.desired_playback.paused();
                        let play_result = match &mut self.audio {
                            AudioState::Ready(output) if output.is_healthy() => output
                                .play_source(source, position_base, paused)
                                .and_then(|()| {
                                    if output.is_healthy() {
                                        Ok(())
                                    } else {
                                        Err(AppError::Playback(
                                            "audio output failed while starting playback".into(),
                                        ))
                                    }
                                }),
                            _ => {
                                if !restoring {
                                    self.cancel_stream_task();
                                }
                                self.playback = PlaybackPhase::WaitingForAudio;
                                return;
                            }
                        };
                        if let Err(err) = play_result {
                            if !restoring {
                                self.cancel_stream_task();
                            }
                            let output_failed = matches!(
                                &self.audio,
                                AudioState::Ready(output) if !output.is_healthy()
                            );
                            if output_failed {
                                self.begin_audio_failure_recovery(format!(
                                    "{err}; reconnecting..."
                                ));
                            } else {
                                self.stop_requested_playback();
                                self.status_line = format!("Playback failed: {err}");
                            }
                            return;
                        }
                        self.playback = PlaybackPhase::Active;
                        if restoring {
                            self.playback_checkpoint = None;
                            self.status_line = if paused {
                                format!(
                                    "Paused after audio output change: {label}; press Space to resume"
                                )
                            } else {
                                format!("Playing {label}")
                            };
                        } else {
                            self.status_line = if paused {
                                format!("Paused {label}")
                            } else {
                                format!("Playing {label}")
                            };
                        }
                    }
                    Err(err) => {
                        self.stop_requested_playback();
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
        self.stop_requested_playback();
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
    }

    // ---- rendering ---------------------------------------------------------

    fn playback_summary(&self, library: &Library) -> ui::PlaybackSummary {
        let state = self.playback_state();
        let Some(track_id) = self.queue.current_track() else {
            return ui::PlaybackSummary {
                label: "Nothing playing".to_owned(),
                state,
                elapsed: None,
                duration: None,
            };
        };

        let label = library.track_label(track_id);
        let Some(track) = library.tracks.get(&track_id) else {
            return ui::PlaybackSummary {
                label,
                state,
                elapsed: None,
                duration: None,
            };
        };

        let duration = Duration::from_secs(track.length.max(0) as u64);
        let elapsed = match self.playback {
            PlaybackPhase::Active => match &self.audio {
                AudioState::Ready(output) => output.position().unwrap_or_default(),
                AudioState::Opening { .. } | AudioState::Unavailable { .. } => Duration::ZERO,
            },
            PlaybackPhase::WaitingForAudio => self
                .playback_checkpoint
                .filter(|checkpoint| {
                    checkpoint.generation == self.playback_generation
                        && checkpoint.track_id == track_id
                })
                .map_or(Duration::ZERO, |checkpoint| checkpoint.position),
            PlaybackPhase::Idle | PlaybackPhase::Loading => Duration::ZERO,
        };
        let elapsed = if duration == Duration::ZERO {
            elapsed
        } else {
            elapsed.min(duration)
        };

        ui::PlaybackSummary {
            label,
            state,
            elapsed: Some(elapsed),
            duration: Some(duration),
        }
    }

    fn playback_state(&self) -> ui::PlaybackState {
        match self.playback {
            PlaybackPhase::Idle => ui::PlaybackState::Stopped,
            PlaybackPhase::WaitingForAudio if self.playback_checkpoint.is_some() => {
                ui::PlaybackState::Paused
            }
            PlaybackPhase::WaitingForAudio => ui::PlaybackState::WaitingForAudio,
            PlaybackPhase::Loading => ui::PlaybackState::Loading,
            PlaybackPhase::Active => {
                if self.desired_playback.paused() {
                    ui::PlaybackState::Paused
                } else {
                    ui::PlaybackState::Playing
                }
            }
        }
    }

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
                        audio_warning: self.audio_warning.as_deref(),
                        playback: self.playback_summary(&ctx.library),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delay_uses_bounded_exponential_backoff() {
        assert_eq!(audio_retry_delay(0), Duration::from_millis(500));
        assert_eq!(audio_retry_delay(1), Duration::from_millis(500));
        assert_eq!(audio_retry_delay(2), Duration::from_secs(1));
        assert_eq!(audio_retry_delay(3), Duration::from_secs(2));
        assert_eq!(audio_retry_delay(4), Duration::from_secs(4));
        assert_eq!(audio_retry_delay(5), AUDIO_RETRY_MAX);
        assert_eq!(audio_retry_delay(u32::MAX), AUDIO_RETRY_MAX);
    }

    #[test]
    fn latest_playback_intent_wins_and_stop_cancels_recovery() {
        let mut desired = DesiredPlayback::Stopped;
        desired = reduce_desired_playback(desired, DesiredPlaybackAction::Play(10));
        desired = reduce_desired_playback(desired, DesiredPlaybackAction::TogglePause);
        assert_eq!(
            desired,
            DesiredPlayback::Track {
                track_id: 10,
                paused: true,
            }
        );

        // Models Next/Activate while the audio worker is reopening. The
        // eventual resume must use only the most recent requested track.
        desired = reduce_desired_playback(desired, DesiredPlaybackAction::Play(20));
        desired = reduce_desired_playback(desired, DesiredPlaybackAction::Play(30));
        assert_eq!(desired.track_id(), Some(30));
        assert!(!desired.paused());

        // Models ClearQueue/Logout: a late open result has no track to resume.
        desired = reduce_desired_playback(desired, DesiredPlaybackAction::Stop);
        assert_eq!(desired, DesiredPlayback::Stopped);
        assert_eq!(
            reduce_desired_playback(desired, DesiredPlaybackAction::TogglePause),
            DesiredPlayback::Stopped
        );
        assert_eq!(
            reduce_desired_playback(desired, DesiredPlaybackAction::Pause),
            DesiredPlayback::Stopped
        );
    }

    #[test]
    fn audio_loss_checkpoints_position_and_forces_an_idempotent_pause() {
        let desired = DesiredPlayback::Track {
            track_id: 42,
            paused: false,
        };
        let position = Duration::from_secs(73);

        assert_eq!(
            checkpoint_after_audio_loss(None, PlaybackPhase::Active, desired, 11, Some(position),),
            Some(PlaybackCheckpoint {
                generation: 11,
                track_id: 42,
                position,
            })
        );
        let paused = reduce_desired_playback(desired, DesiredPlaybackAction::Pause);
        assert_eq!(
            paused,
            DesiredPlayback::Track {
                track_id: 42,
                paused: true,
            }
        );
        assert_eq!(
            reduce_desired_playback(paused, DesiredPlaybackAction::Pause),
            paused
        );
    }

    #[test]
    fn rapid_audio_failures_keep_the_first_nonzero_checkpoint() {
        let original = PlaybackCheckpoint {
            generation: 7,
            track_id: 9,
            position: Duration::from_secs(125),
        };
        let desired = DesiredPlayback::Track {
            track_id: 9,
            paused: true,
        };

        // Models both fatal-first/probe-second and probe-first/fatal-second:
        // after the first teardown there is no replacement Player position to
        // sample, but the original point must survive unchanged.
        assert_eq!(
            checkpoint_after_audio_loss(
                Some(original),
                PlaybackPhase::WaitingForAudio,
                desired,
                7,
                Some(Duration::ZERO),
            ),
            Some(original)
        );
        assert_eq!(
            checkpoint_after_audio_loss(
                Some(original),
                PlaybackPhase::WaitingForAudio,
                desired,
                7,
                None,
            ),
            Some(original)
        );
    }

    #[test]
    fn only_active_playback_creates_a_new_audio_checkpoint() {
        let desired = DesiredPlayback::Track {
            track_id: 3,
            paused: false,
        };
        for phase in [
            PlaybackPhase::Idle,
            PlaybackPhase::WaitingForAudio,
            PlaybackPhase::Loading,
        ] {
            assert_eq!(
                checkpoint_after_audio_loss(None, phase, desired, 5, Some(Duration::from_secs(8)),),
                None
            );
        }
    }

    #[test]
    fn restored_source_requires_matching_generation_checkpoint_and_sink() {
        let position = Duration::from_secs(32);
        let checkpoint = Some(PlaybackCheckpoint {
            generation: 4,
            track_id: 8,
            position,
        });
        let desired = DesiredPlayback::Track {
            track_id: 8,
            paused: true,
        };

        assert!(decoded_source_is_current(
            4,
            8,
            Some(22),
            position,
            4,
            PlaybackPhase::WaitingForAudio,
            desired,
            checkpoint,
            Some(22),
        ));
        assert!(!decoded_source_is_current(
            4,
            8,
            Some(21),
            position,
            4,
            PlaybackPhase::WaitingForAudio,
            desired,
            checkpoint,
            Some(22),
        ));
        assert!(!decoded_source_is_current(
            3,
            8,
            Some(22),
            position,
            4,
            PlaybackPhase::WaitingForAudio,
            desired,
            checkpoint,
            Some(22),
        ));
        assert!(!decoded_source_is_current(
            4,
            8,
            Some(22),
            Duration::ZERO,
            4,
            PlaybackPhase::WaitingForAudio,
            desired,
            checkpoint,
            Some(22),
        ));

        // The normal first decode remains valid only in Loading and has no
        // physical-sink target or resume offset.
        assert!(decoded_source_is_current(
            4,
            8,
            None,
            Duration::ZERO,
            4,
            PlaybackPhase::Loading,
            desired,
            None,
            Some(22),
        ));
    }

    #[test]
    fn only_current_open_attempt_can_be_installed() {
        let state = AudioState::Opening {
            attempt_id: 12,
            retry_count: 3,
        };

        assert_eq!(open_attempt_retry_count(&state, 11), None);
        assert_eq!(open_attempt_retry_count(&state, 12), Some(3));
        assert_eq!(open_attempt_retry_count(&state, 13), None);

        let unavailable = AudioState::Unavailable {
            retry_at: Instant::now(),
            retry_count: 3,
        };
        assert_eq!(open_attempt_retry_count(&unavailable, 12), None);
    }

    #[test]
    fn rapid_default_device_changes_only_accept_the_final_match() {
        let a = "device-a";
        let b = "device-b";
        let c = "device-c";

        assert_eq!(
            classify_audio_candidate_state(true, Some(&a), Ok(Some(&b))),
            AudioCandidate::DefaultChanged
        );
        assert_eq!(
            classify_audio_candidate_state(true, Some(&b), Ok(Some(&c))),
            AudioCandidate::DefaultChanged
        );
        assert_eq!(
            classify_audio_candidate_state(true, Some(&c), Ok(Some(&c))),
            AudioCandidate::Accept
        );
    }

    #[test]
    fn candidate_health_and_probe_results_have_distinct_meanings() {
        let device = "device-a";

        // A callback can fire before OpenFinished is handled. The shared
        // health flag must veto that otherwise matching candidate.
        assert_eq!(
            classify_audio_candidate_state(false, Some(&device), Ok(Some(&device))),
            AudioCandidate::Failed
        );
        assert_eq!(
            classify_audio_candidate_state(true, Some(&device), Ok(None)),
            AudioCandidate::NoDefault
        );
        // One identity-query failure is not evidence that a healthy sink died.
        assert_eq!(
            classify_audio_candidate_state(true, Some(&device), Err(())),
            AudioCandidate::Accept
        );
        // If the opened endpoint's ID itself was unavailable, retain the
        // successfully opened sink and rely on later best-effort probes.
        assert_eq!(
            classify_audio_candidate_state(true, None, Ok(Some(&device))),
            AudioCandidate::Accept
        );
    }

    #[test]
    fn stale_probe_id_or_sink_epoch_cannot_change_audio_state() {
        let in_flight = Some((8, Some(21)));

        assert_eq!(
            classify_probe_event(in_flight, 8, Some(21), Some(21)),
            ProbeEventDisposition::Apply
        );
        // An older response must not clear the marker for probe 8.
        assert_eq!(
            classify_probe_event(in_flight, 7, Some(21), Some(21)),
            ProbeEventDisposition::Ignore
        );
        assert_eq!(
            classify_probe_event(in_flight, 8, Some(20), Some(21)),
            ProbeEventDisposition::Ignore
        );
        // Probe 8 is the expected response, but sink 21 was replaced while
        // the query ran. Consume it without applying it to sink 22.
        assert_eq!(
            classify_probe_event(in_flight, 8, Some(21), Some(22)),
            ProbeEventDisposition::ConsumeStale
        );
        assert_eq!(
            classify_probe_event(None, 8, Some(21), Some(21)),
            ProbeEventDisposition::Ignore
        );
    }

    #[test]
    fn queue_advances_only_for_a_healthy_natural_completion() {
        let desired = DesiredPlayback::Track {
            track_id: 42,
            paused: false,
        };

        assert!(should_advance_queue(
            PlaybackPhase::Active,
            desired,
            Some(42),
            true,
            true,
        ));
        // Fatal callback and natural completion becoming visible together
        // must be treated as device failure, not end-of-track.
        assert!(!should_advance_queue(
            PlaybackPhase::Active,
            desired,
            Some(42),
            false,
            true,
        ));
        assert!(!should_advance_queue(
            PlaybackPhase::WaitingForAudio,
            desired,
            Some(42),
            true,
            true,
        ));
        assert!(!should_advance_queue(
            PlaybackPhase::Active,
            desired,
            Some(99),
            true,
            true,
        ));
        assert!(!should_advance_queue(
            PlaybackPhase::Active,
            DesiredPlayback::Stopped,
            Some(42),
            true,
            true,
        ));
    }
}
