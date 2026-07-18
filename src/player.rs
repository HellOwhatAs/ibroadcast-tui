use std::{
    fmt,
    fs::File,
    io::BufReader,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use rodio::{
    Decoder, DeviceSinkBuilder, MixerDeviceSink, Player,
    cpal::{
        self, DeviceId, DeviceIdError,
        traits::{DeviceTrait, HostTrait},
    },
};

use crate::{
    error::{AppError, Result},
    progressive::ProgressiveReader,
};

/// A decoded progressive stream, ready to hand to [`AudioOutput::play_source`].
///
/// Building this can block until enough of the stream has arrived to probe the
/// container format, so it should happen on a blocking-capable thread, not in
/// the UI event loop.
pub type StreamSource = Decoder<BufReader<ProgressiveReader>>;

/// The epoch assigned by the application to one physical output stream.
///
/// Every stream event carries this value so that the application can ignore
/// callbacks from an output that has already been replaced.
pub type SinkEpoch = u64;

/// A normalized error reported by the physical output stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AudioStreamError {
    /// The device backing the stream is no longer available.
    DeviceNotAvailable,
    /// The stream configuration became invalid and the stream must be rebuilt.
    StreamInvalidated,
    /// The backend reported an underrun or overrun. This is a transient glitch.
    BufferUnderrun,
    /// A backend-specific failure occurred.
    BackendSpecific(String),
}

impl AudioStreamError {
    /// Whether this error invalidates the current output stream.
    pub const fn is_fatal(&self) -> bool {
        !matches!(self, Self::BufferUnderrun)
    }
}

impl fmt::Display for AudioStreamError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeviceNotAvailable => formatter.write_str("audio device is no longer available"),
            Self::StreamInvalidated => formatter.write_str("audio stream was invalidated"),
            Self::BufferUnderrun => formatter.write_str("audio buffer underrun or overrun"),
            Self::BackendSpecific(message) => write!(formatter, "audio backend error: {message}"),
        }
    }
}

impl From<cpal::StreamError> for AudioStreamError {
    fn from(error: cpal::StreamError) -> Self {
        match error {
            cpal::StreamError::DeviceNotAvailable => Self::DeviceNotAvailable,
            cpal::StreamError::StreamInvalidated => Self::StreamInvalidated,
            cpal::StreamError::BufferUnderrun => Self::BufferUnderrun,
            cpal::StreamError::BackendSpecific { err } => Self::BackendSpecific(err.description),
        }
    }
}

/// An event emitted by an open audio stream's error callback.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AudioStreamEvent {
    pub sink_epoch: SinkEpoch,
    pub error: AudioStreamError,
}

/// Queries the identity of the current system default output device.
///
/// `Ok(None)` means that the host currently has no default output device.
/// `Err(_)` means a device existed, but its identity could not be queried. A
/// caller polling this function should not treat one such error as proof that
/// an otherwise healthy stream must be destroyed.
pub fn default_output_device_id() -> std::result::Result<Option<DeviceId>, DeviceIdError> {
    match cpal::default_host().default_output_device() {
        Some(device) => device.id().map(Some),
        None => Ok(None),
    }
}

#[derive(Clone, Debug, Default)]
struct StreamHealth {
    failed: Arc<AtomicBool>,
}

impl StreamHealth {
    fn is_healthy(&self) -> bool {
        !self.failed.load(Ordering::Acquire)
    }

    /// Returns true only for the first transition to the failed state.
    fn mark_failed(&self) -> bool {
        !self.failed.swap(true, Ordering::AcqRel)
    }
}

fn handle_stream_error<F>(
    sink_epoch: SinkEpoch,
    health: &StreamHealth,
    notify: &F,
    error: cpal::StreamError,
) where
    F: Fn(AudioStreamEvent),
{
    let error = AudioStreamError::from(error);

    // Mark a fatal stream failure before notifying the application. This
    // prevents the UI tick that follows event handling from mistaking a dead
    // stream for a naturally completed track. Repeated fatal callbacks from
    // the same sink are collapsed into one event.
    if error.is_fatal() && !health.mark_failed() {
        return;
    }

    notify(AudioStreamEvent { sink_epoch, error });
}

pub fn decode_progressive_stream(
    reader: ProgressiveReader,
    mime_type: &str,
    extension_hint: &str,
) -> Result<StreamSource> {
    Decoder::builder()
        .with_data(BufReader::new(reader))
        .with_hint(extension_hint)
        .with_mime_type(mime_type)
        .with_seekable(false)
        .build()
        .map_err(|err| AppError::Playback(err.to_string()))
}

/// Thin wrapper around the rodio output device and the currently playing
/// track. One `Player` is created per track so that end-of-track can be
/// detected through [`AudioOutput::is_finished`].
pub struct AudioOutput {
    stream: MixerDeviceSink,
    player: Option<Player>,
    volume: f32,
    sink_epoch: SinkEpoch,
    device_id: Option<DeviceId>,
    health: StreamHealth,
}

impl AudioOutput {
    /// Opens the current system default output device.
    ///
    /// Configuration fallback is restricted to the selected default device;
    /// this function never silently switches to another physical output. The
    /// error callback only updates the shared health flag and calls `notify`.
    /// Stream destruction and reopening must be performed by the application,
    /// outside the callback thread.
    pub fn open_default<F>(sink_epoch: SinkEpoch, notify: F) -> Result<Self>
    where
        F: Fn(AudioStreamEvent) + Send + Sync + 'static,
    {
        let device = cpal::default_host()
            .default_output_device()
            .ok_or_else(|| AppError::Playback("no default audio output device available".into()))?;
        // Failure to obtain an ID must not prevent playback. `None` here means
        // that this selected device's identity is unknown; absence of a
        // default device was already handled above.
        let device_id = device.id().ok();
        let health = StreamHealth::default();
        let callback_health = health.clone();
        let notify = Arc::new(notify);
        let error_callback = move |error| {
            handle_stream_error(sink_epoch, &callback_health, notify.as_ref(), error);
        };

        let mut stream = DeviceSinkBuilder::from_device(device)
            .and_then(|builder| {
                builder
                    .with_error_callback(error_callback)
                    .open_sink_or_fallback()
            })
            .map_err(|error| AppError::Playback(error.to_string()))?;
        // Reopening is an expected lifecycle operation. Suppress rodio's
        // unconditional drop message; runtime failures are already handled by
        // the custom callback above and therefore never use its stderr path.
        stream.log_on_drop(false);

        Ok(Self {
            stream,
            player: None,
            volume: 0.8,
            sink_epoch,
            device_id,
            health,
        })
    }

    /// The epoch supplied when this output was opened.
    pub const fn sink_epoch(&self) -> SinkEpoch {
        self.sink_epoch
    }

    /// The identity of the physical device selected when the stream opened.
    ///
    /// `None` means that this already selected device's identity could not be
    /// queried; it does not mean that no default device existed.
    pub fn device_id(&self) -> Option<&DeviceId> {
        self.device_id.as_ref()
    }

    /// Whether the stream has avoided a known fatal callback.
    ///
    /// `true` is not proof that the operating system stream is physically
    /// alive; it only means that no fatal error has been observed yet.
    pub fn is_healthy(&self) -> bool {
        self.health.is_healthy()
    }

    fn ensure_healthy(&self) -> Result<()> {
        if self.is_healthy() {
            Ok(())
        } else {
            Err(AppError::Playback(
                "audio output stream has failed and must be reopened".into(),
            ))
        }
    }

    /// Starts an already decoded stream if this output has no known failure.
    ///
    /// Success only confirms that the source was attached while the shared
    /// failure flag was clear. Rodio cannot synchronously prove that the
    /// physical stream remains alive.
    pub fn play_source(&mut self, source: StreamSource) -> Result<()> {
        self.ensure_healthy()?;
        self.stop();
        self.ensure_healthy()?;
        let player = Player::connect_new(self.stream.mixer());
        player.set_volume(self.volume);
        player.append(source);
        self.ensure_healthy()?;
        self.player = Some(player);
        Ok(())
    }

    pub fn play_file(&mut self, path: &Path, mime_type: &str, extension_hint: &str) -> Result<()> {
        self.ensure_healthy()?;
        let file = File::open(path)?;
        let content_len = file.metadata().ok().map(|metadata| metadata.len());

        let mut builder = Decoder::builder()
            .with_data(BufReader::new(file))
            .with_hint(extension_hint)
            .with_mime_type(mime_type)
            .with_seekable(true);
        if let Some(content_len) = content_len {
            builder = builder.with_byte_len(content_len);
        }
        let source = builder
            .build()
            .map_err(|err| AppError::Playback(err.to_string()))?;
        self.ensure_healthy()?;
        self.stop();
        self.ensure_healthy()?;
        let player = Player::connect_new(self.stream.mixer());
        player.set_volume(self.volume);
        player.append(source);
        self.ensure_healthy()?;
        self.player = Some(player);
        Ok(())
    }

    /// Idempotently applies an explicit pause state to the loaded track.
    ///
    /// Repeating the same recovery effect cannot accidentally reverse the
    /// desired state.
    pub fn set_paused(&mut self, paused: bool) -> Option<()> {
        let player = self.player.as_ref()?;
        if player.is_paused() != paused {
            if paused {
                player.pause();
            } else {
                player.play();
            }
        }
        Some(())
    }

    /// True when a track was loaded and has played to completion.
    pub fn is_finished(&self) -> bool {
        self.is_healthy() && self.player.as_ref().is_some_and(Player::empty) && self.is_healthy()
    }

    pub fn position(&self) -> Option<Duration> {
        self.player.as_ref().map(Player::get_pos)
    }

    pub fn stop(&mut self) {
        if let Some(player) = self.player.take() {
            player.stop();
        }
    }

    pub fn set_volume(&mut self, volume: f32) {
        self.volume = volume.clamp(0.0, 1.0);
        if let Some(player) = self.player.as_ref() {
            player.set_volume(self.volume);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use rodio::cpal::{BackendSpecificError, StreamError};

    use super::*;

    #[test]
    fn audio_output_can_cross_a_blocking_worker_boundary() {
        fn assert_send<T: Send>() {}

        assert_send::<AudioOutput>();
    }

    #[test]
    fn stream_error_classification_matches_recovery_policy() {
        let cases = [
            (
                StreamError::DeviceNotAvailable,
                AudioStreamError::DeviceNotAvailable,
                true,
            ),
            (
                StreamError::StreamInvalidated,
                AudioStreamError::StreamInvalidated,
                true,
            ),
            (
                StreamError::BufferUnderrun,
                AudioStreamError::BufferUnderrun,
                false,
            ),
            (
                StreamError::BackendSpecific {
                    err: BackendSpecificError {
                        description: "test backend failure".into(),
                    },
                },
                AudioStreamError::BackendSpecific("test backend failure".into()),
                true,
            ),
        ];

        for (input, expected, fatal) in cases {
            let actual = AudioStreamError::from(input);
            assert_eq!(actual, expected);
            assert_eq!(actual.is_fatal(), fatal);
        }
    }

    #[test]
    fn fatal_error_marks_failed_before_notifying_and_is_deduplicated() {
        let health = StreamHealth::default();
        let events = Mutex::new(Vec::new());
        let notify = |event| {
            assert!(!health.is_healthy());
            events.lock().unwrap().push(event);
        };

        handle_stream_error(17, &health, &notify, StreamError::DeviceNotAvailable);
        handle_stream_error(17, &health, &notify, StreamError::StreamInvalidated);

        assert!(!health.is_healthy());
        assert_eq!(
            *events.lock().unwrap(),
            vec![AudioStreamEvent {
                sink_epoch: 17,
                error: AudioStreamError::DeviceNotAvailable,
            }]
        );
    }

    #[test]
    fn buffer_underrun_is_non_fatal_and_remains_observable() {
        let health = StreamHealth::default();
        let events = Mutex::new(Vec::new());
        let notify = |event| events.lock().unwrap().push(event);

        handle_stream_error(9, &health, &notify, StreamError::BufferUnderrun);
        handle_stream_error(9, &health, &notify, StreamError::BufferUnderrun);

        assert!(health.is_healthy());
        assert_eq!(
            *events.lock().unwrap(),
            vec![
                AudioStreamEvent {
                    sink_epoch: 9,
                    error: AudioStreamError::BufferUnderrun,
                },
                AudioStreamEvent {
                    sink_epoch: 9,
                    error: AudioStreamError::BufferUnderrun,
                },
            ]
        );
    }
}
