use std::{
    fmt,
    fs::File,
    io::{self, BufReader, Read, Seek, SeekFrom},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use rodio::{
    ChannelCount, Decoder, DeviceSinkBuilder, MixerDeviceSink, Player, SampleRate, Source,
    cpal::{
        self, DeviceId, DeviceIdError,
        traits::{DeviceTrait, HostTrait},
    },
    source::SeekError,
};

use crate::{
    error::{AppError, Result},
    progressive::ProgressiveReader,
};

/// A decoded source, ready to hand to [`AudioOutput::play_source`].
///
/// Building or fast-forwarding this can block, so it should happen on a
/// blocking-capable thread, not in the UI event loop.
pub type StreamSource = Box<dyn Source + Send>;

/// The epoch assigned by the application to one physical output stream.
///
/// Every stream event carries this value so that the application can ignore
/// callbacks from an output that has already been replaced.
pub type SinkEpoch = u64;

const DECODE_CANCELLED_MESSAGE: &str = "audio decode cancelled";

/// Cooperatively cancels local-file decoder preparation.
///
/// Clones share one flag so the application can retain a handle while a
/// blocking decoder build owns another. Cancellation is observed by both file
/// I/O and decoded-sample fast-forwarding.
#[derive(Clone, Debug, Default)]
pub(crate) struct DecodeCancellation {
    cancelled: Arc<AtomicBool>,
}

impl DecodeCancellation {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn ensure_active(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(decode_cancelled_error())
        } else {
            Ok(())
        }
    }
}

fn decode_cancelled_error() -> AppError {
    AppError::Playback(DECODE_CANCELLED_MESSAGE.to_owned())
}

fn decode_cancelled_io_error() -> io::Error {
    // `Interrupted` permits transparent retries in `Read` consumers, which
    // would defeat cancellation inside a decoder probe or seek.
    io::Error::new(io::ErrorKind::BrokenPipe, DECODE_CANCELLED_MESSAGE)
}

/// Reader used by cancellable decoder preparation.
///
/// Checking both operations matters because decoder probing and seeking can
/// spend their time in either path depending on the container and codec.
struct CancellableReader<R> {
    inner: R,
    cancellation: DecodeCancellation,
}

impl<R> CancellableReader<R> {
    fn new(inner: R, cancellation: DecodeCancellation) -> Self {
        Self {
            inner,
            cancellation,
        }
    }

    fn ensure_active(&self) -> io::Result<()> {
        if self.cancellation.is_cancelled() {
            Err(decode_cancelled_io_error())
        } else {
            Ok(())
        }
    }
}

impl<R: Read> Read for CancellableReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.ensure_active()?;
        self.inner.read(buffer)
    }
}

impl<R: Seek> Seek for CancellableReader<R> {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.ensure_active()?;
        self.inner.seek(position)
    }
}

/// Source wrapper that observes cancellation before every decoded sample.
///
/// A decoder may buffer compressed input, so the cancellable reader alone
/// cannot interrupt rodio's eager `skip_duration` loop promptly.
struct CancellableSource<S> {
    inner: S,
    cancellation: DecodeCancellation,
}

impl<S> CancellableSource<S> {
    fn new(inner: S, cancellation: DecodeCancellation) -> Self {
        Self {
            inner,
            cancellation,
        }
    }

    fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: Source> Iterator for CancellableSource<S> {
    type Item = S::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cancellation.is_cancelled() {
            None
        } else {
            self.inner.next()
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.cancellation.is_cancelled() {
            (0, Some(0))
        } else {
            self.inner.size_hint()
        }
    }
}

impl<S: Source> Source for CancellableSource<S> {
    fn current_span_len(&self) -> Option<usize> {
        if self.cancellation.is_cancelled() {
            Some(0)
        } else {
            self.inner.current_span_len()
        }
    }

    fn channels(&self) -> ChannelCount {
        self.inner.channels()
    }

    fn sample_rate(&self) -> SampleRate {
        self.inner.sample_rate()
    }

    fn total_duration(&self) -> Option<Duration> {
        self.inner.total_duration()
    }

    fn try_seek(&mut self, position: Duration) -> std::result::Result<(), SeekError> {
        if self.cancellation.is_cancelled() {
            Err(SeekError::Other(Arc::new(decode_cancelled_io_error())))
        } else {
            self.inner.try_seek(position)
        }
    }
}

fn skip_duration_cancellable<S>(
    source: S,
    start_at: Duration,
    cancellation: DecodeCancellation,
) -> Result<S>
where
    S: Source,
{
    let source = CancellableSource::new(source, cancellation.clone()).skip_duration(start_at);
    cancellation.ensure_active()?;
    Ok(source.into_inner().into_inner())
}

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
    buffer_underrun: Arc<AtomicBool>,
}

impl StreamHealth {
    fn is_healthy(&self) -> bool {
        !self.failed.load(Ordering::Acquire)
    }

    /// Returns true only for the first transition to the failed state.
    fn mark_failed(&self) -> bool {
        !self.failed.swap(true, Ordering::AcqRel)
    }

    fn mark_buffer_underrun(&self) {
        self.buffer_underrun.store(true, Ordering::Relaxed);
    }

    fn take_buffer_underrun(&self) -> bool {
        self.buffer_underrun.swap(false, Ordering::Relaxed)
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

    // Underruns can arrive on every backend xrun. Keep them observable without
    // allocating one unbounded-channel node per callback; the application
    // periodically consumes this per-sink flag and rate-limits its warning.
    if !error.is_fatal() {
        health.mark_buffer_underrun();
        return;
    }

    // Mark a fatal stream failure before notifying the application. This
    // prevents the UI tick that follows event handling from mistaking a dead
    // stream for a naturally completed track. Repeated fatal callbacks from
    // the same sink are collapsed into one event.
    if !health.mark_failed() {
        return;
    }

    notify(AudioStreamEvent { sink_epoch, error });
}

pub fn decode_progressive_stream(
    reader: ProgressiveReader,
    mime_type: &str,
    extension_hint: &str,
    start_at: Duration,
) -> Result<StreamSource> {
    let build = |reader| {
        Decoder::builder()
            .with_data(BufReader::new(reader))
            .with_hint(extension_hint)
            .with_mime_type(mime_type)
            .with_seekable(false)
            .build()
            .map_err(|err| AppError::Playback(err.to_string()))
    };
    let fallback_reader = (!start_at.is_zero()).then(|| reader.clone());
    let mut source = build(reader)?;

    if start_at.is_zero() {
        return Ok(Box::new(source));
    }
    if source.try_seek(start_at).is_ok() {
        return Ok(Box::new(source));
    }

    // ProgressiveBuffer retains every byte already received. If the format
    // cannot perform a forward seek (notably some raw streams), rebuild after
    // the failed attempt and decode-discard the prefix entirely from memory.
    let fallback_reader = fallback_reader.expect("nonzero start has a fallback reader");
    Ok(Box::new(build(fallback_reader)?.skip_duration(start_at)))
}

/// Reopens a local file at `start_at`, falling back to decoded fast-forwarding
/// for formats whose demuxer cannot seek directly.
pub fn decode_file_from(
    path: &Path,
    mime_type: &str,
    extension_hint: &str,
    start_at: Duration,
) -> Result<StreamSource> {
    let build = || {
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
        builder
            .build()
            .map_err(|err| AppError::Playback(err.to_string()))
    };

    let mut source = build()?;
    if start_at.is_zero() || source.try_seek(start_at).is_ok() {
        return Ok(Box::new(source));
    }

    // Some codecs expose only forward decoding even for a seekable file.
    // Rebuild after the failed seek so a partially-mutated decoder is never
    // reused, then fast-forward on the blocking preparation thread.
    Ok(Box::new(build()?.skip_duration(start_at)))
}

/// Reopens a local file at `start_at`, cooperatively aborting decoder work.
///
/// The reader interrupts decoder probing and seeking. When seeking is not
/// supported, the source wrapper additionally checks before every sample that
/// rodio eagerly decodes and discards. Every cancellation path returns before
/// the decoder leaves this call, so its file is released on the blocking
/// preparation thread.
pub(crate) fn decode_file_from_cancellable(
    path: &Path,
    mime_type: &str,
    extension_hint: &str,
    start_at: Duration,
    cancellation: DecodeCancellation,
) -> Result<StreamSource> {
    let build = || {
        cancellation.ensure_active()?;
        let file = File::open(path)?;
        let content_len = file.metadata().ok().map(|metadata| metadata.len());
        cancellation.ensure_active()?;
        let reader = CancellableReader::new(file, cancellation.clone());
        let mut builder = Decoder::builder()
            .with_data(BufReader::new(reader))
            .with_hint(extension_hint)
            .with_mime_type(mime_type)
            .with_seekable(true);
        if let Some(content_len) = content_len {
            builder = builder.with_byte_len(content_len);
        }
        let source = builder.build();
        cancellation.ensure_active()?;
        source.map_err(|err| AppError::Playback(err.to_string()))
    };

    let mut source = build()?;
    if start_at.is_zero() {
        cancellation.ensure_active()?;
        return Ok(Box::new(source));
    }

    let seek_result = source.try_seek(start_at);
    cancellation.ensure_active()?;
    if seek_result.is_ok() {
        return Ok(Box::new(source));
    }

    // Rebuild after the failed seek so a partially-mutated decoder is never
    // reused. The inner wrapper checks cancellation even while the decoder is
    // serving samples from an already-buffered packet.
    drop(source);
    let source = skip_duration_cancellable(build()?, start_at, cancellation)?;
    Ok(Box::new(source))
}

/// Thin wrapper around the rodio output device and the currently playing
/// track. One `Player` is created per track so that end-of-track can be
/// detected through [`AudioOutput::is_finished`].
pub struct AudioOutput {
    stream: MixerDeviceSink,
    output_device: Box<cpal::Device>,
    player: Option<Player>,
    /// Position within the track at which the currently attached source
    /// begins. Rebuilt progressive sources report their own position from
    /// zero, so this keeps the public position continuous across devices.
    position_base: Duration,
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

        let output_device = device.clone();
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
            output_device: Box::new(output_device),
            player: None,
            position_base: Duration::ZERO,
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

    /// Returns another handle to the exact device used to open this output.
    pub(crate) fn output_device(&self) -> cpal::Device {
        self.output_device.as_ref().clone()
    }

    /// Stores a subsequently recovered identity for this same output device.
    pub(crate) fn remember_device_id(&mut self, device_id: DeviceId) {
        self.device_id = Some(device_id);
    }

    /// Whether the stream has avoided a known fatal callback.
    ///
    /// `true` is not proof that the operating system stream is physically
    /// alive; it only means that no fatal error has been observed yet.
    pub fn is_healthy(&self) -> bool {
        self.health.is_healthy()
    }

    /// Takes the coalesced non-fatal underrun indication for this sink.
    pub(crate) fn take_buffer_underrun(&self) -> bool {
        self.health.take_buffer_underrun()
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
    pub fn play_source(
        &mut self,
        source: StreamSource,
        position_base: Duration,
        paused: bool,
    ) -> Result<()> {
        self.ensure_healthy()?;
        self.stop();
        self.ensure_healthy()?;
        let player = Player::connect_new(self.stream.mixer());
        player.set_volume(self.volume);
        // Attach while paused so recovery can never leak samples before the
        // caller's explicit pause state has been applied.
        player.pause();
        player.append(source);
        if !paused {
            player.play();
        }
        self.ensure_healthy()?;
        self.position_base = position_base;
        self.player = Some(player);
        Ok(())
    }

    pub fn play_file(
        &mut self,
        path: &Path,
        mime_type: &str,
        extension_hint: &str,
        paused: bool,
    ) -> Result<()> {
        self.ensure_healthy()?;
        let source = decode_file_from(path, mime_type, extension_hint, Duration::ZERO)?;
        self.play_source(source, Duration::ZERO, paused)
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
        self.player.as_ref().map(|player| {
            self.position_base
                .checked_add(player.get_pos())
                .unwrap_or(Duration::MAX)
        })
    }

    pub fn stop(&mut self) {
        if let Some(player) = self.player.take() {
            player.stop();
        }
        self.position_base = Duration::ZERO;
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
    use std::{
        io::Cursor,
        sync::{Mutex, atomic::AtomicUsize},
    };

    use rodio::cpal::{BackendSpecificError, StreamError};

    use crate::progressive::ProgressiveBuffer;

    use super::*;

    fn mono_pcm_wav(samples: &[i16], sample_rate: u32) -> Vec<u8> {
        let data_len = std::mem::size_of_val(samples) as u32;
        let mut wav = Vec::with_capacity(44 + data_len as usize);
        wav.extend_from_slice(b"RIFF");
        wav.extend_from_slice(&(36 + data_len).to_le_bytes());
        wav.extend_from_slice(b"WAVEfmt ");
        wav.extend_from_slice(&16_u32.to_le_bytes());
        wav.extend_from_slice(&1_u16.to_le_bytes());
        wav.extend_from_slice(&1_u16.to_le_bytes());
        wav.extend_from_slice(&sample_rate.to_le_bytes());
        wav.extend_from_slice(&(sample_rate * 2).to_le_bytes());
        wav.extend_from_slice(&2_u16.to_le_bytes());
        wav.extend_from_slice(&16_u16.to_le_bytes());
        wav.extend_from_slice(b"data");
        wav.extend_from_slice(&data_len.to_le_bytes());
        for sample in samples {
            wav.extend_from_slice(&sample.to_le_bytes());
        }
        wav
    }

    fn assert_decode_cancelled<T>(result: Result<T>) {
        match result {
            Err(AppError::Playback(message)) => assert_eq!(message, DECODE_CANCELLED_MESSAGE),
            Err(error) => panic!("unexpected cancellation error: {error}"),
            Ok(_) => panic!("cancelled decode unexpectedly succeeded"),
        }
    }

    struct CancelAfterSamples {
        cancellation: DecodeCancellation,
        cancel_after: usize,
        samples_read: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
    }

    impl Iterator for CancelAfterSamples {
        type Item = rodio::Sample;

        fn next(&mut self) -> Option<Self::Item> {
            let samples_read = self.samples_read.fetch_add(1, Ordering::Relaxed) + 1;
            if samples_read >= self.cancel_after {
                self.cancellation.cancel();
            }
            Some(0.0)
        }
    }

    impl Source for CancelAfterSamples {
        fn current_span_len(&self) -> Option<usize> {
            None
        }

        fn channels(&self) -> ChannelCount {
            ChannelCount::new(1).expect("nonzero channel count")
        }

        fn sample_rate(&self) -> SampleRate {
            SampleRate::new(100).expect("nonzero sample rate")
        }

        fn total_duration(&self) -> Option<Duration> {
            None
        }
    }

    impl Drop for CancelAfterSamples {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }

    #[test]
    fn audio_output_can_cross_a_blocking_worker_boundary() {
        fn assert_send<T: Send>() {}

        assert_send::<AudioOutput>();
    }

    #[test]
    fn progressive_source_rebuild_fast_forwards_from_the_same_buffer() {
        let wav = mono_pcm_wav(&[0, 1000, 2000, 3000, 4000, 5000, 6000, 7000], 4);
        let buffer = ProgressiveBuffer::new(Some(wav.len() as u64));
        buffer.push(&wav);
        buffer.finish();

        let original =
            decode_progressive_stream(buffer.reader(), "audio/wav", "wav", Duration::ZERO).unwrap();
        assert_eq!(original.count(), 8);

        // A replacement reader sees the retained compressed bytes. Skipping
        // one second at four mono samples/second leaves the second half, with
        // no new push and therefore no replacement download.
        let restored =
            decode_progressive_stream(buffer.reader(), "audio/wav", "wav", Duration::from_secs(1))
                .unwrap();
        assert_eq!(restored.count(), 4);
    }

    #[test]
    fn cancellation_interrupts_reader_reads_and_seeks() {
        let cancellation = DecodeCancellation::new();
        let mut reader =
            CancellableReader::new(Cursor::new(vec![1_u8, 2, 3, 4]), cancellation.clone());
        let mut byte = [0_u8; 1];

        assert_eq!(Read::read(&mut reader, &mut byte).unwrap(), 1);
        assert_eq!(byte, [1]);

        cancellation.cancel();
        assert!(cancellation.is_cancelled());
        assert_eq!(
            Read::read(&mut reader, &mut byte).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            Seek::seek(&mut reader, SeekFrom::Start(0))
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[test]
    fn pre_cancelled_file_decode_returns_cancellation_before_opening() {
        let cancellation = DecodeCancellation::new();
        cancellation.cancel();

        assert_decode_cancelled(decode_file_from_cancellable(
            Path::new("this-file-must-not-be-opened"),
            "audio/wav",
            "wav",
            Duration::ZERO,
            cancellation,
        ));
    }

    #[test]
    fn eager_fast_forward_stops_per_sample_and_drops_its_source() {
        let cancellation = DecodeCancellation::new();
        let samples_read = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let source = CancelAfterSamples {
            cancellation: cancellation.clone(),
            cancel_after: 8,
            samples_read: Arc::clone(&samples_read),
            dropped: Arc::clone(&dropped),
        };

        assert_decode_cancelled(skip_duration_cancellable(
            source,
            Duration::from_secs(1),
            cancellation,
        ));
        assert_eq!(samples_read.load(Ordering::Relaxed), 8);
        assert!(dropped.load(Ordering::Acquire));
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

        handle_stream_error(17, &health, &notify, StreamError::BufferUnderrun);
        handle_stream_error(17, &health, &notify, StreamError::DeviceNotAvailable);
        handle_stream_error(17, &health, &notify, StreamError::StreamInvalidated);

        assert!(!health.is_healthy());
        assert!(health.take_buffer_underrun());
        assert_eq!(
            *events.lock().unwrap(),
            vec![AudioStreamEvent {
                sink_epoch: 17,
                error: AudioStreamError::DeviceNotAvailable,
            }]
        );
    }

    #[test]
    fn buffer_underruns_are_non_fatal_and_coalesced_without_notification() {
        let health = StreamHealth::default();
        let events = Mutex::new(Vec::new());
        let notify = |event| events.lock().unwrap().push(event);

        for _ in 0..10_000 {
            handle_stream_error(9, &health, &notify, StreamError::BufferUnderrun);
        }

        assert!(health.is_healthy());
        assert!(events.lock().unwrap().is_empty());
        assert!(health.take_buffer_underrun());
        assert!(!health.take_buffer_underrun());

        handle_stream_error(9, &health, &notify, StreamError::BufferUnderrun);
        assert!(health.take_buffer_underrun());
    }
}
