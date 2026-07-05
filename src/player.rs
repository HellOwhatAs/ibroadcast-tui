use std::{fs::File, io::BufReader, path::Path, time::Duration};

use rodio::{Decoder, DeviceSinkBuilder, MixerDeviceSink, Player};

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
}

impl AudioOutput {
    pub fn new() -> Result<Self> {
        let stream = DeviceSinkBuilder::open_default_sink()
            .map_err(|err| AppError::Playback(err.to_string()))?;
        Ok(Self {
            stream,
            player: None,
            volume: 0.8,
        })
    }

    pub fn play_source(&mut self, source: StreamSource) {
        self.stop();
        let player = Player::connect_new(self.stream.mixer());
        player.set_volume(self.volume);
        player.append(source);
        self.player = Some(player);
    }

    pub fn play_file(&mut self, path: &Path, mime_type: &str, extension_hint: &str) -> Result<()> {
        let file = File::open(path)?;
        let content_len = file.metadata().ok().map(|metadata| metadata.len());

        self.stop();
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
        let player = Player::connect_new(self.stream.mixer());
        player.set_volume(self.volume);
        player.append(source);
        self.player = Some(player);
        Ok(())
    }

    /// Returns the new paused state, or `None` when nothing is loaded.
    pub fn toggle_pause(&mut self) -> Option<bool> {
        let player = self.player.as_ref()?;
        if player.is_paused() {
            player.play();
            Some(false)
        } else {
            player.pause();
            Some(true)
        }
    }

    /// True when a track was loaded and has played to completion.
    pub fn is_finished(&self) -> bool {
        self.player.as_ref().is_some_and(Player::empty)
    }

    pub fn is_paused(&self) -> Option<bool> {
        self.player.as_ref().map(Player::is_paused)
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

    pub fn volume(&self) -> f32 {
        self.volume
    }
}
