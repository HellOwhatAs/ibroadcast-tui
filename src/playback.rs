use std::{
    fs::File,
    io::{BufReader, Read, Seek},
    path::Path,
};

use rodio::{Decoder, DeviceSinkBuilder, MixerDeviceSink, Player};

use crate::error::{AppError, Result};

pub struct PlaybackController {
    stream: MixerDeviceSink,
    player: Option<Player>,
    volume: f32,
}

impl PlaybackController {
    pub fn new() -> Result<Self> {
        let stream = DeviceSinkBuilder::open_default_sink()
            .map_err(|err| AppError::Playback(err.to_string()))?;
        Ok(Self {
            stream,
            player: None,
            volume: 0.8,
        })
    }

    pub fn play_stream<R>(
        &mut self,
        reader: R,
        content_len: Option<u64>,
        mime_type: &str,
        extension_hint: &str,
    ) -> Result<()>
    where
        R: Read + Seek + Send + Sync + 'static,
    {
        self.play_reader(reader, content_len, mime_type, extension_hint, false)
    }

    pub fn play_file(&mut self, path: &Path, mime_type: &str, extension_hint: &str) -> Result<()> {
        let file = File::open(path)?;
        let content_len = file.metadata().ok().map(|metadata| metadata.len());
        self.play_reader(file, content_len, mime_type, extension_hint, true)
    }

    fn play_reader<R>(
        &mut self,
        reader: R,
        content_len: Option<u64>,
        mime_type: &str,
        extension_hint: &str,
        seekable: bool,
    ) -> Result<()>
    where
        R: Read + Seek + Send + Sync + 'static,
    {
        self.stop();
        let mut builder = Decoder::builder()
            .with_data(BufReader::new(reader))
            .with_hint(extension_hint)
            .with_mime_type(mime_type)
            .with_seekable(seekable);
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

    pub fn toggle_pause(&mut self) -> bool {
        let Some(player) = self.player.as_ref() else {
            return false;
        };
        if player.is_paused() {
            player.play();
            false
        } else {
            player.pause();
            true
        }
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
