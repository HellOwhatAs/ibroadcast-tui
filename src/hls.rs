//! Minimal HLS client for iBroadcast's transcoded bitrates.
//!
//! The streaming server only exposes 96/192/256/320 kbps as HLS playlists
//! (`/hls_<rate>/...`) whose MPEG-TS segments carry ADTS AAC audio. This
//! module fetches those playlists, extracts the AAC elementary stream from
//! the segments, and feeds it into a [`ProgressiveBuffer`] so the existing
//! progressive decoder can treat it like a plain `.aac` download.

use std::time::Duration;

use reqwest::{Client, Url};

use crate::{
    error::{AppError, Result},
    progressive::ProgressiveBuffer,
};

/// The transcoder publishes segments incrementally (`EXT-X-PLAYLIST-TYPE:
/// EVENT`); poll this often while waiting for new ones.
const PLAYLIST_POLL_INTERVAL: Duration = Duration::from_millis(750);
/// Give up when this many consecutive polls yield no new segments.
const MAX_STALLED_POLLS: u32 = 240;

/// Streams an HLS playlist URL into the buffer as a raw ADTS AAC stream.
pub async fn stream_hls_to_buffer(
    http: &Client,
    url: &str,
    target_bandwidth: u64,
    buffer: ProgressiveBuffer,
) -> Result<()> {
    let mut playlist_url =
        Url::parse(url).map_err(|err| AppError::Playback(format!("invalid HLS url: {err}")))?;
    let mut playlist = fetch_playlist(http, &playlist_url).await?;

    // The server sometimes answers with a master playlist listing the
    // available renditions; pick the one closest to the requested bitrate.
    if let Playlist::Master(variants) = playlist {
        let variant = select_variant(variants, target_bandwidth)
            .ok_or_else(|| AppError::Playback("HLS master playlist has no variants".to_owned()))?;
        playlist_url = variant.url;
        playlist = fetch_playlist(http, &playlist_url).await?;
    }

    let Playlist::Media(mut media) = playlist else {
        return Err(AppError::Playback(
            "HLS master playlist nested another master playlist".to_owned(),
        ));
    };

    let mut demuxer = TsDemuxer::default();
    let mut audio = Vec::new();
    let mut next_segment = 0;
    let mut stalled_polls = 0u32;
    let mut total_bytes = 0usize;

    loop {
        while let Some(segment_url) = media.segments.get(next_segment) {
            let bytes = http
                .get(segment_url.clone())
                .send()
                .await?
                .error_for_status()?
                .bytes()
                .await?;
            audio.clear();
            demuxer.extract_audio(&bytes, &mut audio);
            total_bytes += audio.len();
            if !audio.is_empty() {
                buffer.push(&audio);
            }
            next_segment += 1;
            stalled_polls = 0;
        }

        if media.ended {
            break;
        }

        // Transcoding is still in progress; wait for the playlist to grow.
        tokio::time::sleep(PLAYLIST_POLL_INTERVAL).await;
        match fetch_playlist(http, &playlist_url).await? {
            Playlist::Media(refreshed) => {
                if refreshed.segments.len() <= next_segment && !refreshed.ended {
                    stalled_polls += 1;
                    if stalled_polls >= MAX_STALLED_POLLS {
                        return Err(AppError::Playback(
                            "HLS stream stalled: server stopped producing segments".to_owned(),
                        ));
                    }
                }
                media = refreshed;
            }
            Playlist::Master(_) => {
                return Err(AppError::Playback(
                    "HLS media playlist turned into a master playlist".to_owned(),
                ));
            }
        }
    }

    if total_bytes == 0 {
        return Err(AppError::Playback(
            "HLS stream contained no audio data".to_owned(),
        ));
    }
    buffer.finish();
    Ok(())
}

async fn fetch_playlist(http: &Client, url: &Url) -> Result<Playlist> {
    let text = http
        .get(url.clone())
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    parse_playlist(&text, url)
}

#[derive(Debug)]
enum Playlist {
    Master(Vec<Variant>),
    Media(MediaPlaylist),
}

#[derive(Debug)]
struct Variant {
    bandwidth: u64,
    url: Url,
}

#[derive(Debug)]
struct MediaPlaylist {
    segments: Vec<Url>,
    ended: bool,
}

fn parse_playlist(text: &str, base: &Url) -> Result<Playlist> {
    if !text.trim_start().starts_with("#EXTM3U") {
        return Err(AppError::Playback(
            "server response is not an HLS playlist".to_owned(),
        ));
    }

    let mut variants = Vec::new();
    let mut segments = Vec::new();
    let mut ended = false;
    let mut pending_bandwidth = None;

    for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
        if let Some(attributes) = line.strip_prefix("#EXT-X-STREAM-INF:") {
            pending_bandwidth = Some(parse_bandwidth(attributes).unwrap_or(0));
        } else if line == "#EXT-X-ENDLIST" {
            ended = true;
        } else if line.starts_with('#') {
            continue;
        } else {
            let url = base
                .join(line)
                .map_err(|err| AppError::Playback(format!("invalid HLS entry {line}: {err}")))?;
            match pending_bandwidth.take() {
                Some(bandwidth) => variants.push(Variant { bandwidth, url }),
                None => segments.push(url),
            }
        }
    }

    if variants.is_empty() {
        Ok(Playlist::Media(MediaPlaylist { segments, ended }))
    } else {
        Ok(Playlist::Master(variants))
    }
}

fn parse_bandwidth(attributes: &str) -> Option<u64> {
    attributes.split(',').find_map(|attribute| {
        attribute
            .trim()
            .strip_prefix("BANDWIDTH=")
            .and_then(|value| value.parse().ok())
    })
}

/// Picks the variant whose advertised bandwidth is closest to the target.
fn select_variant(variants: Vec<Variant>, target_bandwidth: u64) -> Option<Variant> {
    variants
        .into_iter()
        .min_by_key(|variant| variant.bandwidth.abs_diff(target_bandwidth))
}

const TS_PACKET_LEN: usize = 188;
const TS_SYNC_BYTE: u8 = 0x47;
/// ISO 13818-7 ADTS AAC.
const STREAM_TYPE_ADTS_AAC: u8 = 0x0F;
/// MPEG-1/2 audio (MP3-family); accepted as a fallback since the progressive
/// decoder sniffs the actual codec from the byte stream anyway.
const STREAM_TYPE_MPEG1_AUDIO: u8 = 0x03;
const STREAM_TYPE_MPEG2_AUDIO: u8 = 0x04;

/// Extracts the audio elementary stream from MPEG-TS segments. Stateful so
/// the program tables found in one segment carry over to the next.
#[derive(Debug, Default)]
struct TsDemuxer {
    pmt_pid: Option<u16>,
    audio_pid: Option<u16>,
}

impl TsDemuxer {
    /// Appends the audio ES bytes found in `ts` to `out`. Malformed packets
    /// are skipped rather than treated as fatal.
    fn extract_audio(&mut self, ts: &[u8], out: &mut Vec<u8>) {
        let mut offset = 0;
        while offset + TS_PACKET_LEN <= ts.len() {
            if ts[offset] != TS_SYNC_BYTE {
                offset += 1; // resync
                continue;
            }
            self.handle_packet(&ts[offset..offset + TS_PACKET_LEN], out);
            offset += TS_PACKET_LEN;
        }
    }

    fn handle_packet(&mut self, packet: &[u8], out: &mut Vec<u8>) {
        let transport_error = packet[1] & 0x80 != 0;
        if transport_error {
            return;
        }
        let payload_unit_start = packet[1] & 0x40 != 0;
        let pid = u16::from(packet[1] & 0x1F) << 8 | u16::from(packet[2]);
        let adaptation_control = (packet[3] >> 4) & 0b11;
        if adaptation_control == 0b00 || adaptation_control == 0b10 {
            return; // reserved / no payload
        }
        let mut payload_start = 4;
        if adaptation_control == 0b11 {
            payload_start += 1 + packet[4] as usize;
        }
        let Some(payload) = packet.get(payload_start..) else {
            return;
        };
        if payload.is_empty() {
            return;
        }

        if pid == 0 {
            if payload_unit_start && let Some(pmt_pid) = parse_pat(payload) {
                self.pmt_pid = Some(pmt_pid);
            }
        } else if Some(pid) == self.pmt_pid {
            if payload_unit_start && let Some(audio_pid) = parse_pmt(payload) {
                self.audio_pid = Some(audio_pid);
            }
        } else if Some(pid) == self.audio_pid {
            if payload_unit_start {
                out.extend_from_slice(pes_payload(payload));
            } else {
                out.extend_from_slice(payload);
            }
        }
    }
}

/// Returns the PMT PID of the first program in a PAT section.
fn parse_pat(payload: &[u8]) -> Option<u16> {
    let section = psi_section(payload, 0x00)?;
    // Program loop: 4-byte entries between the 8-byte header and the CRC.
    for entry in section.get(8..)?.chunks_exact(4) {
        let program_number = u16::from(entry[0]) << 8 | u16::from(entry[1]);
        if program_number != 0 {
            return Some(u16::from(entry[2] & 0x1F) << 8 | u16::from(entry[3]));
        }
    }
    None
}

/// Returns the PID of the first supported audio stream in a PMT section.
fn parse_pmt(payload: &[u8]) -> Option<u16> {
    let section = psi_section(payload, 0x02)?;
    let program_info_length =
        usize::from(section.get(10)? & 0x0F) << 8 | usize::from(*section.get(11)?);
    let mut index = 12 + program_info_length;
    let mut fallback = None;
    while let (Some(&stream_type), Some(es_info_length)) = (
        section.get(index),
        section
            .get(index + 3)
            .zip(section.get(index + 4))
            .map(|(high, low)| usize::from(high & 0x0F) << 8 | usize::from(*low)),
    ) {
        let elementary_pid =
            u16::from(section[index + 1] & 0x1F) << 8 | u16::from(section[index + 2]);
        match stream_type {
            STREAM_TYPE_ADTS_AAC => return Some(elementary_pid),
            STREAM_TYPE_MPEG1_AUDIO | STREAM_TYPE_MPEG2_AUDIO => {
                fallback.get_or_insert(elementary_pid);
            }
            _ => {}
        }
        index += 5 + es_info_length;
    }
    fallback
}

/// Validates a PSI table header and returns the section (starting at
/// `table_id`), trimmed to `section_length` minus the CRC.
fn psi_section(payload: &[u8], table_id: u8) -> Option<&[u8]> {
    let pointer = usize::from(*payload.first()?);
    let section = payload.get(1 + pointer..)?;
    if *section.first()? != table_id {
        return None;
    }
    let section_length = usize::from(section.get(1)? & 0x0F) << 8 | usize::from(*section.get(2)?);
    let end = (3 + section_length).checked_sub(4)?; // exclude CRC32
    section.get(..end)
}

/// Strips the PES header from a payload-unit-start packet, returning the
/// elementary stream bytes (empty when the header is malformed).
fn pes_payload(payload: &[u8]) -> &[u8] {
    if payload.len() < 9 || payload[..3] != [0x00, 0x00, 0x01] {
        return &[];
    }
    let header_length = usize::from(payload[8]);
    payload.get(9 + header_length..).unwrap_or(&[])
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use reqwest::Url;

    use super::{Playlist, TsDemuxer, parse_playlist, select_variant};

    fn base() -> Url {
        Url::parse("https://streaming.ibroadcast.com/hls_96/d0c/6f4/21127414?a=b").unwrap()
    }

    #[test]
    fn parses_master_playlists_and_selects_by_bandwidth() {
        let text = "#EXTM3U\n\
            #EXT-X-VERSION:3\n\
            #EXT-X-STREAM-INF:PROGRAM-ID=1,BANDWIDTH=100000\n\
            https://ib064.ibroadcast.com/hls/abc/96/index.m3u8\n\
            #EXT-X-STREAM-INF:PROGRAM-ID=1,BANDWIDTH=200000\n\
            https://ib064.ibroadcast.com/hls/abc/128/index.m3u8\n\
            #EXT-X-IBROADCAST-SAMPLECOUNT:6554624\n";
        let Playlist::Master(variants) = parse_playlist(text, &base()).unwrap() else {
            panic!("expected master playlist");
        };
        assert_eq!(variants.len(), 2);

        let variant = select_variant(variants, 96_000).unwrap();
        assert_eq!(variant.url.path(), "/hls/abc/96/index.m3u8");
    }

    #[test]
    fn parses_media_playlists_with_relative_segments() {
        let text = "#EXTM3U\n\
            #EXT-X-PLAYLIST-TYPE: EVENT\n\
            #EXTINF:10.008,\n\
            0.ts\n\
            #EXTINF:4.5,\n\
            https://ib064.ibroadcast.com/hls/abc/96/1.ts\n\
            #EXT-X-ENDLIST\n";
        let Playlist::Media(media) = parse_playlist(text, &base()).unwrap() else {
            panic!("expected media playlist");
        };
        assert!(media.ended);
        assert_eq!(media.segments.len(), 2);
        assert_eq!(
            media.segments[0].as_str(),
            "https://streaming.ibroadcast.com/hls_96/d0c/6f4/0.ts"
        );
    }

    #[test]
    fn unfinished_playlists_are_not_ended() {
        let text = "#EXTM3U\n#EXTINF:10.0,\n0.ts\n";
        let Playlist::Media(media) = parse_playlist(text, &base()).unwrap() else {
            panic!("expected media playlist");
        };
        assert!(!media.ended);
        assert_eq!(media.segments.len(), 1);
    }

    #[test]
    fn rejects_non_playlist_responses() {
        assert!(parse_playlist("<html>nope</html>", &base()).is_err());
    }

    fn ts_packet(pid: u16, payload_unit_start: bool, payload: &[u8]) -> Vec<u8> {
        let mut packet = vec![
            0x47,
            ((payload_unit_start as u8) << 6) | (pid >> 8) as u8,
            (pid & 0xFF) as u8,
            0x10, // payload only, continuity 0
        ];
        packet.extend_from_slice(payload);
        packet.resize(188, 0xFF);
        packet
    }

    /// Builds a PSI payload: pointer field + section with a fake CRC.
    fn psi(table_id: u8, body: &[u8]) -> Vec<u8> {
        let section_length = (body.len() + 4) as u16; // body + CRC32
        let mut payload = vec![0x00, table_id, 0xB0 | (section_length >> 8) as u8];
        payload.push((section_length & 0xFF) as u8);
        payload.extend_from_slice(body);
        payload.extend_from_slice(&[0, 0, 0, 0]); // CRC placeholder
        payload
    }

    #[test]
    fn demuxes_adts_audio_from_transport_stream() {
        // PAT: program 1 -> PMT PID 0x1001.
        let pat_body = [0x00, 0x01, 0xC1, 0x00, 0x00, 0x00, 0x01, 0xF0, 0x01];
        // PMT: H.264 on 0x0100, ADTS AAC on 0x0101.
        let pmt_body = [
            0x00, 0x01, 0xC1, 0x00, 0x00, // header remainder
            0xE1, 0x00, // PCR PID
            0xF0, 0x00, // program info length 0
            0x1B, 0xE1, 0x00, 0xF0, 0x00, // H.264 stream, PID 0x0100
            0x0F, 0xE1, 0x01, 0xF0, 0x00, // AAC stream, PID 0x0101
        ];

        // PES with a 0-length optional header carrying "adts" bytes.
        let audio = b"\xFF\xF1\x50\x80fake adts frame";
        let mut pes = vec![0x00, 0x00, 0x01, 0xC0, 0x00, 0x00, 0x80, 0x00, 0x00];
        pes.extend_from_slice(audio);

        let mut ts = Vec::new();
        ts.extend(ts_packet(0, true, &psi(0x00, &pat_body)));
        ts.extend(ts_packet(0x1001, true, &psi(0x02, &pmt_body)));
        ts.extend(ts_packet(0x0101, true, &pes));
        // Continuation packet: raw ES bytes, no PES header.
        ts.extend(ts_packet(0x0101, false, b"more audio"));
        // Unrelated PID is ignored.
        ts.extend(ts_packet(0x0100, true, b"\x00\x00\x01\xE0video"));

        let mut demuxer = TsDemuxer::default();
        let mut out = Vec::new();
        demuxer.extract_audio(&ts, &mut out);

        // Each packet payload is 184 bytes; the PES header consumes 9 of the
        // first one. The 0xFF fill comes from the fixed packet size in this
        // synthetic stream; real packets are fully used.
        assert_eq!(out.len(), (184 - 9) + 184);
        assert!(out.starts_with(audio));
        assert!(out[184 - 9..].starts_with(b"more audio"));
    }

    #[test]
    fn demuxer_survives_garbage_input() {
        let mut demuxer = TsDemuxer::default();
        let mut out = Vec::new();
        demuxer.extract_audio(&[0x47; 400], &mut out);
        demuxer.extract_audio(b"not a transport stream", &mut out);
        assert!(out.is_empty());
    }
}
