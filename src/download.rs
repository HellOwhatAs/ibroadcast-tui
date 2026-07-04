use std::path::{Path, PathBuf};

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::{fs, io::AsyncWriteExt};

use crate::{
    error::{AppError, Result},
    library::{Library, Track},
    progressive::ProgressiveBuffer,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum DownloadStatus {
    Running,
    Finished(PathBuf),
    Failed(String),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DownloadTask {
    pub id: u64,
    pub track_id: u64,
    pub title: String,
    pub status: DownloadStatus,
}

pub async fn download_to_file(http: &reqwest::Client, url: &str, path: &Path) -> Result<()> {
    let response = http.get(url).send().await?.error_for_status()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let part_path = part_path(path);
    let mut file = fs::File::create(&part_path).await?;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        file.write_all(&chunk?).await?;
    }
    file.flush().await?;
    drop(file);
    fs::rename(&part_path, path).await.inspect_err(|_| {
        let _ = std::fs::remove_file(&part_path);
    })?;
    Ok(())
}

pub async fn stream_to_buffer(
    http: &reqwest::Client,
    url: &str,
    buffer: ProgressiveBuffer,
) -> Result<()> {
    let response = http.get(url).send().await?.error_for_status()?;
    buffer.set_content_len(response.content_length());
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        buffer.push(&chunk?);
    }
    buffer.finish();
    Ok(())
}

pub fn build_download_path(root: &Path, library: &Library, track: &Track) -> PathBuf {
    let artist = sanitize_component(library.artist_name(track.artist_id));
    let album = sanitize_component(library.album_name(track.album_id));
    let title = sanitize_component(&track.title);
    let extension = extension_from_mime(&track.mime_type);
    let filename = if track.track > 0 {
        format!("{:02} - {title}.{extension}", track.track)
    } else {
        format!("{title}.{extension}")
    };
    root.join(artist).join(album).join(filename)
}

pub fn extension_from_mime(mime: &str) -> &'static str {
    let mime = mime.to_ascii_lowercase();
    if mime.contains("flac") {
        "flac"
    } else if mime.contains("mp4") || mime.contains("m4a") || mime.contains("aac") {
        "m4a"
    } else if mime.contains("ogg") || mime.contains("opus") {
        "ogg"
    } else if mime.contains("wav") {
        "wav"
    } else if mime.contains("mpeg") || mime.contains("mp3") {
        "mp3"
    } else {
        "audio"
    }
}

pub fn sanitize_component(input: &str) -> String {
    let mut sanitized = input
        .chars()
        .map(|ch| {
            if ch.is_control() || matches!(ch, '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*')
            {
                '_'
            } else {
                ch
            }
        })
        .collect::<String>();

    sanitized = sanitized
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim_matches([' ', '.'])
        .to_owned();

    if sanitized.is_empty() {
        sanitized = "_".to_owned();
    }

    let uppercase = sanitized.to_ascii_uppercase();
    let reserved = ["CON", "PRN", "AUX", "NUL"];
    if reserved.contains(&uppercase.as_str())
        || (uppercase.len() == 4
            && (uppercase.starts_with("COM") || uppercase.starts_with("LPT"))
            && uppercase[3..].chars().all(|ch| ('1'..='9').contains(&ch)))
    {
        sanitized.push('_');
    }

    sanitized.chars().take(120).collect()
}

fn part_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("download");
    path.with_file_name(format!("{file_name}.part"))
}

impl AppError {
    pub fn download_path(path: PathBuf, source: AppError) -> Self {
        Self::Download {
            path,
            source: Box::new(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use pretty_assertions::assert_eq;

    use crate::library::{Album, Artist, Library, Track};

    use super::{build_download_path, extension_from_mime, sanitize_component};

    #[test]
    fn sanitizes_cross_platform_file_components() {
        assert_eq!(sanitize_component("A/B:C*D?"), "A_B_C_D_");
        assert_eq!(sanitize_component("CON"), "CON_");
        assert_eq!(sanitize_component("  ...  "), "_");
    }

    #[test]
    fn maps_audio_mime_types() {
        assert_eq!(extension_from_mime("audio/mpeg3"), "mp3");
        assert_eq!(extension_from_mime("audio/flac"), "flac");
        assert_eq!(extension_from_mime("audio/mp4"), "m4a");
    }

    #[test]
    fn builds_download_paths() {
        let mut library = Library::default();
        library.artists.insert(
            1,
            Artist {
                id: 1,
                name: "Artist/One".to_owned(),
                ..Artist::default()
            },
        );
        library.albums.insert(
            2,
            Album {
                id: 2,
                name: "Album:Two".to_owned(),
                ..Album::default()
            },
        );
        let track = Track {
            artist_id: 1,
            album_id: 2,
            track: 3,
            title: "Song*Three".to_owned(),
            mime_type: "audio/mpeg3".to_owned(),
            ..Track::default()
        };

        assert_eq!(
            build_download_path(Path::new("root"), &library, &track),
            Path::new("root")
                .join("Artist_One")
                .join("Album_Two")
                .join("03 - Song_Three.mp3")
        );
    }
}
