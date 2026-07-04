use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use futures_util::StreamExt;
use tokio::{fs, io::AsyncWriteExt};

use crate::{
    error::Result,
    library::{Library, Track},
    progressive::ProgressiveBuffer,
};

#[derive(Clone, Copy, Debug)]
pub enum DownloadStatus {
    Running,
    Finished,
    /// The failure message is surfaced through the status line when the
    /// download completes; only the terminal state is retained here.
    Failed,
}

#[derive(Clone, Debug)]
pub struct DownloadTask {
    pub id: u64,
    pub track_id: u64,
    pub status: DownloadStatus,
}

/// Tracks running downloads and which tracks exist on disk.
///
/// The local-file index is kept in memory and updated on the events that can
/// change it (library sync, download completion, deletion), so rendering never
/// has to touch the filesystem.
#[derive(Debug, Default)]
pub struct DownloadManager {
    tasks: Vec<DownloadTask>,
    local: HashSet<u64>,
    next_id: u64,
}

impl DownloadManager {
    /// Rebuilds the local-file index by scanning the download directory for
    /// every library track. Called once per library sync.
    pub fn rescan(&mut self, library: &Library, download_dir: &Path) {
        self.local = library
            .tracks
            .iter()
            .filter_map(|(track_id, track)| {
                build_download_path(download_dir, library, track)
                    .exists()
                    .then_some(*track_id)
            })
            .collect();
    }

    pub fn begin(&mut self, track_id: u64) -> u64 {
        self.next_id += 1;
        self.tasks.push(DownloadTask {
            id: self.next_id,
            track_id,
            status: DownloadStatus::Running,
        });
        self.next_id
    }

    /// Records the outcome of a task. Returns `false` when the task is
    /// unknown, e.g. a download that completed after a logout cleared the
    /// manager.
    pub fn complete(&mut self, task_id: u64, result: &Result<PathBuf>) -> bool {
        let Some(task) = self.tasks.iter_mut().find(|task| task.id == task_id) else {
            return false;
        };
        match result {
            Ok(_) => {
                self.local.insert(task.track_id);
                task.status = DownloadStatus::Finished;
            }
            Err(_) => task.status = DownloadStatus::Failed,
        }
        true
    }

    pub fn is_running(&self, track_id: u64) -> bool {
        self.tasks
            .iter()
            .any(|task| task.track_id == track_id && matches!(task.status, DownloadStatus::Running))
    }

    pub fn is_local(&self, track_id: u64) -> bool {
        self.local.contains(&track_id)
    }

    pub fn mark_not_local(&mut self, track_id: u64) {
        self.local.remove(&track_id);
    }

    pub fn running_count(&self) -> usize {
        self.tasks
            .iter()
            .filter(|task| matches!(task.status, DownloadStatus::Running))
            .count()
    }

    pub fn local_count(&self) -> usize {
        self.local.len()
    }

    pub fn storage_label(&self, track_id: u64) -> &'static str {
        if self.is_running(track_id) {
            "downloading"
        } else if self.is_local(track_id) {
            "local"
        } else if self
            .tasks
            .iter()
            .any(|task| task.track_id == track_id && matches!(task.status, DownloadStatus::Failed))
        {
            "failed"
        } else {
            ""
        }
    }

    pub fn clear(&mut self) {
        self.tasks.clear();
        self.local.clear();
    }
}

/// Downloads a URL to disk atomically: bytes stream into a `.part` file that
/// is renamed into place only when complete.
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

/// Streams a URL into an in-memory progressive buffer for diskless playback.
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

/// Makes a string safe to use as a single path component on every platform.
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

/// Removes now-empty artist/album directories after deleting a local file,
/// walking up from the file's parent but never past the download root.
pub fn remove_empty_download_dirs(file_path: &Path, root: &Path) {
    let Ok(root) = root.canonicalize() else {
        return;
    };
    let mut current = file_path
        .parent()
        .and_then(|path| path.canonicalize().ok());

    while let Some(dir) = current {
        if dir == root || !dir.starts_with(&root) {
            break;
        }
        if std::fs::remove_dir(&dir).is_err() {
            break;
        }
        current = dir.parent().map(Path::to_path_buf);
    }
}

fn part_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("download");
    path.with_file_name(format!("{file_name}.part"))
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use pretty_assertions::assert_eq;

    use crate::{
        error::AppError,
        library::{Album, Artist, Library, Track},
    };

    use super::{
        DownloadManager, build_download_path, extension_from_mime, sanitize_component,
    };

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
                name: "Artist/One".to_owned(),
            },
        );
        library.albums.insert(
            2,
            Album {
                name: "Album:Two".to_owned(),
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

    #[test]
    fn manager_tracks_task_and_local_state() {
        let mut manager = DownloadManager::default();
        assert_eq!(manager.storage_label(7), "");

        let task = manager.begin(7);
        assert!(manager.is_running(7));
        assert_eq!(manager.storage_label(7), "downloading");

        manager.complete(task, &Ok(PathBuf::from("x.mp3")));
        assert!(!manager.is_running(7));
        assert!(manager.is_local(7));
        assert_eq!(manager.storage_label(7), "local");
        assert_eq!(manager.local_count(), 1);

        manager.mark_not_local(7);
        assert!(!manager.is_local(7));

        let task = manager.begin(8);
        manager.complete(task, &Err(AppError::Api("boom".to_owned())));
        assert_eq!(manager.storage_label(8), "failed");
        assert_eq!(manager.running_count(), 0);
    }
}
