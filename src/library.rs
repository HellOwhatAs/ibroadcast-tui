use std::{cmp::Ordering, collections::BTreeMap};

use serde_json::{Map, Value};

use crate::error::{AppError, Result};

#[derive(Clone, Debug, Default)]
#[allow(dead_code)]
pub struct Library {
    pub tracks: BTreeMap<u64, Track>,
    pub albums: BTreeMap<u64, Album>,
    pub artists: BTreeMap<u64, Artist>,
    pub playlists: BTreeMap<u64, Playlist>,
    pub tags: BTreeMap<u64, Tag>,
    pub status: Option<Value>,
    pub expires: Option<i64>,
}

#[derive(Clone, Debug, Default)]
#[allow(dead_code)]
pub struct Track {
    pub id: u64,
    pub track: i64,
    pub year: i64,
    pub title: String,
    pub genre: String,
    pub length: i64,
    pub album_id: u64,
    pub artwork_id: u64,
    pub artist_id: u64,
    pub uploaded_on: String,
    pub trashed: bool,
    pub size: u64,
    pub path: String,
    pub rating: i64,
    pub plays: i64,
    pub file: String,
    pub mime_type: String,
    pub replay_gain: String,
}

#[derive(Clone, Debug, Default)]
#[allow(dead_code)]
pub struct Album {
    pub id: u64,
    pub name: String,
    pub tracks: Vec<u64>,
    pub artist_id: u64,
    pub trashed: bool,
    pub rating: i64,
    pub year: i64,
    pub disc: i64,
}

#[derive(Clone, Debug, Default)]
#[allow(dead_code)]
pub struct Artist {
    pub id: u64,
    pub name: String,
    pub tracks: Vec<u64>,
    pub trashed: bool,
    pub rating: i64,
}

#[derive(Clone, Debug, Default)]
#[allow(dead_code)]
pub struct Playlist {
    pub id: u64,
    pub name: String,
    pub tracks: Vec<u64>,
    pub description: String,
    pub system_created: bool,
}

#[derive(Clone, Debug, Default)]
#[allow(dead_code)]
pub struct Tag {
    pub id: u64,
    pub name: String,
    pub archived: bool,
    pub tracks: Vec<u64>,
}

impl Library {
    pub fn from_value(value: &Value) -> Result<Self> {
        let root = value
            .as_object()
            .ok_or_else(|| AppError::Library("library root is not an object".to_owned()))?;

        let tracks = decode_collection(root.get("tracks"))
            .into_iter()
            .map(|(id, fields)| (id, Track::from_fields(id, &fields)))
            .collect();
        let albums = decode_collection(root.get("albums"))
            .into_iter()
            .map(|(id, fields)| (id, Album::from_fields(id, &fields)))
            .collect();
        let artists = decode_collection(root.get("artists"))
            .into_iter()
            .map(|(id, fields)| (id, Artist::from_fields(id, &fields)))
            .collect();
        let playlists = decode_collection(root.get("playlists"))
            .into_iter()
            .map(|(id, fields)| (id, Playlist::from_fields(id, &fields)))
            .collect();
        let tags = decode_tags(root.get("tags"))
            .into_iter()
            .map(|(id, fields)| (id, Tag::from_fields(id, &fields)))
            .collect();

        Ok(Self {
            tracks,
            albums,
            artists,
            playlists,
            tags,
            status: root.get("status").cloned(),
            expires: root.get("expires").and_then(value_to_i64),
        })
    }

    pub fn sorted_track_ids(&self) -> Vec<u64> {
        let mut ids: Vec<_> = self
            .tracks
            .values()
            .filter(|track| !track.trashed)
            .map(|track| track.id)
            .collect();
        ids.sort_by(|left, right| self.compare_tracks(*left, *right));
        ids
    }

    pub fn search_track_ids(&self, query: &str) -> Vec<u64> {
        let query = query.trim().to_lowercase();
        if query.is_empty() {
            return self.sorted_track_ids();
        }

        let mut ids: Vec<_> = self
            .tracks
            .values()
            .filter(|track| !track.trashed)
            .filter(|track| {
                let haystack = format!(
                    "{} {} {} {} {}",
                    track.title,
                    track.genre,
                    track.path,
                    self.artist_name(track.artist_id),
                    self.album_name(track.album_id)
                )
                .to_lowercase();
                haystack.contains(&query)
            })
            .map(|track| track.id)
            .collect();
        ids.sort_by(|left, right| self.compare_tracks(*left, *right));
        ids
    }

    pub fn artist_name(&self, artist_id: u64) -> &str {
        self.artists
            .get(&artist_id)
            .map(|artist| artist.name.as_str())
            .unwrap_or("Unknown Artist")
    }

    pub fn album_name(&self, album_id: u64) -> &str {
        self.albums
            .get(&album_id)
            .map(|album| album.name.as_str())
            .unwrap_or("Unknown Album")
    }

    pub fn track_label(&self, track_id: u64) -> String {
        self.tracks.get(&track_id).map_or_else(
            || format!("Track {track_id}"),
            |track| format!("{} - {}", self.artist_name(track.artist_id), track.title),
        )
    }

    fn compare_tracks(&self, left: u64, right: u64) -> Ordering {
        let Some(left) = self.tracks.get(&left) else {
            return Ordering::Greater;
        };
        let Some(right) = self.tracks.get(&right) else {
            return Ordering::Less;
        };
        (
            self.artist_name(left.artist_id),
            self.album_name(left.album_id),
            left.track,
            left.title.as_str(),
        )
            .cmp(&(
                self.artist_name(right.artist_id),
                self.album_name(right.album_id),
                right.track,
                right.title.as_str(),
            ))
    }
}

impl Track {
    fn from_fields(id: u64, fields: &Map<String, Value>) -> Self {
        Self {
            id,
            track: get_i64(fields, "track"),
            year: get_i64(fields, "year"),
            title: get_string(fields, "title", "Untitled"),
            genre: get_string(fields, "genre", ""),
            length: get_i64(fields, "length"),
            album_id: get_u64(fields, "album_id"),
            artwork_id: get_u64(fields, "artwork_id"),
            artist_id: get_u64(fields, "artist_id"),
            uploaded_on: get_string(fields, "uploaded_on", ""),
            trashed: get_bool(fields, "trashed"),
            size: get_u64(fields, "size"),
            path: get_string(fields, "path", ""),
            rating: get_i64(fields, "rating"),
            plays: get_i64(fields, "plays"),
            file: get_string(fields, "file", ""),
            mime_type: get_string(fields, "type", ""),
            replay_gain: get_string(fields, "replay_gain", ""),
        }
    }

    pub fn duration_label(&self) -> String {
        let minutes = self.length.max(0) / 60;
        let seconds = self.length.max(0) % 60;
        format!("{minutes}:{seconds:02}")
    }
}

impl Album {
    fn from_fields(id: u64, fields: &Map<String, Value>) -> Self {
        Self {
            id,
            name: get_string(fields, "name", "Unknown Album"),
            tracks: get_vec_u64(fields, "tracks"),
            artist_id: get_u64(fields, "artist_id"),
            trashed: get_bool(fields, "trashed"),
            rating: get_i64(fields, "rating"),
            year: get_i64(fields, "year"),
            disc: get_i64(fields, "disc"),
        }
    }
}

impl Artist {
    fn from_fields(id: u64, fields: &Map<String, Value>) -> Self {
        Self {
            id,
            name: get_string(fields, "name", "Unknown Artist"),
            tracks: get_vec_u64(fields, "tracks"),
            trashed: get_bool(fields, "trashed"),
            rating: get_i64(fields, "rating"),
        }
    }
}

impl Playlist {
    fn from_fields(id: u64, fields: &Map<String, Value>) -> Self {
        Self {
            id,
            name: get_string(fields, "name", "Untitled Playlist"),
            tracks: get_vec_u64(fields, "tracks"),
            description: get_string(fields, "description", ""),
            system_created: get_bool(fields, "system_created"),
        }
    }
}

impl Tag {
    fn from_fields(id: u64, fields: &Map<String, Value>) -> Self {
        Self {
            id,
            name: get_string(fields, "name", "Untitled Tag"),
            archived: get_bool(fields, "archived"),
            tracks: get_vec_u64(fields, "tracks"),
        }
    }
}

fn decode_collection(value: Option<&Value>) -> BTreeMap<u64, Map<String, Value>> {
    let Some(object) = value.and_then(Value::as_object) else {
        return BTreeMap::new();
    };
    let keymap = object
        .get("map")
        .and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .filter_map(|(key, index)| value_to_u64(index).map(|index| (index as usize, key)))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();

    object
        .iter()
        .filter_map(|(id, raw)| {
            let id = id.parse::<u64>().ok()?;
            let array = raw.as_array()?;
            let mut fields = Map::new();
            for (index, key) in &keymap {
                if let Some(value) = array.get(*index) {
                    fields.insert((*key).clone(), value.clone());
                }
            }
            Some((id, fields))
        })
        .collect()
}

fn decode_tags(value: Option<&Value>) -> BTreeMap<u64, Map<String, Value>> {
    let Some(object) = value.and_then(Value::as_object) else {
        return BTreeMap::new();
    };

    if object.get("map").is_some() {
        return decode_collection(value);
    }

    object
        .iter()
        .filter_map(|(id, raw)| Some((id.parse::<u64>().ok()?, raw.as_object()?.clone())))
        .collect()
}

fn get_string(fields: &Map<String, Value>, key: &str, default: &str) -> String {
    fields
        .get(key)
        .and_then(|value| match value {
            Value::String(value) => Some(value.clone()),
            Value::Number(value) => Some(value.to_string()),
            _ => None,
        })
        .unwrap_or_else(|| default.to_owned())
}

fn get_u64(fields: &Map<String, Value>, key: &str) -> u64 {
    fields.get(key).and_then(value_to_u64).unwrap_or_default()
}

fn get_i64(fields: &Map<String, Value>, key: &str) -> i64 {
    fields.get(key).and_then(value_to_i64).unwrap_or_default()
}

fn get_bool(fields: &Map<String, Value>, key: &str) -> bool {
    fields.get(key).and_then(Value::as_bool).unwrap_or_default()
}

fn get_vec_u64(fields: &Map<String, Value>, key: &str) -> Vec<u64> {
    fields
        .get(key)
        .and_then(Value::as_array)
        .map(|values| values.iter().filter_map(value_to_u64).collect())
        .unwrap_or_default()
}

fn value_to_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn value_to_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_u64().map(|value| value as i64)),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::Library;

    #[test]
    fn decodes_mapped_library_collections() {
        let raw = json!({
            "tracks": {
                "211504300": [
                    1, 2013, "Pop Up", "Pop", 105, 78903103, 160155,
                    22917502, 0, "2021-02-17", false, 2573544, "",
                    "", 0, 0, "/128/74c/fdd/10026629", "audio/mpeg3",
                    "-7.9", "18:42:39"
                ],
                "map": {
                    "length": 4, "track": 0, "title": 2, "album_id": 5,
                    "artist_id": 7, "file": 16, "type": 17, "trashed": 10
                }
            },
            "albums": {
                "78903103": ["Digital Guilt", [211504300], 22917502, false, 0, 0, 2013],
                "map": {"name": 0, "tracks": 1, "artist_id": 2, "trashed": 3}
            },
            "artists": {
                "22917502": ["Zoe.LeelA", [211504300], false, 0],
                "map": {"name": 0, "tracks": 1, "trashed": 2, "rating": 3}
            },
            "playlists": {},
            "tags": {
                "455976": {"name": "Test Tag", "archived": false, "tracks": [211504300]}
            }
        });

        let library = Library::from_value(&raw).unwrap();
        assert_eq!(library.tracks[&211504300].title, "Pop Up");
        assert_eq!(library.albums[&78903103].tracks, vec![211504300]);
        assert_eq!(library.artist_name(22917502), "Zoe.LeelA");
        assert_eq!(library.tags[&455976].name, "Test Tag");
    }
}
