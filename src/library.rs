use std::{cmp::Ordering, collections::BTreeMap};

use serde_json::{Map, Value};

use crate::error::{AppError, Result};

/// The subset of the iBroadcast library that this client consumes.
///
/// iBroadcast returns collections in a compressed form: each entity is an
/// array of values plus a `map` object that names each array index. Decoding
/// only extracts the fields modeled here; everything else is dropped.
#[derive(Clone, Debug, Default)]
pub struct Library {
    pub tracks: BTreeMap<u64, Track>,
    pub albums: BTreeMap<u64, Album>,
    pub artists: BTreeMap<u64, Artist>,
}

#[derive(Clone, Debug, Default)]
pub struct Track {
    pub id: u64,
    pub track: i64,
    pub title: String,
    pub genre: String,
    pub length: i64,
    pub album_id: u64,
    pub artist_id: u64,
    pub trashed: bool,
    pub path: String,
    pub file: String,
    pub mime_type: String,
}

/// Albums and artists are keyed by id in [`Library`]; only display data is
/// stored on the values themselves.
#[derive(Clone, Debug, Default)]
pub struct Album {
    pub name: String,
}

#[derive(Clone, Debug, Default)]
pub struct Artist {
    pub name: String,
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
            .map(|(id, fields)| (id, Album::from_fields(&fields)))
            .collect();
        let artists = decode_collection(root.get("artists"))
            .into_iter()
            .map(|(id, fields)| (id, Artist::from_fields(&fields)))
            .collect();

        Ok(Self {
            tracks,
            albums,
            artists,
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
            title: get_string(fields, "title", "Untitled"),
            genre: get_string(fields, "genre", ""),
            length: get_i64(fields, "length"),
            album_id: get_u64(fields, "album_id"),
            artist_id: get_u64(fields, "artist_id"),
            trashed: get_bool(fields, "trashed"),
            path: get_string(fields, "path", ""),
            file: get_string(fields, "file", ""),
            mime_type: get_string(fields, "type", ""),
        }
    }

    pub fn duration_label(&self) -> String {
        let minutes = self.length.max(0) / 60;
        let seconds = self.length.max(0) % 60;
        format!("{minutes}:{seconds:02}")
    }
}

impl Album {
    fn from_fields(fields: &Map<String, Value>) -> Self {
        Self {
            name: get_string(fields, "name", "Unknown Album"),
        }
    }
}

impl Artist {
    fn from_fields(fields: &Map<String, Value>) -> Self {
        Self {
            name: get_string(fields, "name", "Unknown Artist"),
        }
    }
}

/// Decodes an iBroadcast compressed collection: `{ "<id>": [values...], "map": {field: index} }`.
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

pub fn value_to_u64(value: &Value) -> Option<u64> {
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
            }
        });

        let library = Library::from_value(&raw).unwrap();
        assert_eq!(library.tracks[&211504300].title, "Pop Up");
        assert_eq!(library.tracks[&211504300].file, "/128/74c/fdd/10026629");
        assert_eq!(library.albums[&78903103].name, "Digital Guilt");
        assert_eq!(library.artist_name(22917502), "Zoe.LeelA");
    }

    #[test]
    fn search_matches_artist_and_title() {
        let raw = json!({
            "tracks": {
                "1": ["Song A", 5, false, 10],
                "2": ["Other", 5, false, 10],
                "map": {"title": 0, "artist_id": 1, "trashed": 2, "album_id": 3}
            },
            "artists": {
                "5": ["The Band"],
                "map": {"name": 0}
            },
            "albums": {}
        });
        let library = Library::from_value(&raw).unwrap();
        assert_eq!(library.search_track_ids("song"), vec![1]);
        assert_eq!(library.search_track_ids("band").len(), 2);
        assert_eq!(library.search_track_ids(""), vec![2, 1]); // "Other" < "Song A"
    }
}
