/// The playback queue. A track appears at most once: re-adding an already
/// queued track is a no-op that yields the existing entry's position.
#[derive(Clone, Debug, Default)]
pub struct PlaybackQueue {
    tracks: Vec<u64>,
    current: Option<usize>,
}

impl PlaybackQueue {
    pub fn tracks(&self) -> &[u64] {
        &self.tracks
    }

    pub fn current_index(&self) -> Option<usize> {
        self.current
    }

    pub fn play_index(&mut self, index: usize) -> Option<u64> {
        if index >= self.tracks.len() {
            return None;
        }
        self.current = Some(index);
        self.current_track()
    }

    /// Adds a track to the queue, or finds its existing entry. Returns the
    /// track's index and whether it was newly added.
    pub fn enqueue(&mut self, track_id: u64) -> (usize, bool) {
        if let Some(index) = self.tracks.iter().position(|&id| id == track_id) {
            return (index, false);
        }
        self.tracks.push(track_id);
        if self.current.is_none() {
            self.current = Some(0);
        }
        (self.tracks.len() - 1, true)
    }

    /// Adds every track that is not already queued; returns how many were
    /// newly added.
    pub fn enqueue_many(&mut self, track_ids: impl IntoIterator<Item = u64>) -> usize {
        track_ids
            .into_iter()
            .filter(|&track_id| self.enqueue(track_id).1)
            .count()
    }

    pub fn remove(&mut self, index: usize) -> Option<u64> {
        if index >= self.tracks.len() {
            return None;
        }

        let removed = self.tracks.remove(index);
        self.current = match self.current {
            None => None,
            Some(_) if self.tracks.is_empty() => None,
            Some(current) if index < current => Some(current - 1),
            Some(current) if index == current => Some(index.min(self.tracks.len() - 1)),
            Some(current) => Some(current),
        };
        Some(removed)
    }

    pub fn move_up(&mut self, index: usize) -> Option<usize> {
        if index == 0 || index >= self.tracks.len() {
            return None;
        }
        self.tracks.swap(index, index - 1);
        self.current = match self.current {
            Some(current) if current == index => Some(index - 1),
            Some(current) if current == index - 1 => Some(index),
            other => other,
        };
        Some(index - 1)
    }

    pub fn move_down(&mut self, index: usize) -> Option<usize> {
        if index + 1 >= self.tracks.len() {
            return None;
        }
        self.tracks.swap(index, index + 1);
        self.current = match self.current {
            Some(current) if current == index => Some(index + 1),
            Some(current) if current == index + 1 => Some(index),
            other => other,
        };
        Some(index + 1)
    }

    pub fn clear(&mut self) {
        self.tracks.clear();
        self.current = None;
    }

    pub fn len(&self) -> usize {
        self.tracks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tracks.is_empty()
    }

    pub fn current_track(&self) -> Option<u64> {
        self.current
            .and_then(|index| self.tracks.get(index).copied())
    }

    pub fn next(&mut self) -> Option<u64> {
        let next = self.current.map_or(0, |index| index.saturating_add(1));
        if next >= self.tracks.len() {
            return None;
        }
        self.current = Some(next);
        self.current_track()
    }

    pub fn previous(&mut self) -> Option<u64> {
        let previous = self.current?.saturating_sub(1);
        self.current = Some(previous);
        self.current_track()
    }
}

#[cfg(test)]
mod tests {
    use super::PlaybackQueue;

    #[test]
    fn queue_tracks_advance_and_stop_at_end() {
        let mut queue = PlaybackQueue::default();
        queue.enqueue_many([10, 20]);
        assert_eq!(queue.play_index(0), Some(10));
        assert_eq!(queue.next(), Some(20));
        assert_eq!(queue.next(), None);
        assert_eq!(queue.previous(), Some(10));
    }

    #[test]
    fn queue_can_enqueue_move_and_remove_tracks() {
        let mut queue = PlaybackQueue::default();
        assert_eq!(queue.enqueue(10), (0, true));
        assert_eq!(queue.enqueue_many([20, 30]), 2);
        assert_eq!(queue.tracks(), &[10, 20, 30]);
        assert_eq!(queue.current_index(), Some(0));

        assert_eq!(queue.move_down(0), Some(1));
        assert_eq!(queue.tracks(), &[20, 10, 30]);
        assert_eq!(queue.current_index(), Some(1));

        assert_eq!(queue.move_up(1), Some(0));
        assert_eq!(queue.tracks(), &[10, 20, 30]);
        assert_eq!(queue.current_index(), Some(0));

        assert_eq!(queue.remove(0), Some(10));
        assert_eq!(queue.tracks(), &[20, 30]);
        assert_eq!(queue.current_index(), Some(0));
    }

    #[test]
    fn removing_before_current_preserves_current_track() {
        let mut queue = PlaybackQueue::default();
        queue.enqueue_many([10, 20, 30]);
        queue.play_index(2);
        assert_eq!(queue.remove(0), Some(10));
        assert_eq!(queue.current_track(), Some(30));
        assert_eq!(queue.current_index(), Some(1));
    }

    #[test]
    fn enqueueing_a_queued_track_returns_its_existing_position() {
        let mut queue = PlaybackQueue::default();
        queue.enqueue_many([10, 20]);
        queue.play_index(1);

        assert_eq!(queue.enqueue(10), (0, false));
        assert_eq!(queue.tracks(), &[10, 20]);
        // Re-adding must not disturb what is currently playing.
        assert_eq!(queue.current_track(), Some(20));
    }

    #[test]
    fn enqueue_many_skips_duplicates() {
        let mut queue = PlaybackQueue::default();
        queue.enqueue(10);
        assert_eq!(queue.enqueue_many([10, 20, 20, 30]), 2);
        assert_eq!(queue.tracks(), &[10, 20, 30]);
    }
}
