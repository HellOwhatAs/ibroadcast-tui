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

    pub fn enqueue(&mut self, track_id: u64) -> usize {
        self.tracks.push(track_id);
        let index = self.tracks.len() - 1;
        if self.current.is_none() {
            self.current = Some(0);
        }
        index
    }

    pub fn enqueue_many(&mut self, track_ids: impl IntoIterator<Item = u64>) -> usize {
        let start_len = self.tracks.len();
        self.tracks.extend(track_ids);
        if self.current.is_none() && !self.tracks.is_empty() {
            self.current = Some(0);
        }
        self.tracks.len().saturating_sub(start_len)
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
        assert_eq!(queue.enqueue(10), 0);
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
}
