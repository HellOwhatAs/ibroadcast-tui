use std::fmt;

use rand::Rng;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PlaybackMode {
    #[default]
    Sequential,
    RepeatOne,
    RepeatAll,
    Shuffle,
}

impl PlaybackMode {
    pub fn next(self) -> Self {
        match self {
            Self::Sequential => Self::RepeatOne,
            Self::RepeatOne => Self::RepeatAll,
            Self::RepeatAll => Self::Shuffle,
            Self::Shuffle => Self::Sequential,
        }
    }
}

impl fmt::Display for PlaybackMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Sequential => "Sequential",
            Self::RepeatOne => "Repeat one",
            Self::RepeatAll => "Repeat all",
            Self::Shuffle => "Shuffle",
        };
        formatter.write_str(label)
    }
}

/// The playback queue. A track appears at most once: re-adding an already
/// queued track is a no-op that yields the existing entry's position.
#[derive(Clone, Debug, Default)]
pub struct PlaybackQueue {
    tracks: Vec<u64>,
    current: Option<usize>,
    mode: PlaybackMode,
    shuffle_remaining: Vec<usize>,
    shuffle_history: Vec<usize>,
}

impl PlaybackQueue {
    pub fn tracks(&self) -> &[u64] {
        &self.tracks
    }

    pub fn playback_mode(&self) -> PlaybackMode {
        self.mode
    }

    pub fn cycle_playback_mode(&mut self) -> PlaybackMode {
        self.set_playback_mode(self.mode.next());
        self.mode
    }

    pub fn set_playback_mode(&mut self, mode: PlaybackMode) {
        self.mode = mode;
        self.reset_shuffle();
    }

    pub fn current_index(&self) -> Option<usize> {
        self.current
    }

    pub fn play_index(&mut self, index: usize) -> Option<u64> {
        if index >= self.tracks.len() {
            return None;
        }
        self.current = Some(index);
        self.reset_shuffle();
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
        self.reset_shuffle();
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
        self.reset_shuffle();
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
        self.reset_shuffle();
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
        self.reset_shuffle();
        Some(index + 1)
    }

    pub fn clear(&mut self) {
        self.tracks.clear();
        self.current = None;
        self.reset_shuffle();
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
        match self.mode {
            PlaybackMode::Sequential => self.next_sequential(false),
            PlaybackMode::RepeatOne => {
                if self.current.is_none() && !self.tracks.is_empty() {
                    self.current = Some(0);
                }
                self.current_track()
            }
            PlaybackMode::RepeatAll => self.next_sequential(true),
            PlaybackMode::Shuffle => self.next_shuffle(),
        }
    }

    pub fn previous(&mut self) -> Option<u64> {
        match self.mode {
            PlaybackMode::Sequential | PlaybackMode::RepeatAll => {
                let current = self.current?;
                let previous = if current == 0 && self.mode == PlaybackMode::RepeatAll {
                    self.tracks.len().checked_sub(1)?
                } else {
                    current.saturating_sub(1)
                };
                self.current = Some(previous);
                self.current_track()
            }
            PlaybackMode::RepeatOne => self.current_track(),
            PlaybackMode::Shuffle => self.previous_shuffle(),
        }
    }

    fn next_sequential(&mut self, wrap: bool) -> Option<u64> {
        let next = self.current.map_or(0, |index| index.saturating_add(1));
        if next >= self.tracks.len() {
            if wrap && !self.tracks.is_empty() {
                self.current = Some(0);
                return self.current_track();
            }
            return None;
        }
        self.current = Some(next);
        self.current_track()
    }

    fn next_shuffle(&mut self) -> Option<u64> {
        if self.tracks.is_empty() {
            self.current = None;
            return None;
        }
        if self.tracks.len() == 1 {
            self.current = Some(0);
            return self.current_track();
        }

        if self.shuffle_remaining.is_empty() {
            self.refill_shuffle_remaining();
        }
        if self.shuffle_remaining.is_empty() {
            return self.current_track();
        }

        let slot = rand::rng().random_range(0..self.shuffle_remaining.len());
        let next = self.shuffle_remaining.swap_remove(slot);
        if let Some(current) = self.current {
            self.shuffle_history.push(current);
        }
        self.current = Some(next);
        self.current_track()
    }

    fn previous_shuffle(&mut self) -> Option<u64> {
        if self.tracks.is_empty() {
            self.current = None;
            return None;
        }

        let Some(previous) = self.shuffle_history.pop() else {
            return self.current_track();
        };
        if previous >= self.tracks.len() {
            self.reset_shuffle();
            return self.current_track();
        }
        if let Some(current) = self.current
            && current < self.tracks.len()
            && !self.shuffle_remaining.contains(&current)
        {
            self.shuffle_remaining.push(current);
        }
        self.current = Some(previous);
        self.current_track()
    }

    fn refill_shuffle_remaining(&mut self) {
        self.shuffle_remaining = (0..self.tracks.len())
            .filter(|&index| Some(index) != self.current)
            .collect();
    }

    fn reset_shuffle(&mut self) {
        self.shuffle_remaining.clear();
        self.shuffle_history.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::{PlaybackMode, PlaybackQueue};

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

    #[test]
    fn playback_modes_cycle_in_display_order() {
        let mut queue = PlaybackQueue::default();
        assert_eq!(queue.playback_mode(), PlaybackMode::Sequential);
        assert_eq!(queue.cycle_playback_mode(), PlaybackMode::RepeatOne);
        assert_eq!(queue.cycle_playback_mode(), PlaybackMode::RepeatAll);
        assert_eq!(queue.cycle_playback_mode(), PlaybackMode::Shuffle);
        assert_eq!(queue.cycle_playback_mode(), PlaybackMode::Sequential);
    }

    #[test]
    fn repeat_one_keeps_the_current_track() {
        let mut queue = PlaybackQueue::default();
        queue.enqueue_many([10, 20]);
        queue.play_index(1);
        queue.cycle_playback_mode();

        assert_eq!(queue.next(), Some(20));
        assert_eq!(queue.previous(), Some(20));
        assert_eq!(queue.current_index(), Some(1));
    }

    #[test]
    fn repeat_all_wraps_at_queue_edges() {
        let mut queue = PlaybackQueue::default();
        queue.enqueue_many([10, 20]);
        queue.play_index(1);
        queue.cycle_playback_mode();
        queue.cycle_playback_mode();

        assert_eq!(queue.next(), Some(10));
        assert_eq!(queue.previous(), Some(20));
    }

    #[test]
    fn shuffle_keeps_queue_order_and_avoids_repeats_until_cache_is_empty() {
        let mut queue = PlaybackQueue::default();
        queue.enqueue_many([10, 20, 30]);
        queue.play_index(0);
        queue.cycle_playback_mode();
        queue.cycle_playback_mode();
        queue.cycle_playback_mode();

        let first = queue.next();
        let second = queue.next();

        assert_eq!(queue.tracks(), &[10, 20, 30]);
        assert_ne!(first, Some(10));
        assert_ne!(second, Some(10));
        assert_ne!(second, first);
    }

    #[test]
    fn shuffle_previous_uses_play_history() {
        let mut queue = PlaybackQueue::default();
        queue.enqueue_many([10, 20]);
        queue.play_index(0);
        queue.cycle_playback_mode();
        queue.cycle_playback_mode();
        queue.cycle_playback_mode();

        assert_eq!(queue.next(), Some(20));
        assert_eq!(queue.previous(), Some(10));
    }
}
