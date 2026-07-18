use std::{
    io::{self, Read, Seek, SeekFrom},
    sync::{Arc, Condvar, Mutex},
};

#[derive(Clone, Debug)]
pub struct ProgressiveBuffer {
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    changed: Condvar,
}

#[derive(Debug, Default)]
struct State {
    data: Vec<u8>,
    content_len: Option<u64>,
    finished: bool,
    error: Option<String>,
    /// Readers capture this value when they are created. Advancing it wakes
    /// and invalidates only existing readers while preserving buffered data
    /// and the network feeder for a replacement audio output.
    reader_epoch: u64,
}

#[derive(Clone, Debug)]
pub struct ProgressiveReader {
    shared: Arc<Shared>,
    pos: u64,
    reader_epoch: u64,
}

impl ProgressiveBuffer {
    pub fn new(content_len: Option<u64>) -> Self {
        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    content_len,
                    ..State::default()
                }),
                changed: Condvar::new(),
            }),
        }
    }

    pub fn reader(&self) -> ProgressiveReader {
        let reader_epoch = self
            .shared
            .state
            .lock()
            .expect("progressive buffer poisoned")
            .reader_epoch;
        ProgressiveReader {
            shared: Arc::clone(&self.shared),
            pos: 0,
            reader_epoch,
        }
    }

    /// Cancels every reader created before this call without failing the
    /// shared transfer. New readers can immediately reuse all buffered bytes.
    pub fn cancel_current_readers(&self) {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("progressive buffer poisoned");
        state.reader_epoch = state.reader_epoch.wrapping_add(1);
        self.shared.changed.notify_all();
    }

    pub fn set_content_len(&self, content_len: Option<u64>) {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("progressive buffer poisoned");
        state.content_len = content_len;
        self.shared.changed.notify_all();
    }

    pub fn push(&self, chunk: &[u8]) {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("progressive buffer poisoned");
        state.data.extend_from_slice(chunk);
        self.shared.changed.notify_all();
    }

    pub fn finish(&self) {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("progressive buffer poisoned");
        state.finished = true;
        self.shared.changed.notify_all();
    }

    pub fn fail(&self, error: impl Into<String>) {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("progressive buffer poisoned");
        state.error = Some(error.into());
        state.finished = true;
        self.shared.changed.notify_all();
    }
}

impl Read for ProgressiveReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }

        let mut state = self.shared.state.lock().map_err(|_| poisoned())?;
        loop {
            if self.reader_epoch != state.reader_epoch {
                return Err(reader_cancelled());
            }
            if let Some(error) = state.error.as_ref()
                && self.pos >= state.data.len() as u64
            {
                return Err(io::Error::other(error.clone()));
            }

            if self.pos < state.data.len() as u64 {
                let start = self.pos as usize;
                let available = state.data.len() - start;
                let count = available.min(out.len());
                out[..count].copy_from_slice(&state.data[start..start + count]);
                self.pos += count as u64;
                return Ok(count);
            }

            if state.finished {
                return Ok(0);
            }

            state = self.shared.changed.wait(state).map_err(|_| poisoned())?;
        }
    }
}

impl Seek for ProgressiveReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let mut state = self.shared.state.lock().map_err(|_| poisoned())?;
        let new_pos = loop {
            if self.reader_epoch != state.reader_epoch {
                return Err(reader_cancelled());
            }
            let len = state.content_len.or_else(|| {
                if state.finished {
                    Some(state.data.len() as u64)
                } else {
                    None
                }
            });

            match pos {
                SeekFrom::Start(offset) => break offset,
                SeekFrom::Current(offset) => break checked_offset(self.pos, offset)?,
                SeekFrom::End(offset) => {
                    if let Some(len) = len {
                        break checked_offset(len, offset)?;
                    }
                    state = self.shared.changed.wait(state).map_err(|_| poisoned())?;
                }
            }
        };
        self.pos = new_pos;
        Ok(self.pos)
    }
}

fn checked_offset(base: u64, offset: i64) -> io::Result<u64> {
    if offset >= 0 {
        base.checked_add(offset as u64)
    } else {
        base.checked_sub(offset.unsigned_abs())
    }
    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid seek offset"))
}

fn poisoned() -> io::Error {
    io::Error::other("progressive buffer lock poisoned")
}

fn reader_cancelled() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "progressive reader replaced by a new audio output",
    )
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Read, Seek, SeekFrom},
        sync::mpsc,
        time::Duration,
    };

    use super::ProgressiveBuffer;

    #[test]
    fn reader_blocks_until_bytes_are_available() {
        let buffer = ProgressiveBuffer::new(Some(3));
        let mut reader = buffer.reader();
        buffer.push(b"abc");

        let mut out = [0; 2];
        assert_eq!(reader.read(&mut out).unwrap(), 2);
        assert_eq!(&out, b"ab");
        assert_eq!(reader.read(&mut out).unwrap(), 1);
        assert_eq!(out[0], b'c');
    }

    #[test]
    fn reader_can_seek_within_buffered_data() {
        let buffer = ProgressiveBuffer::new(Some(5));
        let mut reader = buffer.reader();
        buffer.push(b"hello");
        assert_eq!(reader.seek(SeekFrom::End(-2)).unwrap(), 3);

        let mut out = [0; 2];
        assert_eq!(reader.read(&mut out).unwrap(), 2);
        assert_eq!(&out, b"lo");
    }

    #[test]
    fn readers_keep_independent_positions() {
        let buffer = ProgressiveBuffer::new(Some(6));
        buffer.push(b"abcdef");

        let mut first = buffer.reader();
        let mut second = buffer.reader();
        let mut first_out = [0; 2];
        let mut second_out = [0; 3];

        assert_eq!(first.read(&mut first_out).unwrap(), 2);
        assert_eq!(&first_out, b"ab");
        assert_eq!(second.read(&mut second_out).unwrap(), 3);
        assert_eq!(&second_out, b"abc");

        assert_eq!(first.read(&mut first_out).unwrap(), 2);
        assert_eq!(&first_out, b"cd");
        assert_eq!(second.read(&mut second_out[..2]).unwrap(), 2);
        assert_eq!(&second_out[..2], b"de");
    }

    #[test]
    fn replacement_reader_reuses_already_buffered_bytes() {
        let buffer = ProgressiveBuffer::new(Some(6));
        buffer.push(b"cached");
        buffer.finish();

        let mut original = buffer.reader();
        let mut consumed = [0; 4];
        assert_eq!(original.read(&mut consumed).unwrap(), 4);
        assert_eq!(&consumed, b"cach");
        drop(original);

        // No additional push is needed: a replacement reader starts with an
        // independent cursor over the bytes retained by the shared buffer.
        let mut replacement = buffer.reader();
        let mut replayed = Vec::new();
        replacement.read_to_end(&mut replayed).unwrap();
        assert_eq!(replayed, b"cached");
    }

    #[test]
    fn cancelling_old_readers_wakes_them_without_discarding_the_buffer() {
        let buffer = ProgressiveBuffer::new(None);
        let mut old_reader = buffer.reader();
        let (tx, rx) = mpsc::channel();
        let blocked = std::thread::spawn(move || {
            let mut byte = [0; 1];
            tx.send(old_reader.read(&mut byte).unwrap_err().kind())
                .unwrap();
        });

        buffer.cancel_current_readers();
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            io::ErrorKind::BrokenPipe
        );
        blocked.join().unwrap();

        // Cancellation is reader-local: the feeder can continue, and a new
        // output gets all bytes retained by the same buffer.
        buffer.push(b"still cached");
        buffer.finish();
        let mut replacement = buffer.reader();
        let mut bytes = Vec::new();
        replacement.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"still cached");
    }
}
