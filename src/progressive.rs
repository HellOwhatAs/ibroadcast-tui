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
}

#[derive(Clone, Debug)]
pub struct ProgressiveReader {
    shared: Arc<Shared>,
    pos: u64,
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
        ProgressiveReader {
            shared: Arc::clone(&self.shared),
            pos: 0,
        }
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

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, SeekFrom};

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
}
