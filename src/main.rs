mod api;
mod app;
mod config;
mod downloads;
mod error;
mod hls;
mod library;
mod oauth;
mod player;
mod progressive;
mod queue;
mod session;
mod storage;
mod ui;

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Seek, SeekFrom, Write},
    path::Path,
    sync::Mutex,
};

use clap::Parser;
use config::{Cli, Config};
use error::Result;
use storage::TokenStore;
use tracing_subscriber::EnvFilter;

const LOG_FILE_MAX_BYTES: u64 = 1024 * 1024;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = Config::load(&cli)?;
    init_logging(&cli.log_level, &config.config_dir);

    let token_store = TokenStore::new(config.config_dir.clone(), config.plain_token_file);
    app::App::new(config, token_store).run().await
}

fn init_logging(level: &str, config_dir: &Path) {
    let log_file = config_dir.join("ibroadcast-tui.log");
    let writer = fs::create_dir_all(config_dir).and_then(|()| BoundedLogWriter::open(&log_file));

    match writer {
        Ok(writer) => {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(logging_filter(level))
                .with_target(false)
                .with_ansi(false)
                .with_writer(Mutex::new(writer))
                .try_init();
        }
        Err(_) => {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(logging_filter(level))
                .with_target(false)
                .with_ansi(false)
                .with_writer(std::io::sink)
                .try_init();
        }
    }
}

fn logging_filter(level: &str) -> EnvFilter {
    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("warn"));
    filter.add_directive(
        "symphonia_core::probe=off"
            .parse()
            .expect("valid symphonia probe log directive"),
    )
}

struct BoundedLogWriter {
    file: File,
    bytes_written: u64,
}

impl BoundedLogWriter {
    fn open(path: &Path) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        let mut bytes_written = file.metadata()?.len();
        if bytes_written > LOG_FILE_MAX_BYTES {
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;
            bytes_written = 0;
        }
        Ok(Self {
            file,
            bytes_written,
        })
    }

    fn reset_if_needed(&mut self, next_len: usize) -> io::Result<()> {
        if self.bytes_written.saturating_add(next_len as u64) <= LOG_FILE_MAX_BYTES {
            return Ok(());
        }
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.bytes_written = 0;
        Ok(())
    }
}

impl Write for BoundedLogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.reset_if_needed(buf.len())?;
        let written = self.file.write(buf)?;
        self.bytes_written = self.bytes_written.saturating_add(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}
