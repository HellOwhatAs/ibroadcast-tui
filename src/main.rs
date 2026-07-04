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

use clap::Parser;
use config::{Cli, Config};
use error::Result;
use storage::TokenStore;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_logging(&cli.log_level);

    let config = Config::load(&cli)?;
    let token_store = TokenStore::new(config.config_dir.clone(), config.plain_token_file);
    app::App::new(config, token_store).run().await
}

fn init_logging(level: &str) {
    let filter = tracing_subscriber::EnvFilter::try_new(level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .try_init();
}
