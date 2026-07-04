mod api;
mod app;
mod config;
mod download;
mod error;
mod library;
mod oauth;
mod playback;
mod progressive;
mod queue;
mod storage;

use clap::Parser;
use config::{AppConfig, Cli};
use error::Result;
use storage::TokenStore;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_logging(&cli.log_level);

    let (config, paths) = AppConfig::load(&cli)?;
    let token_store = TokenStore::new(paths.config_dir.clone(), config.plain_token_file);
    let app = app::App::new(config, paths, token_store);
    app.run().await
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
