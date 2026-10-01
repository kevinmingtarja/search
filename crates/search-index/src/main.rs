use std::{process::ExitCode, sync::Arc};

use anyhow::Result;
use clap::Parser;
use object_store::aws::AmazonS3Builder;
use search_index::Indexer;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{info, error};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// S3 Endpoint URL.
    #[arg(long)]
    s3_endpoint: Option<String>,

    /// S3 Region.
    #[arg(long, default_value = "auto")]
    s3_region: String,

    /// S3 Bucket.
    #[arg(long)]
    s3_bucket: String,
}

#[tokio::main]
async fn start(args: Args) -> Result<()> {
    let s3_access_key_id = std::env::var("S3_ACCESS_KEY_ID").expect("S3_ACCESS_KEY_ID must be set");
    let s3_secret_access_key = std::env::var("S3_SECRET_ACCESS_KEY").expect("S3_SECRET_ACCESS_KEY must be set");
    
    let mut s3_builder = AmazonS3Builder::new()
        .with_region(args.s3_region)
        .with_bucket_name(args.s3_bucket)
        .with_access_key_id(s3_access_key_id)
        .with_secret_access_key(s3_secret_access_key);

    if let Some(endpoint) = args.s3_endpoint {
        s3_builder = s3_builder.with_endpoint(endpoint);
    }
    let s3 = Arc::new(s3_builder.build()?);

    let indexer = Indexer::new(s3).await?;

    let main_task = async {
        info!("indexer starting");
        indexer.start().await
    };

    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    let signals_task = async {
        tokio::select! {
            Some(()) = sigterm.recv() => (),
            Some(()) = sigint.recv() => (),
            else => return Ok(()),
        }
        info!("gracefully shutting down...");
        indexer.shutdown().await?;
        Ok(())
    };

    tokio::try_join!(main_task, signals_task)?;

    Ok(())
}

fn main() -> ExitCode {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or("info".into()))
        .with_writer(std::io::stderr)
        .init();

    match start(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            error!("{err:?}");
            ExitCode::FAILURE
        }
    }
}
