use std::sync::Arc;

use anyhow::Result;
use search_core::wal::WalEntry;
use prost::Message;
use object_store::aws::AmazonS3;
use slatedb::{Db, Error};
use tokio_util::sync::CancellationToken;

// fn deserialize() {
//     WalEntry::decode(buf)
// }


pub struct Indexer {
    s3: Arc<AmazonS3>,
    db: Db,
    shutdown: CancellationToken
}

impl Indexer {
    pub async fn new(s3: Arc<AmazonS3>) -> Result<Self> {
        let db = Db::open("/lsm", s3.clone()).await?;
        let shutdown = CancellationToken::new();
        Ok(Self {
            s3,
            db,
            shutdown
        })
    }

    pub async fn start(&self) -> Result<()> {
        let cloned_shutdown = self.shutdown.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cloned_shutdown.cancelled() => {
                        println!("shutting down indexer...");
                        break;
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                        println!("doing some work...")
                    }
                }
            }
            Ok(())
        });

        task.await?
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.shutdown.cancel();
        self.db.close().await?;
        Ok(())
    }
}
