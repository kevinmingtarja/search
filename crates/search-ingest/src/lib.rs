use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use search_core::wal;
use worker::{Bucket, Conditional, Context, Env, Request, Response, RouteContext, Router, event};

/// Attempts to claim the next WAL id before giving up on a request.
const MAX_APPEND_ATTEMPTS: u32 = 10;

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> worker::Result<Response> {
    Router::new()
        .post_async("/ingest", ingest)
        .run(req, env)
        .await
}

async fn ingest(mut req: Request, ctx: RouteContext<()>) -> worker::Result<Response> {
    let body: IngestRequest = match req.json().await {
        Ok(body) => body,
        Err(e) => return Response::error(format!("invalid request body: {e}"), 400),
    };
    let entry = wal::WalEntry::try_from(body)?;

    let bucket = ctx.bucket("WAL")?;
    append_wal(&bucket, &entry).await?;

    let rows_affected = (entry.upserts.len() + entry.deletes.len()) as u64;
    Response::from_json(&IngestResponse { rows_affected })
}

/// Appends `entry` to the WAL under the next free id.
async fn append_wal(bucket: &Bucket, entry: &wal::WalEntry) -> worker::Result<()> {
    let data = entry.encode_to_vec();
    for _ in 0..MAX_APPEND_ATTEMPTS {
        let id = find_next_wal_id(bucket).await?;
        let created = bucket
            .put(wal_key(id), data.clone())
            .only_if(Conditional {
                // "*" matches any existing object, so the put only creates.
                etag_does_not_match: Some("*".into()),
                ..Default::default()
            })
            .execute()
            .await?;
        if created.is_some() {
            return Ok(());
        }
    }
    Err(format!("no free WAL id after {MAX_APPEND_ATTEMPTS} attempts").into())
}

/// Returns the id after the highest WAL id in the bucket, or 1 if the WAL is empty.
async fn find_next_wal_id(bucket: &Bucket) -> worker::Result<u64> {
    let mut head = 0;
    let mut cursor = None;
    loop {
        let mut list = bucket.list().prefix("wal/");
        if let Some(c) = cursor {
            list = list.cursor(c);
        }
        let page = list.execute().await?;
        if let Some(last) = page.objects().last().map(|o| o.key()) {
            head = parse_wal_key(&last).ok_or_else(|| format!("malformed WAL key: {last}"))?;
        }
        if !page.truncated() {
            return Ok(head + 1);
        }
        cursor = page.cursor();
    }
}

/// Zero-padded so lexicographic key order matches numeric id order.
fn wal_key(id: u64) -> String {
    format!("wal/{id:020}.bin")
}

fn parse_wal_key(key: &str) -> Option<u64> {
    key.strip_prefix("wal/")?.strip_suffix(".bin")?.parse().ok()
}

/// Request body for ingest.
#[derive(Debug, Deserialize)]
pub struct IngestRequest {
    /// Upserts documents in a row-based format.
    /// 
    /// Existing documents with matching IDs are overwritten entirely.
    pub upserts: Vec<Document>,

    /// Deletes documents by ID.
    pub deletes: Vec<u64>
}

/// Response body for ingest.
#[derive(Debug, Serialize)]
pub struct IngestResponse {
    /// The total number of rows affected by the write request
    /// (sum of upserted and deleted rows).
    pub rows_affected: u64
}

#[derive(Debug, Deserialize)]
pub struct Document {
    /// Document ID.
    /// 
    /// All documents must have a unique id attribute.
    pub id: u64,

    /// Other attribute fields.
    #[serde(flatten)]
    pub fields: Map<String, Value>
}

impl TryFrom<IngestRequest> for wal::WalEntry {
    type Error = serde_json::Error;

    fn try_from(req: IngestRequest) -> Result<Self, Self::Error> {
        let upserts = req
            .upserts
            .into_iter()
            .map(|doc| {
                Ok(wal::Document {
                    id: Some(doc.id),
                    attributes: Some(serde_json::to_vec(&doc.fields)?),
                })
            })
            .collect::<Result<_, _>>()?;
        Ok(wal::WalEntry { upserts, deletes: req.deletes })
    }
}
