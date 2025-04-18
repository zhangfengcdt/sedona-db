use futures::TryStreamExt;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use tokio::runtime::Runtime;

pub struct SedonaStreamReader {
    runtime: Arc<Runtime>,
    stream: SendableRecordBatchStream,
}

impl SedonaStreamReader {
    pub fn new(runtime: Arc<Runtime>, stream: SendableRecordBatchStream) -> Self {
        Self { runtime, stream }
    }
}

impl Iterator for SedonaStreamReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.runtime.block_on(self.stream.try_next()) {
            Ok(maybe_batch) => maybe_batch.map(Ok),
            Err(err) => Some(Err(ArrowError::ExternalError(Box::new(err)))),
        }
    }
}

impl RecordBatchReader for SedonaStreamReader {
    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}
