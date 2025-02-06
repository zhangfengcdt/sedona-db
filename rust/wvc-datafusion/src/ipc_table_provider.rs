use parking_lot::RwLock;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_schema::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::memory::{LazyBatchGenerator, LazyMemoryExec};

use crate::projection::{wrap_arrow_batch, wrap_arrow_schema};

pub fn ipc_stream_exec(reader: StreamReader<BufReader<File>>) -> Result<LazyMemoryExec> {
    // In theory could have multiple files here (for multiple generators)
    let generator = IpcStreamProvider::new(reader);
    let schema = generator.schema();
    let dyn_generator: Arc<RwLock<dyn LazyBatchGenerator>> = Arc::new(RwLock::new(generator));
    LazyMemoryExec::try_new(schema, vec![dyn_generator])
}

#[derive(Debug)]
pub struct IpcStreamProvider {
    reader: StreamReader<BufReader<File>>,
}

impl IpcStreamProvider {
    pub fn new(reader: StreamReader<BufReader<File>>) -> IpcStreamProvider {
        Self { reader }
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::new(wrap_arrow_schema(&self.reader.schema()))
    }
}

impl Display for IpcStreamProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "IpcStreamProvider")
    }
}

impl LazyBatchGenerator for IpcStreamProvider {
    fn generate_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let maybe_next = self.reader.next();
        match maybe_next {
            Some(next_result) => match next_result {
                Ok(batch) => Ok(Some(wrap_arrow_batch(batch))),
                Err(err) => Err(DataFusionError::ArrowError(err, None)),
            },
            None => Ok(None),
        }
    }
}
