use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::internal_err;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::{Expr, TableType};
use parking_lot::RwLock;
use std::any::Any;
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

#[derive(Debug)]
pub struct CustomDataSource {
    raw_source_schema: SchemaRef,
    maybe_reader: Option<StreamReader<BufReader<File>>>,
}

impl CustomDataSource {
    pub fn new(reader: StreamReader<BufReader<File>>) -> Self {
        let raw_source_schema = reader.schema();
        let maybe_reader = Some(reader);
        Self {
            raw_source_schema,
            maybe_reader,
        }
    }

    pub(crate) async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match self.maybe_reader {
            Some(reader) => {
                let generator = IpcStreamGenerator::new(reader);
                let schema = generator.schema();
                let dyn_generator: Arc<RwLock<dyn LazyBatchGenerator>> =
                    Arc::new(RwLock::new(generator));
                let exec = LazyMemoryExec::try_new(
                    schema,
                    vec![dyn_generator],
                );
                Ok(Arc::new(exec))
            }
            None => internal_err!("Can't scan twice"),
        }
    }
}

#[async_trait]
impl TableProvider for CustomDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(wrap_arrow_schema(&self.raw_source_schema))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection, self.schema()).await;
    }
}

pub fn ipc_stream_exec(reader: StreamReader<BufReader<File>>) -> Result<LazyMemoryExec> {
    // In theory could have multiple files here (for multiple generators)
    let generator = IpcStreamGenerator::new(reader);
    let schema = generator.schema();
    let dyn_generator: Arc<RwLock<dyn LazyBatchGenerator>> = Arc::new(RwLock::new(generator));
    LazyMemoryExec::try_new(schema, vec![dyn_generator])
}

#[derive(Debug)]
pub struct IpcStreamGenerator {
    reader: StreamReader<BufReader<File>>,
}

impl IpcStreamGenerator {
    pub fn new(reader: StreamReader<BufReader<File>>) -> IpcStreamGenerator {
        Self { reader }
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::new(wrap_arrow_schema(&self.reader.schema()))
    }
}

impl Display for IpcStreamGenerator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "IpcStreamGenerator")
    }
}

impl LazyBatchGenerator for IpcStreamGenerator {
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
