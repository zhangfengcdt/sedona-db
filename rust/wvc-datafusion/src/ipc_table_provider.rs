use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::common::Statistics;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

#[derive(Debug, Clone)]
struct CustomExec {
    path: String,
    projected_schema: SchemaRef,
}

impl DisplayAs for CustomExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CustomExec")
    }
}

impl ExecutionPlan for CustomExec {
    fn name(&self) -> &str {
        todo!()
    }

    fn properties(&self) -> &PlanProperties {
        todo!()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn children(&self) -> Vec<&Arc<(dyn ExecutionPlan + 'static)>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // let users: Vec<User> = {
        //     let db = self.db.inner.lock().unwrap();
        //     db.data.values().cloned().collect()
        // };

        // let mut id_array = UInt8Builder::with_capacity(users.len());
        // let mut account_array = UInt64Builder::with_capacity(users.len());

        // for user in users {
        //     id_array.append_value(user.id);
        //     account_array.append_value(user.bank_account);
        // }

        // Ok(Box::pin(MemoryStream::try_new(
        //     vec![RecordBatch::try_new(
        //         self.projected_schema.clone(),
        //         vec![
        //             Arc::new(id_array.finish()),
        //             Arc::new(account_array.finish()),
        //         ],
        //     )?],
        //     self.schema(),
        //     None,
        // )?))
        todo!()
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.projected_schema))
    }
}
