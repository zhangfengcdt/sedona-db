// Because a number of methods only return Err() for not implemented,
// the compiler doesn't know how to guess which impl RecordBatchReader
// will be returned. When we implement the methods, we can remove this.
#![allow(refining_impl_trait)]

use adbc_core::{
    options::{InfoCode, ObjectDepth},
    Connection,
};
use sedona::context::SedonaContext;
use std::sync::Arc;
use tokio::runtime::Runtime;

use adbc_core::{
    error::{Error, Result, Status},
    options::{OptionConnection, OptionValue},
    Optionable,
};

use crate::{
    err_not_implemented, err_unrecognized_option, reader::SedonaStreamReader,
    statement::SedonaStatement, utils::from_datafusion_error,
};

pub struct SedonaConnection {
    runtime: Arc<Runtime>,
    ctx: Arc<SedonaContext>,
}

impl SedonaConnection {
    pub(crate) fn try_new(
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                Error::with_message_and_status(
                    format!("Failed to build multithreaded runtime: {e}"),
                    Status::Internal,
                )
            })?;

        let ctx = runtime.block_on(async {
            SedonaContext::new_local_interactive()
                .await
                .map_err(from_datafusion_error)
        })?;

        let mut connection = Self {
            runtime: Arc::new(runtime),
            ctx: Arc::new(ctx),
        };

        for (key, value) in opts {
            connection.set_option(key, value)?;
        }

        Ok(connection)
    }
}

impl Optionable for SedonaConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, _value: OptionValue) -> Result<()> {
        err_unrecognized_option!(key)
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        err_unrecognized_option!(key)
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        err_unrecognized_option!(key)
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        err_unrecognized_option!(key)
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        err_unrecognized_option!(key)
    }
}

impl Connection for SedonaConnection {
    type StatementType = SedonaStatement;

    fn new_statement(&mut self) -> Result<SedonaStatement> {
        Ok(SedonaStatement::new(self.runtime.clone(), self.ctx.clone()))
    }

    fn cancel(&mut self) -> Result<()> {
        err_not_implemented!()
    }

    fn get_info(
        &self,
        _codes: Option<std::collections::HashSet<InfoCode>>,
    ) -> Result<SedonaStreamReader> {
        err_not_implemented!()
    }

    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> Result<SedonaStreamReader> {
        err_not_implemented!()
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> Result<arrow_schema::Schema> {
        err_not_implemented!()
    }

    fn get_table_types(&self) -> Result<SedonaStreamReader> {
        err_not_implemented!()
    }

    fn get_statistic_names(&self) -> Result<SedonaStreamReader> {
        err_not_implemented!()
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<SedonaStreamReader> {
        err_not_implemented!()
    }

    fn commit(&mut self) -> Result<()> {
        err_not_implemented!()
    }

    fn rollback(&mut self) -> Result<()> {
        err_not_implemented!()
    }

    fn read_partition(&self, _partition: impl AsRef<[u8]>) -> Result<SedonaStreamReader> {
        err_not_implemented!()
    }
}
