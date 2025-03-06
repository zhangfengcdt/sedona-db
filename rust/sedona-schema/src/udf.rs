use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::common::plan_err;
use datafusion::error::Result;
use datafusion::logical_expr::{Signature, Volatility};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

use crate::logical_type::LogicalType;

pub type ScalarFunctionImplementation =
    Arc<dyn Fn(&[ColumnarValue], usize) -> Result<ColumnarValue> + Send + Sync>;

pub struct SimpleSpatialUdf {
    name: String,
    input_types: Vec<LogicalType>,
    return_type: LogicalType,
    signature: Signature,
    fun: ScalarFunctionImplementation,
}

impl SimpleSpatialUdf {
    pub fn new(
        name: String,
        input_types: Vec<LogicalType>,
        return_type: LogicalType,
        volatility: Volatility,
        fun: ScalarFunctionImplementation,
    ) -> Self {
        let signature = Signature::user_defined(volatility);

        return Self {
            name,
            input_types,
            return_type,
            signature,
            fun,
        };
    }
}

impl Debug for SimpleSpatialUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("SimpleSpatialUdf")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("return_type", &self.return_type)
            .field("fun", &"<FUNC>")
            .finish()
    }
}

impl ScalarUDFImpl for SimpleSpatialUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        return &self.name;
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if args.len() != self.input_types.len() {
            return plan_err!("Wrong number arguments passed to simple scalar function");
        }

        match &self.return_type {
            LogicalType::Normal(data_type) => Ok(data_type.clone()),
            LogicalType::Extension(extension_type) => Ok(extension_type.to_data_type()),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        // TODO: call data_types_with_scalar_udf to coerce non-spatial arguments
        let mut out: Vec<DataType> = vec![];
        for data_type in arg_types {
            out.push(data_type.clone());
        }
        Ok(out)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
        (self.fun)(args, number_rows)
    }
}
