use arrow_schema::SchemaRef;
use futures::Stream;
use futures::TryStreamExt;
use sedona_schema::projection::unwrap_batch;
use std::any::Any;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use arrow_array::{new_null_array, RecordBatch};
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::execution::RecordBatchStream;
use datafusion::prelude::DataFrame;
use datafusion::scalar::ScalarValue;
use datafusion::{common::DFSchema, execution::SendableRecordBatchStream};
use datafusion_expr::{ColumnarValue, Expr, ScalarUDF, ScalarUDFImpl, Signature};
use sedona_schema::{extension_type::ExtensionType, projection::unwrap_schema};

/// Possibly project a DataFrame such that the output expresses extension types as data types
///
/// This is a "lazy" version of wrap_arrow_batch() that appends a projection to a DataFrame.
pub fn wrap_df(df: DataFrame) -> Result<DataFrame> {
    if let Some(exprs) = wrap_expressions(df.schema())? {
        df.select(exprs)
    } else {
        Ok(df)
    }
}

/// Possibly project a DataFrame such that the output expresses extension types as data types
///
/// This is a "lazy" version of unwrap_arrow_batch() that appends a projection to a DataFrame.
pub fn unwrap_df(df: DataFrame) -> Result<(DFSchema, DataFrame)> {
    if let Some((schema, exprs)) = unwrap_expressions(df.schema())? {
        Ok((schema, df.select(exprs)?))
    } else {
        Ok((df.schema().clone(), df))
    }
}

/// Possibly project a SendableRecordBatchStream such that the output expresses extension
/// types as data types
pub fn unwrap_stream(stream: SendableRecordBatchStream) -> SendableRecordBatchStream {
    let wrapper = UnwrapRecordBatchStream { parent: stream };
    Box::pin(wrapper)
}

struct UnwrapRecordBatchStream {
    parent: SendableRecordBatchStream,
}

impl RecordBatchStream for UnwrapRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::new(unwrap_schema(&self.parent.schema()))
    }
}

impl Stream for UnwrapRecordBatchStream {
    type Item = Result<RecordBatch>;
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        match self.parent.try_poll_next_unpin(cx) {
            Poll::Ready(maybe_parent) => {
                Poll::Ready(maybe_parent.map(|parent| parent.map(unwrap_batch)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Implementation underlying wrap_df
///
/// Returns None if there is no need to wrap the input, or a list of expressions that
/// either pass along the existing column or a UDF call that applies the wrap.
pub(crate) fn wrap_expressions(schema: &DFSchema) -> Result<Option<Vec<Expr>>> {
    let wrap_udf = WrapExtensionUdf::udf();
    let mut wrap_count = 0;

    let mut exprs = Vec::with_capacity(schema.fields().len());
    for i in 0..exprs.capacity() {
        let this_column = Expr::Column(schema.columns()[i].clone());
        let (this_qualifier, this_field) = schema.qualified_field(i);

        if let Some(ext) = ExtensionType::from_field(schema.field(i)) {
            let dummy_array = new_null_array(&ext.to_data_type(), 1);
            let wrap_call = wrap_udf
                .call(vec![
                    this_column.clone(),
                    Expr::Literal(ScalarValue::try_from_array(&dummy_array, 0)?),
                ])
                .alias_qualified(this_qualifier.cloned(), this_field.name());

            exprs.push(wrap_call);
            wrap_count += 1;
        } else {
            exprs.push(this_column.alias_qualified(this_qualifier.cloned(), this_field.name()));
        }
    }

    if wrap_count > 0 {
        Ok(Some(exprs))
    } else {
        Ok(None)
    }
}

/// Implementation underlying unwrap_df
///
/// Returns None if there is no need to unwrap the input, or a list of expressions that
/// either pass along the existing column or a UDF call that applies the unwrap.
/// Returns a DFSchema because the resulting schema based purely on the expressions would
/// otherwise not include field metadata.
pub(crate) fn unwrap_expressions(schema: &DFSchema) -> Result<Option<(DFSchema, Vec<Expr>)>> {
    let unwrap_udf = UnwrapExtensionUdf::udf();
    let mut exprs = Vec::with_capacity(schema.fields().len());
    let mut qualifiers = Vec::with_capacity(exprs.capacity());
    let mut unwrap_count = 0;

    for i in 0..exprs.capacity() {
        let this_column = Expr::Column(schema.columns()[i].clone());
        let (this_qualifier, this_field) = schema.qualified_field(i);
        qualifiers.push(this_qualifier.cloned());

        if ExtensionType::from_data_type(this_field.data_type()).is_some() {
            let unwrap_call = unwrap_udf
                .call(vec![this_column.clone()])
                .alias_qualified(this_qualifier.cloned(), this_field.name());

            exprs.push(unwrap_call);
            unwrap_count += 1;
        } else {
            exprs.push(this_column.alias_qualified(this_qualifier.cloned(), this_field.name()));
        }
    }

    if unwrap_count > 0 {
        let schema_unwrapped = unwrap_schema(schema.as_arrow());
        let dfschema_unwrapped = DFSchema::from_field_specific_qualified_schema(
            qualifiers,
            &Arc::new(schema_unwrapped),
        )?;

        Ok(Some((dfschema_unwrapped, exprs)))
    } else {
        Ok(None)
    }
}

#[derive(Debug)]
pub struct WrapExtensionUdf {
    signature: Signature,
}

impl WrapExtensionUdf {
    pub fn udf() -> ScalarUDF {
        let signature = Signature::any(2, datafusion_expr::Volatility::Immutable);
        ScalarUDF::new_from_impl(Self { signature })
    }
}

impl ScalarUDFImpl for WrapExtensionUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "wrap_extension_internal"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        debug_assert_eq!(args.len(), 2);
        Ok(args[1].clone())
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _num_rows: usize) -> Result<ColumnarValue> {
        if let Some(extension_type) = ExtensionType::from_data_type(&args[1].data_type()) {
            extension_type.wrap_arg(&args[0])
        } else {
            Ok(args[0].clone())
        }
    }
}

#[derive(Debug)]
pub struct UnwrapExtensionUdf {
    signature: Signature,
}

impl UnwrapExtensionUdf {
    pub fn udf() -> ScalarUDF {
        let signature = Signature::any(1, datafusion_expr::Volatility::Immutable);
        ScalarUDF::new_from_impl(Self { signature })
    }
}

impl ScalarUDFImpl for UnwrapExtensionUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "unwrap_extension_internal"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        debug_assert_eq!(args.len(), 1);
        if let Some(extension_type) = ExtensionType::from_data_type(&args[0]) {
            Ok(extension_type.to_field("", true).data_type().clone())
        } else {
            Ok(args[0].clone())
        }
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _num_rows: usize) -> Result<ColumnarValue> {
        if let Some(extension) = ExtensionType::from_data_type(&args[0].data_type()) {
            extension.unwrap_arg(&args[0])
        } else {
            Ok(args[0].clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{create_array, record_batch, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use sedona_schema::projection::wrap_batch;

    use super::*;

    /// An ExtensionType for tests
    pub fn geoarrow_wkt() -> ExtensionType {
        ExtensionType::new("geoarrow.wkt", DataType::Utf8, None)
    }

    #[tokio::test]
    async fn df_wrap_unwrap() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("col1", DataType::Utf8, true),
            geoarrow_wkt().to_field("col2", true),
        ]);
        let col1 = create_array!(Utf8, ["POINT (0 1)", "POINT (2 3)"]);
        let col2 = col1.clone();

        let batch_no_extensions = record_batch!(("col1", Utf8, ["POINT (0 1)", "POINT (2 3)"]))?;
        let batch = RecordBatch::try_new(schema.clone().into(), vec![col1, col2])?;

        let ctx = SessionContext::new();

        // A batch with no extensions should be unchanged by wrap_df()
        let df_no_extensions = wrap_df(ctx.read_batch(batch_no_extensions.clone())?)?;
        let results_no_extensions = df_no_extensions.clone().collect().await?;
        assert_eq!(results_no_extensions.len(), 1);
        assert_eq!(results_no_extensions[0], batch_no_extensions);

        // A batch with no extensions should be unchanged by unwrap_df()
        let (schema_roundtrip_no_extensions, roundtrip_no_extensions) =
            unwrap_df(df_no_extensions.clone())?;
        assert_eq!(&schema_roundtrip_no_extensions, df_no_extensions.schema());
        assert_eq!(
            roundtrip_no_extensions.collect().await?[0],
            batch_no_extensions
        );

        // A batch with extensions should have extension fields wrapped as structs by df_wrap()
        let df = wrap_df(ctx.read_batch(batch.clone())?)?;
        let results = df.clone().collect().await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], wrap_batch(batch.clone()));

        // unwrap_df() will result in a batch with no extensions in the results
        // (but with the extension information communicated in the returned schema)
        let batch_without_extensions = record_batch!(
            ("col1", Utf8, ["POINT (0 1)", "POINT (2 3)"]),
            ("col2", Utf8, ["POINT (0 1)", "POINT (2 3)"])
        )?;
        let (schema_roundtrip, roundtrip) = unwrap_df(df)?;
        assert_eq!(schema_roundtrip.as_arrow(), &schema);

        assert_eq!(roundtrip.collect().await?[0], batch_without_extensions);

        Ok(())
    }
}
