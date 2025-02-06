use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaBuilder};

use crate::datatypes::{ExtensionType, LogicalArray};

/// Wrap a Schema possibly containing Extension Types
///
/// The resulting Schema will have all Extension types wrapped such that they
/// are propagated through operations that only supply a data type (e.g., UDF
/// execution). This is the projection that should be applied to input that
/// might contain extension types.
pub fn wrap_arrow_schema(schema: &Schema) -> Schema {
    let mut builder = SchemaBuilder::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let field_out = match ExtensionType::from_field(field) {
            Some(ext) => Field::new(field.name(), ext.to_data_type(), false).into(),
            None => field.clone(),
        };

        builder.push(field_out);
    }

    return builder.finish();
}

/// Unwrap a Schema that contains wrapped extension types
///
/// The resulting schema will have extension types represented with field metadata
/// instead of as wrapped structs. This is the projection that should be applied
/// when writing to output.
pub fn unwrap_arrow_schema(schema: &Schema) -> Schema {
    let mut builder = SchemaBuilder::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let field_out = match ExtensionType::from_data_type(field.data_type()) {
            Some(ext) => ext.to_field(field.name()).into(),
            None => field.clone(),
        };

        builder.push(field_out);
    }

    return builder.finish();
}

/// Wrap a record batch possibly containing extension types encoded as field metadata
///
/// The resulting batch will wrap columns with extension types as struct arrays
/// that can be passed to APIs that operate purely on ArrayRefs (e.g., UDFs).
/// This is the projection that should be applied when wrapping an input stream.
pub fn wrap_arrow_batch(batch: RecordBatch) -> RecordBatch {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for i in 0..batch.num_columns() {
        let column_out = match ExtensionType::from_field(batch.schema().field(i)) {
            Some(ext) => ext.wrap_storage(batch.column(i).clone()).unwrap(),
            None => batch.column(i).clone(),
        };
        columns.push(column_out);
    }

    let schema = wrap_arrow_schema(&batch.schema());
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

/// Unwrap a record batch such that the output expresses
///
/// The resulting output will have extension types represented with field metadata
/// instead of as wrapped structs. This is the projection that should be applied
/// when writing to output.
pub fn unwrap_arrow_batch(batch: RecordBatch) -> RecordBatch {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for i in 0..batch.num_columns() {
        let logical_array: LogicalArray = batch.column(i).clone().into();
        match logical_array {
            LogicalArray::Normal(array) => columns.push(array),
            LogicalArray::Extension(_, array) => columns.push(array),
        }
    }

    let schema = unwrap_arrow_schema(&batch.schema());
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;
    use datafusion::arrow::array::create_array;

    use super::*;

    /// An ExtensionType for tests
    pub fn geoarrow_wkt() -> ExtensionType {
        ExtensionType::new("geoarrow.wkt", DataType::Utf8, None)
    }

    #[test]
    fn schema_wrap_unwrap() {
        let mut builder = SchemaBuilder::new();
        builder.push(Field::new("field1", DataType::Boolean, true));
        builder.push(geoarrow_wkt().to_field("field2"));
        let schema_normal = builder.finish();

        let schema_wrapped = wrap_arrow_schema(&schema_normal);
        assert_eq!(schema_wrapped.field(0).name(), "field1");
        assert_eq!(*schema_wrapped.field(0).data_type(), DataType::Boolean);
        assert_eq!(schema_wrapped.field(1).name(), "field2");
        assert!(schema_wrapped.field(1).data_type().is_nested());

        let schema_unwrapped = unwrap_arrow_schema(&schema_wrapped);
        assert_eq!(schema_unwrapped, schema_normal);
    }

    #[test]
    fn batch_wrap_unwrap() {
        let schema = Schema::new(vec![
            Field::new("col1", DataType::Utf8, false),
            geoarrow_wkt().to_field("col2"),
        ]);

        let col1 = create_array!(Utf8, ["POINT (0 1)", "POINT (2, 3)"]);
        let col2 = col1.clone();

        let batch = RecordBatch::try_new(schema.into(), vec![col1, col2]).unwrap();
        let batch_wrapped = wrap_arrow_batch(batch.clone());
        assert_eq!(batch_wrapped.column(0).data_type(), &DataType::Utf8);
        assert!(batch_wrapped.column(1).data_type().is_nested());

        let batch_unwrapped = unwrap_arrow_batch(batch_wrapped);
        assert_eq!(batch_unwrapped, batch);
    }
}
