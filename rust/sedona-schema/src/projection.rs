use std::sync::Arc;

use arrow_array::{RecordBatch, StructArray};
use arrow_schema::{Field, Schema};

use crate::extension_type::ExtensionType;

/// Wrap a Schema possibly containing Extension Types
///
/// The resulting Schema will have all Extension types wrapped such that they
/// are propagated through operations that only supply a data type (e.g., UDF
/// execution). This is the projection that should be applied to input that
/// might contain extension types.
pub fn wrap_schema(schema: &Schema) -> Schema {
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| match ExtensionType::from_field(field) {
            Some(ext) => Field::new(field.name(), ext.to_data_type(), true).into(),
            None => field.clone(),
        })
        .collect();

    Schema::new(fields)
}

/// Unwrap a Schema that contains wrapped extension types
///
/// The resulting schema will have extension types represented with field metadata
/// instead of as wrapped structs. This is the projection that should be applied
/// when writing to output.
pub fn unwrap_schema(schema: &Schema) -> Schema {
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(
            |field| match ExtensionType::from_data_type(field.data_type()) {
                Some(ext) => ext.to_field(field.name(), true).into(),
                None => field.clone(),
            },
        )
        .collect();

    Schema::new(fields)
}

/// Wrap a record batch possibly containing extension types encoded as field metadata
///
/// The resulting batch will wrap columns with extension types as struct arrays
/// that can be passed to APIs that operate purely on ArrayRefs (e.g., UDFs).
/// This is the projection that should be applied when wrapping an input stream.
pub fn wrap_batch(batch: RecordBatch) -> RecordBatch {
    let columns = batch
        .columns()
        .iter()
        .enumerate()
        .map(|(i, column)| {
            if let Some(ext) = ExtensionType::from_field(batch.schema().field(i)) {
                ext.wrap_array(column.clone()).unwrap()
            } else {
                column.clone()
            }
        })
        .collect();

    let schema = wrap_schema(&batch.schema());
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

/// Unwrap a record batch such that the output expresses extension types as fields
///
/// The resulting output will have extension types represented with field metadata
/// instead of as wrapped structs. This is the projection that should be applied
/// when writing to output.
pub fn unwrap_batch(batch: RecordBatch) -> RecordBatch {
    let columns: Vec<_> = batch
        .columns()
        .iter()
        .map(|column| {
            if ExtensionType::from_data_type(column.data_type()).is_some() {
                let struct_array = StructArray::from(column.to_data());
                struct_array.column(0).clone()
            } else {
                column.clone()
            }
        })
        .collect();

    let schema = unwrap_schema(&batch.schema());
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

#[cfg(test)]
mod tests {
    use arrow_array::create_array;
    use arrow_schema::DataType;

    use super::*;

    /// An ExtensionType for tests
    pub fn geoarrow_wkt() -> ExtensionType {
        ExtensionType::new("geoarrow.wkt", DataType::Utf8, None)
    }

    #[test]
    fn schema_wrap_unwrap() {
        let schema_normal = Schema::new(vec![
            Field::new("field1", DataType::Boolean, true),
            geoarrow_wkt().to_field("field2", true),
        ]);

        let schema_wrapped = wrap_schema(&schema_normal);
        assert_eq!(schema_wrapped.field(0).name(), "field1");
        assert_eq!(*schema_wrapped.field(0).data_type(), DataType::Boolean);
        assert_eq!(schema_wrapped.field(1).name(), "field2");
        assert!(schema_wrapped.field(1).data_type().is_nested());

        let schema_unwrapped = unwrap_schema(&schema_wrapped);
        assert_eq!(schema_unwrapped, schema_normal);
    }

    #[test]
    fn batch_wrap_unwrap() {
        let schema = Schema::new(vec![
            Field::new("col1", DataType::Utf8, false),
            geoarrow_wkt().to_field("col2", true),
        ]);

        let col1 = create_array!(Utf8, ["POINT (0 1)", "POINT (2, 3)"]);
        let col2 = col1.clone();

        let batch = RecordBatch::try_new(schema.into(), vec![col1, col2]).unwrap();
        let batch_wrapped = wrap_batch(batch.clone());
        assert_eq!(batch_wrapped.column(0).data_type(), &DataType::Utf8);
        assert!(batch_wrapped.column(1).data_type().is_nested());

        let batch_unwrapped = unwrap_batch(batch_wrapped);
        assert_eq!(batch_unwrapped, batch);
    }
}
