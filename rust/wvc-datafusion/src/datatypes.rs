use std::collections::HashMap;

use arrow_schema::{DataType, Field, Fields};
use datafusion::logical_expr::{Signature, Volatility};

#[derive(Debug)]
pub struct ExtensionType {
    extension_name: &'static str,
    storage_type: DataType,
}

impl ExtensionType {
    pub const fn new(extension_name: &'static str, storage_type: DataType) -> Self {
        Self {
            extension_name,
            storage_type,
        }
    }

    pub fn to_field(&self, name: &str) -> Field {
        let field = Field::new(name, self.storage_type.clone(), false);
        return field.with_metadata(HashMap::from([(
            "ARROW:extension:name".to_string(),
            self.extension_name.to_string(),
        )]));
    }

    pub fn wrap_struct(&self) -> DataType {
        return DataType::Struct(Fields::from(vec![self.to_field(self.extension_name)]));
    }
}

pub const GEOARROW_WKT: ExtensionType = ExtensionType::new("geoarrow.wkt", DataType::Binary);

pub fn any_single_geometry_type_input() -> Signature {
    Signature::uniform(1, vec![GEOARROW_WKT.wrap_struct()], Volatility::Immutable)
}

#[cfg(test)]
mod tests {
    use datafusion::error::DataFusionError;
    use datafusion::error::Result;
    use datafusion_expr::type_coercion::functions::data_types_with_scalar_udf;
    use datafusion_expr::ScalarUDF;
    use std::any::Any;

    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use super::*;

    #[test]
    fn extension_type_field() {
        let ext_type = ExtensionType::new("foofy", DataType::Binary);

        let field = ext_type.to_field("some name");
        assert_eq!(field.name(), "some name");
        assert_eq!(*field.data_type(), DataType::Binary);

        let metadata = field.metadata();
        assert!(metadata.contains_key("ARROW:extension:name"));
        assert_eq!(metadata["ARROW:extension:name"], "foofy");
    }

    #[test]
    fn extension_type_struct() {
        let ext_struct = ExtensionType::new("foofy", DataType::Binary);
        match ext_struct.wrap_struct() {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].name(), "foofy");
            }
            _ => panic!("not a struct"),
        }
    }

    #[test]
    fn geometry_type_signature() {
        let udf = ScalarUDF::from(DoNothing::new());

        // Fail with an invalid type
        data_types_with_scalar_udf(&[DataType::Binary], &udf).expect_err("should fail");

        // Pass with an extension type wrapped in a struct
        let valid_type = GEOARROW_WKT.wrap_struct();
        data_types_with_scalar_udf(&[valid_type], &udf).expect("should pass");

        // The matching includes the field metadata, so this fails to match
        // This can be overcome by implementing coerce_types() instead of signature()
        let valid_type_with_other_metadata = DataType::Struct(Fields::from(vec![Field::new(
            "geoarrow.wkt",
            DataType::Binary,
            false,
        )]));
        data_types_with_scalar_udf(&[valid_type_with_other_metadata], &udf).expect_err("should fail");
    }

    #[derive(Debug)]
    struct DoNothing {
        signature: Signature,
    }

    impl DoNothing {
        fn new() -> Self {
            let signature = any_single_geometry_type_input();
            Self { signature }
        }
    }

    impl ScalarUDFImpl for DoNothing {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn name(&self) -> &str {
            "do_nothing"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int32)
        }
        // The actual implementation would add one to the argument
        fn invoke_batch(&self, _: &[ColumnarValue], _number_rows: usize) -> Result<ColumnarValue> {
            Err(DataFusionError::NotImplemented("".to_string()))
        }
    }
}
