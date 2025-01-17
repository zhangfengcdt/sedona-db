use std::collections::HashMap;

use arrow_schema::{DataType, Field, Fields};
use datafusion::logical_expr::{Signature, Volatility};

#[derive(Debug)]
pub struct ExtensionType {
    extension_name: String,
    storage_type: DataType,
    extension_metadata: Option<String>,
}

impl ExtensionType {
    pub fn new(ext_name: &str, storage_type: DataType, extension_metadata: Option<String>) -> Self {
        let extension_name = ext_name.to_string();
        Self {
            extension_name,
            storage_type,
            extension_metadata,
        }
    }

    pub fn to_field(&self, name: &str) -> Field {
        let mut field = Field::new(name, self.storage_type.clone(), false);
        let mut metadata = HashMap::from([(
            "ARROW:extension:name".to_string(),
            self.extension_name.clone(),
        )]);

        match &self.extension_metadata {
            Some(extension_metadata) => {
                metadata.insert(
                    "ARROW:extension:metadata".to_string(),
                    extension_metadata.clone(),
                );
            }
            None => {}
        }

        field.set_metadata(metadata);
        return field;
    }

    pub fn wrap_struct(&self) -> DataType {
        let field = self.to_field(&self.extension_name);
        return DataType::Struct(Fields::from(vec![field]));
    }

    pub fn unwrap_struct(storage_type: &DataType) -> Option<ExtensionType> {
        match storage_type {
            DataType::Struct(fields) => {
                if fields.len() != 1 {
                    return None;
                }

                let field = &fields[0];
                let metadata = field.metadata();

                match metadata.get("ARROW:extension:name") {
                    Some(extension_name) => {
                        if extension_name != field.name() {
                            return None;
                        }
                    }
                    None => return None,
                }

                Some(ExtensionType::new(
                    field.name(),
                    field.data_type().clone(),
                    metadata.get("ARROW:extension:metadata").cloned(),
                ))
            }
            _ => return None,
        }
    }
}

pub fn geoarrow_wkt() -> ExtensionType {
    ExtensionType::new("geoarrow.wkt", DataType::Binary, None)
}

pub fn any_single_geometry_type_input() -> Signature {
    Signature::uniform(1, vec![geoarrow_wkt().wrap_struct()], Volatility::Immutable)
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
        let ext_type = ExtensionType::new("foofy", DataType::Binary, None);

        let field = ext_type.to_field("some name");
        assert_eq!(field.name(), "some name");
        assert_eq!(*field.data_type(), DataType::Binary);

        let metadata = field.metadata();
        assert_eq!(metadata.len(), 1);
        assert!(metadata.contains_key("ARROW:extension:name"));
        assert_eq!(metadata["ARROW:extension:name"], "foofy");
    }

    #[test]
    fn extension_type_field_with_metadata() {
        let ext_type = ExtensionType::new(
            "foofy",
            DataType::Binary,
            Some("foofy metadata".to_string()),
        );
        let field = ext_type.to_field("some name");
        let metadata = field.metadata();
        assert_eq!(metadata.len(), 2);
        assert!(metadata.contains_key("ARROW:extension:name"));
        assert_eq!(metadata["ARROW:extension:name"], "foofy");
        assert!(metadata.contains_key("ARROW:extension:metadata"));
        assert_eq!(metadata["ARROW:extension:metadata"], "foofy metadata");
    }

    #[test]
    fn extension_type_struct() {
        let ext_type = ExtensionType::new(
            "foofy",
            DataType::Binary,
            Some("foofy metadata".to_string()),
        );
        let ext_struct = &ext_type.wrap_struct();
        match ext_struct {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].name(), "foofy");
            }
            _ => panic!("not a struct"),
        }

        match ExtensionType::unwrap_struct(ext_struct) {
            Some(ext_type) => {
                assert_eq!(ext_type.extension_name, "foofy");
                assert_eq!(
                    ext_type.extension_metadata,
                    Some("foofy metadata".to_string())
                );
                assert_eq!(ext_type.storage_type, DataType::Binary);
            }
            None => panic!("unwrap did not detect valid extension type"),
        }
    }

    #[test]
    fn geometry_type_signature() {
        let udf = ScalarUDF::from(DoNothing::new());

        // Fail with an invalid type
        data_types_with_scalar_udf(&[DataType::Binary], &udf).expect_err("should fail");

        // Pass with an extension type wrapped in a struct
        let valid_type = geoarrow_wkt().wrap_struct();
        data_types_with_scalar_udf(&[valid_type], &udf).expect("should pass");

        // The matching includes the field metadata, so this fails to match
        // This can be overcome by implementing coerce_types() instead of signature()
        let valid_type_with_other_metadata = DataType::Struct(Fields::from(vec![Field::new(
            "geoarrow.wkt",
            DataType::Binary,
            false,
        )]));
        data_types_with_scalar_udf(&[valid_type_with_other_metadata], &udf)
            .expect_err("should fail");
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
