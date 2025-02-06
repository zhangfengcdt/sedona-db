use std::{collections::HashMap, sync::Arc};

use arrow_schema::{DataType, Field, Fields};
use datafusion::arrow::array::{ArrayRef, StructArray};
use datafusion::common::internal_err;
use datafusion::error::Result;

/// Parsed representation of an Arrow extension type
///
/// Because arrow-rs doesn't transport extension names or metadata alongside
/// a DataType, and DataFusion does not provide a built-in mechanism for
/// user-defined types we have to do a bit of wrapping and unwrapping to represent
/// them in a way that can be plugged in to user-defined functions. In particular,
/// we need to be able to:
///
/// - Declare function signatures in such a way that ST_something(some_geometry)
///   can find the user-defined function implementation (or error if some_geometry
///   is not geometry).
/// - Declare an output type so that geometry can be recognized by the next ST
///   function.
///
/// Strictly speaking we don't need to use the Arrow extension type (i.e., name
/// + metadata) to do this; however, GeoArrow uses it and representing the types
/// in this way means we don't have to try very hard to integrate with geoarrow-rs
/// or geoarrow-c via FFI.
///
/// This wrapping/unwrapping can disappear when there is a built-in logical type
/// representation.
#[derive(Debug, PartialEq)]
pub struct ExtensionType {
    extension_name: String,
    storage_type: DataType,
    extension_metadata: Option<String>,
}

/// Simple logical type representation that is either a built-in Arrow data type
/// or an ExtensionType.
#[derive(Debug, PartialEq)]
pub enum LogicalType {
    Normal(DataType),
    Extension(ExtensionType),
}

impl From<DataType> for LogicalType {
    fn from(value: DataType) -> Self {
        match ExtensionType::from_data_type(&value) {
            Some(extension_type) => LogicalType::Extension(extension_type),
            None => LogicalType::Normal(value),
        }
    }
}

impl From<Field> for LogicalType {
    fn from(value: Field) -> Self {
        match ExtensionType::from_field(&value) {
            Some(extension_type) => LogicalType::Extension(extension_type),
            None => LogicalType::Normal(value.data_type().clone()),
        }
    }
}

impl From<ExtensionType> for LogicalType {
    fn from(value: ExtensionType) -> Self {
        return LogicalType::Extension(value);
    }
}

/// Simple logical array representation to handle the wrapping and unwrapping of
/// ArrayRef values
pub enum LogicalArray {
    Normal(ArrayRef),
    Extension(ExtensionType, ArrayRef),
}

impl From<ArrayRef> for LogicalArray {
    fn from(value: ArrayRef) -> LogicalArray {
        match ExtensionType::from_data_type(value.data_type()) {
            Some(extension_type) => {
                let struct_array = StructArray::from(value.to_data());
                LogicalArray::Extension(extension_type, struct_array.column(0).clone())
            }
            None => LogicalArray::Normal(value),
        }
    }
}

impl From<LogicalArray> for ArrayRef {
    fn from(value: LogicalArray) -> Self {
        match value {
            LogicalArray::Normal(array) => array,
            LogicalArray::Extension(extension_type, array) => {
                match extension_type.wrap_storage(array) {
                    Ok(wrapped) => wrapped,
                    Err(err) => panic!("{}", err),
                }
            }
        }
    }
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

    /// Wraps this ExtensionType as a Field whose data_type is the actual storage type
    ///
    /// This is how an Arrow extension type would be normally wrapped if it were a column
    /// in a RecordBatch.
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

    /// Wrap this ExtensionType as a Struct DataType
    ///
    /// This is the representation required internally until DataFusion can represent
    /// a non-standard Arrow type. This representation is a Struct that contains exactly
    /// one field whose name is the extension name and whose field metadata contains the
    /// extension name and metadata.
    pub fn to_data_type(&self) -> DataType {
        let field = self.to_field(&self.extension_name);
        return DataType::Struct(Fields::from(vec![field]));
    }

    /// Wrap storage array as a StructArray
    pub fn wrap_storage(&self, array: ArrayRef) -> Result<ArrayRef> {
        if array.data_type() != &self.storage_type {
            return internal_err!(
                "Type to wrap ({}) does not match storage type ({})",
                array.data_type(),
                &self.storage_type
            );
        }

        let wrapped = StructArray::new(
            vec![self.to_field(&self.extension_name)].into(),
            vec![array],
            None,
        );

        return Ok(Arc::new(wrapped));
    }

    /// Unwrap a Field into an ExtensionType if the field represents one
    ///
    /// Returns None if the field does not have Arrow extension metadata
    /// for the extension name. This is the inverse of to_field().
    pub fn from_field(field: &Field) -> Option<ExtensionType> {
        let metadata = field.metadata();

        match metadata.get("ARROW:extension:name") {
            Some(extension_name) => Some(ExtensionType::new(
                extension_name,
                field.data_type().clone(),
                metadata.get("ARROW:extension:metadata").cloned(),
            )),
            None => return None,
        }
    }

    /// Unwrap a DataType that is potentially an extension type wrapped in a Struct
    ///
    /// Returns None if the storage type is not a Struct, if the Struct contains
    /// any number of fields != 1, if its only field is does not contain extension
    /// metadata, or if its extension name does not match the name of the struct.
    pub fn from_data_type(storage_type: &DataType) -> Option<ExtensionType> {
        match storage_type {
            DataType::Struct(fields) => {
                if fields.len() != 1 {
                    return None;
                }

                let field = &fields[0];
                let maybe_extension_type = ExtensionType::from_field(field);
                match maybe_extension_type {
                    Some(extension_type) => {
                        if &extension_type.extension_name == field.name() {
                            Some(extension_type)
                        } else {
                            None
                        }
                    }
                    None => None,
                }
            }
            _ => return None,
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::create_array;

    use super::*;

    /// An ExtensionType for tests
    pub fn geoarrow_wkt() -> ExtensionType {
        ExtensionType::new("geoarrow.wkt", DataType::Utf8, None)
    }

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
        let ext_struct = &ext_type.to_data_type();
        match ext_struct {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].name(), "foofy");
            }
            _ => panic!("not a struct"),
        }

        match ExtensionType::from_data_type(ext_struct) {
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
    fn logical_type() {
        let logical_type_normal: LogicalType = DataType::Boolean.into();
        assert_eq!(logical_type_normal, LogicalType::Normal(DataType::Boolean));

        let logical_type_ext: LogicalType = geoarrow_wkt().into();
        assert_eq!(logical_type_ext, LogicalType::Extension(geoarrow_wkt()));

        let logical_type_ext2: LogicalType = geoarrow_wkt().to_data_type().into();
        assert_eq!(logical_type_ext2, logical_type_ext);

        let logical_type_ext3: LogicalType = geoarrow_wkt().to_field("foofy").into();
        assert_eq!(logical_type_ext3, logical_type_ext);
    }

    #[test]
    fn array_wrap_unwrap() {
        let array: ArrayRef = create_array!(Utf8, ["POINT (0 1)", "POINT (2, 3)"]);
        let logical_array: LogicalArray = array.clone().into();
        match &logical_array {
            LogicalArray::Normal(array) => assert_eq!(array.data_type(), &DataType::Utf8),
            LogicalArray::Extension(_, _) => panic!("Expected normal array!"),
        }

        let logical_array_ext = LogicalArray::Extension(geoarrow_wkt(), array);
        let wrapped_array: ArrayRef = logical_array_ext.into();
        assert!(wrapped_array.data_type().is_nested());
        let logical_array_ext_roundtrip: LogicalArray = wrapped_array.into();
        match &logical_array_ext_roundtrip {
            LogicalArray::Normal(_) => panic!("Expected extension array"),
            LogicalArray::Extension(extension_type, array) => {
                assert_eq!(extension_type, &geoarrow_wkt());
                assert_eq!(array.data_type(), &DataType::Utf8)
            }
        }
    }
}
