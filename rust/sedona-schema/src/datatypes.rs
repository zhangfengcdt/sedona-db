use datafusion::common::{internal_err, not_impl_err};
use datafusion::error::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use serde_json::Value;
use std::fmt::Debug;
use std::sync::Arc;

use arrow_schema::DataType;

use crate::extension_type::ExtensionType;

/// Data types supported by Sedona that resolve to a concrete Arrow DataType
#[derive(Debug, PartialEq, Clone)]
pub enum SedonaPhysicalType {
    Arrow(DataType),
    Wkb(Edges, Crs),
}

/// Edge interpolations
///
/// While at the logical level we refer to geometries and geographies, at the execution
/// layer we can reuse implementations for structural manipulation more efficiently if
/// we consider the edge interpolation as a parameter of the physical type. This maps to
/// the concept of "edges" in GeoArrow and "algorithm" in Parquet and Iceberg (where the
/// planar case would be resolved to a geometry instead of a geography).
#[derive(Debug, PartialEq, Clone)]
pub enum Edges {
    Planar,
    Spherical,
}

/// Sentinel for [`SedonaPhysicalType::Wkb`] with planar edges
///
/// This constant is useful when defining type signatures as these ignore the Crs when
/// matching (and `SedonaPhysicalType::Wkb(...)` is verbose)
pub const WKB_GEOMETRY: SedonaPhysicalType = SedonaPhysicalType::Wkb(Edges::Planar, Crs::None);

/// Sentinel for [`SedonaPhysicalType::Wkb`] with spherical edges
///
/// This constant is useful when defining type signatures as these ignore the Crs when
/// matching (and `SedonaPhysicalType::Wkb(...)` is verbose)
pub const WKB_GEOGRAPHY: SedonaPhysicalType = SedonaPhysicalType::Wkb(Edges::Spherical, Crs::None);

/// Longitude/latitude CRS (WGS84)
///
/// A [`CrsRef`] that matches EPSG:4326 or OGC:CRS84.
pub fn lnglat() -> Crs {
    LngLat::crs()
}

/// Coordinate reference systems
///
/// From Sedona's perspective, we can have a missing Crs at the type level or
/// something that can resolve to a JSON value (which is what we need to export it
/// to GeoArrow), something that can resolve to a PROJJSON value (which is what we need
/// to export it to Parquet/Iceberg) and something with which we can check
/// equality (for binary operators).
pub type Crs = Option<Arc<dyn CoordinateReferenceSystem + Send + Sync>>;

impl PartialEq<dyn CoordinateReferenceSystem + Send + Sync>
    for dyn CoordinateReferenceSystem + Send + Sync
{
    fn eq(&self, other: &Self) -> bool {
        self.crs_equals(other)
    }
}

/// Coordinate reference system abstraction
///
/// A trait defining the minimum required properties of a concrete coordinate
/// reference system, allowing the details of this to be implemented elsewhere.
pub trait CoordinateReferenceSystem: Debug {
    fn to_json(&self) -> String;
    fn to_authority_code(&self) -> Result<String>;
    fn crs_equals(&self, other: &dyn CoordinateReferenceSystem) -> bool;
}

// Implementation details

impl TryFrom<&DataType> for SedonaPhysicalType {
    type Error = DataFusionError;

    fn try_from(value: &DataType) -> Result<Self> {
        SedonaPhysicalType::from_data_type(value)
    }
}

impl TryFrom<DataType> for SedonaPhysicalType {
    type Error = DataFusionError;

    fn try_from(value: DataType) -> Result<Self> {
        SedonaPhysicalType::from_data_type(&value)
    }
}

impl From<&SedonaPhysicalType> for DataType {
    fn from(value: &SedonaPhysicalType) -> Self {
        value.data_type()
    }
}

impl From<SedonaPhysicalType> for DataType {
    fn from(value: SedonaPhysicalType) -> Self {
        value.data_type()
    }
}

impl SedonaPhysicalType {
    /// Given a data type, return the appropriate SedonaPhysicalType
    ///
    /// This is expected to be the "wrapped" version of an extension type.
    pub fn from_data_type(data_type: &DataType) -> Result<SedonaPhysicalType> {
        match ExtensionType::from_data_type(data_type) {
            Some(ext) => {
                let (edges, crs) = deserialize_edges_and_crs(&ext.extension_metadata)?;
                if ext.extension_name == "geoarrow.wkb" {
                    physical_type_wkb(edges, crs, ext.storage_type)
                } else {
                    internal_err!(
                        "Extension type not implemented: <{}>:{}",
                        ext.extension_name,
                        ext.storage_type
                    )
                }
            }
            None => Ok(Self::Arrow(data_type.clone())),
        }
    }

    /// Compute the Arrow data type used to represent this physical type in DataFusion
    pub fn data_type(&self) -> DataType {
        match &self {
            SedonaPhysicalType::Arrow(data_type) => data_type.clone(),
            SedonaPhysicalType::Wkb(edges, crs) => ExtensionType::new(
                "geoarrow.wkb",
                DataType::Binary,
                Some(serialize_edges_and_crs(edges, crs)),
            )
            .to_data_type(),
        }
    }

    /// Returns True if another physical type matches this one for the purposes of dispatch
    ///
    /// For Arrow types this matches on type equality; for other type it matches on edges
    /// but not crs.
    pub fn match_signature(&self, other: &SedonaPhysicalType) -> bool {
        match (self, other) {
            (SedonaPhysicalType::Arrow(data_type), SedonaPhysicalType::Arrow(other_data_type)) => {
                data_type == other_data_type
            }
            (SedonaPhysicalType::Wkb(edges, _), SedonaPhysicalType::Wkb(other_edges, _)) => {
                edges == other_edges
            }
            _ => false,
        }
    }

    pub fn wrap_arg(&self, arg: &ColumnarValue) -> Result<ColumnarValue> {
        match self {
            SedonaPhysicalType::Arrow(_) => Ok(arg.clone()),
            SedonaPhysicalType::Wkb(edges, crs) => ExtensionType::new(
                "geoarrow.wkb",
                DataType::Binary,
                Some(serialize_edges_and_crs(edges, crs)),
            )
            .wrap_arg(arg),
        }
    }

    pub fn unwrap_arg(&self, arg: &ColumnarValue) -> Result<ColumnarValue> {
        match self {
            SedonaPhysicalType::Arrow(_) => Ok(arg.clone()),
            SedonaPhysicalType::Wkb(_, _) => ExtensionType::unwrap_arg(arg),
        }
    }
}

// Implementation details for importing/exporting types from/to Arrow + metadata

/// Check a storage type for SedonaPhysicalType::Wkb
fn physical_type_wkb(edges: Edges, crs: Crs, storage_type: DataType) -> Result<SedonaPhysicalType> {
    if let DataType::Binary = storage_type {
        Ok(SedonaPhysicalType::Wkb(edges, crs))
    } else {
        internal_err!(
            "Expected Wkb type with Binary storage but got {}",
            storage_type
        )
    }
}

/// Parse a GeoArrow metadata string
///
/// Deserializes the extension metadata from a GeoArrow extension type. See
/// https://geoarrow.org/extension-types.html for a full definition of the metadata
/// format.
fn deserialize_edges_and_crs(value: &Option<String>) -> Result<(Edges, Crs)> {
    match value {
        Some(val) => {
            if val.is_empty() || val == "{}" {
                return Ok((Edges::Planar, Crs::None));
            }

            let json_value: Value = serde_json::from_str(val).map_err(|err| {
                DataFusionError::Internal(format!("Error deserializing GeoArrow metadata: {}", err))
            })?;
            if !json_value.is_object() {
                return internal_err!("Expected GeoArrow metadata as JSON object but got {}", val);
            }

            let edges = match json_value.get("edges") {
                Some(edges_value) => deserialize_edges(edges_value)?,
                None => Edges::Planar,
            };

            let crs = match json_value.get("crs") {
                Some(crs_value) => deserialize_crs(crs_value)?,
                None => Crs::None,
            };

            Ok((edges, crs))
        }
        None => Ok((Edges::Planar, Crs::None)),
    }
}

/// Create a GeoArrow metadata string
///
/// Deserializes the extension metadata from a GeoArrow extension type. See
/// https://geoarrow.org/extension-types.html for a full definition of the metadata
/// format.
fn serialize_edges_and_crs(edges: &Edges, crs: &Crs) -> String {
    let crs_component = crs
        .as_ref()
        .map(|crs| format!(r#""crs":{}"#, crs.to_json()));

    let edges_component = match edges {
        Edges::Planar => None,
        Edges::Spherical => Some(r#""edges":"spherical""#),
    };

    match (crs_component, edges_component) {
        (None, None) => "".to_string(),
        (None, Some(edges)) => format!("{{{}}}", edges),
        (Some(crs), None) => format!("{{{}}}", crs),
        (Some(crs), Some(edges)) => format!("{{{},{}}}", edges, crs),
    }
}

/// Deserialize a specific GeoArrow "edges" value
fn deserialize_edges(edges: &Value) -> Result<Edges> {
    match edges.as_str() {
        Some(edges_str) => {
            if edges_str == "planar" {
                Ok(Edges::Planar)
            } else if edges_str == "spherical" {
                Ok(Edges::Spherical)
            } else {
                internal_err!("Unsupported edges value {}", edges_str)
            }
        }
        None => {
            internal_err!("Unsupported edges JSON type in metadata {}", edges)
        }
    }
}

/// Deserialize a specific GeoArrow "crs" value
fn deserialize_crs(crs: &Value) -> Result<Crs> {
    if LngLat::is_lnglat(crs) {
        return Ok(lnglat());
    }

    not_impl_err!("Deserialization of crs {}", crs)
}

/// Concrete implementation of a default longitude/latitude coordinate reference system
#[derive(Debug)]
struct LngLat {}

impl LngLat {
    pub fn crs() -> Crs {
        Crs::Some(Arc::new(LngLat {}))
    }

    pub fn is_lnglat(value: &Value) -> bool {
        if let Some(string_value) = value.as_str() {
            if string_value == "OGC:CRS84" || string_value == "EPSG:4326" {
                return true;
            }
        }

        // TODO: check id.authority + id.code
        false
    }
}

impl CoordinateReferenceSystem for LngLat {
    fn to_json(&self) -> String {
        // We could return a full-on PROJJSON string here but this string gets copied
        // a fair amount
        "\"OGC:CRS84\"".to_string()
    }

    fn to_authority_code(&self) -> Result<String> {
        Ok("OGC:CRS84".to_string())
    }

    fn crs_equals(&self, other: &dyn CoordinateReferenceSystem) -> bool {
        if let Ok(auth_code) = other.to_authority_code() {
            auth_code == "OGC:CRS84" || auth_code == "EPSG:4326"
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn physical_type_arrow() -> Result<()> {
        let physical_type = SedonaPhysicalType::from_data_type(&DataType::Int32)?;
        assert_eq!(physical_type.data_type(), DataType::Int32);
        assert_eq!(physical_type, SedonaPhysicalType::Arrow(DataType::Int32));
        assert!(physical_type.match_signature(&SedonaPhysicalType::Arrow(DataType::Int32)));
        assert!(!physical_type.match_signature(&SedonaPhysicalType::Arrow(DataType::Utf8)));

        Ok(())
    }

    #[test]
    fn physical_type_wkb() -> Result<()> {
        assert_eq!(WKB_GEOMETRY, WKB_GEOMETRY);

        assert!(WKB_GEOMETRY.data_type().is_nested());
        assert_eq!(
            SedonaPhysicalType::from_data_type(&WKB_GEOMETRY.data_type())?,
            WKB_GEOMETRY
        );

        assert!(WKB_GEOMETRY.match_signature(&WKB_GEOMETRY));
        Ok(())
    }

    #[test]
    fn physical_type_wkb_geography() -> Result<()> {
        assert_eq!(WKB_GEOGRAPHY, WKB_GEOGRAPHY);

        assert!(WKB_GEOGRAPHY.data_type().is_nested());
        assert_eq!(
            SedonaPhysicalType::from_data_type(&WKB_GEOGRAPHY.data_type())?,
            WKB_GEOGRAPHY
        );

        assert!(WKB_GEOGRAPHY.match_signature(&WKB_GEOGRAPHY));
        Ok(())
    }

    #[test]
    fn geoarrow_serialize() {
        assert_eq!(serialize_edges_and_crs(&Edges::Planar, &Crs::None), "");
        assert_eq!(
            serialize_edges_and_crs(&Edges::Planar, &lnglat()),
            r#"{"crs":"OGC:CRS84"}"#
        );
        assert_eq!(
            serialize_edges_and_crs(&Edges::Spherical, &Crs::None),
            r#"{"edges":"spherical"}"#
        );
        assert_eq!(
            serialize_edges_and_crs(&Edges::Spherical, &lnglat()),
            r#"{"edges":"spherical","crs":"OGC:CRS84"}"#
        );
    }

    #[test]
    fn geoarrow_serialize_roundtrip() -> Result<()> {
        // Check configuration resulting in empty metadata
        assert_eq!(
            deserialize_edges_and_crs(&Some(serialize_edges_and_crs(&Edges::Planar, &Crs::None)))?,
            (Edges::Planar, Crs::None)
        );

        // Check configuration with non-empty metadata for both edges and crs
        assert_eq!(
            deserialize_edges_and_crs(&Some(serialize_edges_and_crs(
                &Edges::Spherical,
                &lnglat()
            )))?,
            (Edges::Spherical, lnglat())
        );

        Ok(())
    }

    #[test]
    fn geoarrow_deserialize_invalid() {
        let bad_json =
            ExtensionType::new("geoarrow.wkb", DataType::Binary, Some(r#"{"#.to_string()))
                .to_data_type();
        assert!(SedonaPhysicalType::from_data_type(&bad_json)
            .unwrap_err()
            .message()
            .contains("Error deserializing GeoArrow metadata"));

        let bad_type =
            ExtensionType::new("geoarrow.wkb", DataType::Binary, Some(r#"[]"#.to_string()))
                .to_data_type();
        assert!(SedonaPhysicalType::from_data_type(&bad_type)
            .unwrap_err()
            .message()
            .contains("Expected GeoArrow metadata as JSON object"));

        let bad_edges_type = ExtensionType::new(
            "geoarrow.wkb",
            DataType::Binary,
            Some(r#"{"edges": []}"#.to_string()),
        )
        .to_data_type();
        assert!(SedonaPhysicalType::from_data_type(&bad_edges_type)
            .unwrap_err()
            .message()
            .contains("Unsupported edges JSON type"));

        let bad_edges_value = ExtensionType::new(
            "geoarrow.wkb",
            DataType::Binary,
            Some(r#"{"edges": "gazornenplat"}"#.to_string()),
        )
        .to_data_type();
        assert!(SedonaPhysicalType::from_data_type(&bad_edges_value)
            .unwrap_err()
            .message()
            .contains("Unsupported edges value"));
    }

    #[test]
    fn crs_lnglat() {
        let lnglat = LngLat {};
        assert!(lnglat.crs_equals(&lnglat));
        assert_eq!(lnglat.to_authority_code().unwrap(), "OGC:CRS84");

        let json_value = lnglat.to_json();
        assert_eq!(json_value, "\"OGC:CRS84\"");

        let lnglat_crs = LngLat::crs();
        assert_eq!(lnglat_crs, lnglat_crs);
        assert_ne!(lnglat_crs, Crs::None);

        let lnglat_parsed: Result<Value, _> = serde_json::from_str(&json_value);
        assert!(LngLat::is_lnglat(&lnglat_parsed.unwrap()))
    }
}
