use datafusion_common::{DataFusionError, Result};
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

use serde_json::Value;

/// Deserialize a specific GeoArrow "crs" value
///
/// Currently the only CRS types supported are PROJJSON and authority:code
/// where the authority and code are well-known lon/lat (WGS84) codes.
pub fn deserialize_crs(crs: &Value) -> Result<Crs> {
    if crs.is_null() {
        return Ok(None);
    }

    // Handle JSON strings "OGC:CRS84" and "EPSG:4326"
    if LngLat::is_lnglat(crs) {
        return Ok(lnglat());
    }

    let projjson = if let Some(string) = crs.as_str() {
        // Handle the geopandas bug where it exported stringified projjson.
        // This could in the future handle other stringified versions with some
        // auto detection logic.
        string.parse()?
    } else {
        // Handle the case where Value is already an object
        ProjJSON::try_new(crs.clone())?
    };

    Ok(Some(Arc::new(projjson)))
}

/// Longitude/latitude CRS (WGS84)
///
/// A [`Crs`] that matches EPSG:4326 or OGC:CRS84.
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
    fn to_authority_code(&self) -> Result<Option<String>>;
    fn crs_equals(&self, other: &dyn CoordinateReferenceSystem) -> bool;
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

    fn to_authority_code(&self) -> Result<Option<String>> {
        Ok(Some("OGC:CRS84".to_string()))
    }

    fn crs_equals(&self, other: &dyn CoordinateReferenceSystem) -> bool {
        if let Ok(Some(auth_code)) = other.to_authority_code() {
            auth_code == "OGC:CRS84" || auth_code == "EPSG:4326"
        } else {
            false
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ProjJSON {
    value: Value,
}

impl FromStr for ProjJSON {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        let value: Value = serde_json::from_str(s).map_err(|err| {
            DataFusionError::Internal(format!("Error deserializing PROJJSON Crs: {}", err))
        })?;

        Self::try_new(value)
    }
}

impl ProjJSON {
    pub fn try_new(value: Value) -> Result<Self> {
        if !value.is_object() {
            return Err(DataFusionError::Internal(format!(
                "Can't create PROJJSON from non-object: {}",
                value
            )));
        }

        Ok(Self { value })
    }
}

impl CoordinateReferenceSystem for ProjJSON {
    fn to_json(&self) -> String {
        self.value.to_string()
    }

    fn to_authority_code(&self) -> Result<Option<String>> {
        if let Some(identifier) = self.value.get("id") {
            let maybe_authority = identifier.get("authority").map(|v| v.as_str());
            let maybe_code = identifier.get("code").map(|v| {
                if let Some(string) = v.as_str() {
                    Some(string.to_string())
                } else {
                    v.as_number().map(|number| number.to_string())
                }
            });
            if let (Some(Some(authority)), Some(Some(code))) = (maybe_authority, maybe_code) {
                return Ok(Some(format!("{authority}:{code}")));
            }
        }

        Ok(None)
    }

    fn crs_equals(&self, other: &dyn CoordinateReferenceSystem) -> bool {
        if let (Ok(Some(auth_code)), Ok(Some(other_auth_code))) =
            (self.to_authority_code(), other.to_authority_code())
        {
            auth_code == other_auth_code
        } else if let Ok(other_value) = serde_json::from_str::<Value>(&other.to_json()) {
            self.value == other_value
        } else {
            false
        }
    }
}

pub const OGC_CRS84_PROJJSON: &str = r#"{"$schema":"https://proj.org/schemas/v0.7/projjson.schema.json","type":"GeographicCRS","name":"WGS 84 (CRS84)","datum_ensemble":{"name":"World Geodetic System 1984 ensemble","members":[{"name":"World Geodetic System 1984 (Transit)","id":{"authority":"EPSG","code":1166}},{"name":"World Geodetic System 1984 (G730)","id":{"authority":"EPSG","code":1152}},{"name":"World Geodetic System 1984 (G873)","id":{"authority":"EPSG","code":1153}},{"name":"World Geodetic System 1984 (G1150)","id":{"authority":"EPSG","code":1154}},{"name":"World Geodetic System 1984 (G1674)","id":{"authority":"EPSG","code":1155}},{"name":"World Geodetic System 1984 (G1762)","id":{"authority":"EPSG","code":1156}},{"name":"World Geodetic System 1984 (G2139)","id":{"authority":"EPSG","code":1309}},{"name":"World Geodetic System 1984 (G2296)","id":{"authority":"EPSG","code":1383}}],"ellipsoid":{"name":"WGS 84","semi_major_axis":6378137,"inverse_flattening":298.257223563},"accuracy":"2.0","id":{"authority":"EPSG","code":6326}},"coordinate_system":{"subtype":"ellipsoidal","axis":[{"name":"Geodetic longitude","abbreviation":"Lon","direction":"east","unit":"degree"},{"name":"Geodetic latitude","abbreviation":"Lat","direction":"north","unit":"degree"}]},"scope":"Not known.","area":"World.","bbox":{"south_latitude":-90,"west_longitude":-180,"north_latitude":90,"east_longitude":180},"id":{"authority":"OGC","code":"CRS84"}}"#;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn deserialize() {
        let value_auth_code_4326: Value = serde_json::from_str(r#""EPSG:4326""#).unwrap();
        assert_eq!(deserialize_crs(&value_auth_code_4326).unwrap(), lnglat());

        let value_auth_code_crs84: Value = serde_json::from_str(r#""OGC:CRS84""#).unwrap();
        assert_eq!(deserialize_crs(&value_auth_code_crs84).unwrap(), lnglat());

        let value_projjson: Value = serde_json::from_str(OGC_CRS84_PROJJSON).unwrap();
        assert_eq!(deserialize_crs(&value_projjson).unwrap(), lnglat());

        let value_gross_unescaped = Value::String(OGC_CRS84_PROJJSON.to_string());
        assert_eq!(deserialize_crs(&value_gross_unescaped).unwrap(), lnglat());

        assert!(deserialize_crs(&Value::Null).unwrap().is_none());

        let value_unsupported: Value = serde_json::from_str("[]").unwrap();
        assert!(deserialize_crs(&value_unsupported).is_err());
    }

    #[test]
    fn crs_lnglat() {
        let lnglat = LngLat {};
        assert!(lnglat.crs_equals(&lnglat));
        assert_eq!(lnglat.to_authority_code().unwrap().unwrap(), "OGC:CRS84");

        let json_value = lnglat.to_json();
        assert_eq!(json_value, "\"OGC:CRS84\"");

        let lnglat_crs = LngLat::crs();
        assert_eq!(lnglat_crs, lnglat_crs);
        assert_ne!(lnglat_crs, Crs::None);

        let lnglat_parsed: Result<Value, _> = serde_json::from_str(&json_value);
        assert!(LngLat::is_lnglat(&lnglat_parsed.unwrap()))
    }

    #[test]
    fn crs_projjson() {
        let projjson = OGC_CRS84_PROJJSON.parse::<ProjJSON>().unwrap();
        assert_eq!(projjson.to_authority_code().unwrap().unwrap(), "OGC:CRS84");

        let json_value: Value = serde_json::from_str(OGC_CRS84_PROJJSON).unwrap();
        let json_value_roundtrip: Value = serde_json::from_str(&projjson.to_json()).unwrap();
        assert_eq!(json_value_roundtrip, json_value);

        assert!(projjson.crs_equals(&LngLat {}));
        assert!(projjson.crs_equals(&projjson));

        let projjson_without_identifier = "{}".parse::<ProjJSON>().unwrap();
        assert!(projjson_without_identifier
            .to_authority_code()
            .unwrap()
            .is_none());
        assert!(!projjson.crs_equals(&projjson_without_identifier))
    }
}
