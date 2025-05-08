use datafusion_common::{error::Result, not_impl_err};
use datafusion_expr::ColumnarValue;
use geo_traits::{
    to_geo::{
        ToGeoLineString, ToGeoMultiLineString, ToGeoMultiPoint, ToGeoMultiPolygon, ToGeoPoint,
        ToGeoPolygon,
    },
    GeometryTrait,
    GeometryType::*,
};
use geo_types::Geometry;
use sedona_functions::executor::IterGeo;
use sedona_schema::datatypes::SedonaType;

pub fn scalar_arg_to_geometry(
    arg_type: &SedonaType,
    arg: &ColumnarValue,
) -> Result<Option<Geometry>> {
    let mut out: Option<Geometry> = None;
    arg.iter_as_wkb(arg_type, 1, |_i, maybe_item| -> Result<()> {
        match maybe_item {
            Some(item) => {
                out = Some(item_to_geometry(item)?);
            }
            None => {
                out = None;
            }
        }

        Ok(())
    })?;

    Ok(out)
}

pub fn item_to_geometry(item_a: impl GeometryTrait<T = f64>) -> Result<Geometry> {
    if let Some(geom_a) = to_geometry(item_a) {
        Ok(geom_a)
    } else {
        not_impl_err!(
            "geo kernel implementation on {}, {}, or {} not supported",
            "MULTIPOINT with EMPTY child",
            "POINT EMPTY",
            "GEOMETRYCOLLECTION"
        )
    }
}

// GeometryCollection causes issues because it has a recursive definition and won't work
// with cargo run --release. Thus, we need our own version of this that limits the
// recursion supported in a GeometryCollection. This version just disallows collections
// for now.
fn to_geometry(item: impl GeometryTrait<T = f64>) -> Option<Geometry> {
    match item.as_type() {
        Point(geom) => geom.try_to_point().map(Geometry::Point),
        LineString(geom) => Some(Geometry::LineString(geom.to_line_string())),
        Polygon(geom) => Some(Geometry::Polygon(geom.to_polygon())),
        MultiPoint(geom) => geom.try_to_multi_point().map(Geometry::MultiPoint),
        MultiLineString(geom) => Some(Geometry::MultiLineString(geom.to_multi_line_string())),
        MultiPolygon(geom) => Some(Geometry::MultiPolygon(geom.to_multi_polygon())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use geo_traits::to_geo::ToGeoGeometry;
    use rstest::rstest;
    use sedona_schema::datatypes::WKB_GEOMETRY;
    use sedona_testing::create::create_scalar_storage;
    use std::str::FromStr;
    use wkt::Wkt;

    use super::*;

    #[test]
    fn scalar_null() {
        assert!(scalar_arg_to_geometry(
            &WKB_GEOMETRY,
            &create_scalar_storage(None, &WKB_GEOMETRY).into(),
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn unsupported() {
        let err = scalar_arg_to_geometry(
            &WKB_GEOMETRY,
            &create_scalar_storage(Some("POINT EMPTY"), &WKB_GEOMETRY).into(),
        )
        .unwrap_err();
        assert!(err.message().starts_with("geo kernel implementation"));

        let err = scalar_arg_to_geometry(
            &WKB_GEOMETRY,
            &create_scalar_storage(Some("GEOMETRYCOLLECTION (POINT (1 2))"), &WKB_GEOMETRY).into(),
        )
        .unwrap_err();
        assert!(err.message().starts_with("geo kernel implementation"));
    }

    #[rstest]
    fn custom_to_geom(
        #[values(
            "POINT (0 1)",
            "LINESTRING (1 2, 3 4)",
            "POLYGON ((0 0, 1 0, 0 1, 0 0))",
            "MULTIPOINT (1 2, 3 4)",
            "MULTILINESTRING ((1 2, 3 4))",
            "MULTIPOLYGON (((0 0, 1 0, 0 1, 0 0)))"
        )]
        wkt_value: &str,
    ) {
        let geom = Wkt::<f64>::from_str(wkt_value).unwrap();
        assert_eq!(geom.to_geometry(), to_geometry(geom).unwrap())
    }
}
