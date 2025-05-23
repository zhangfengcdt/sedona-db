use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait,
};

use crate::{
    bounding_box::BoundingBox,
    error::SedonaGeometryError,
    interval::{Interval, IntervalTrait},
};

/// Calculate the Cartesian XY bounds of a well-known binary geometry blob
///
/// Note that this bounder ignores Z or M coordinates that may or may not be present
/// for applications where only the XY bounding box is needed.
pub fn wkb_bounds_xy(wkb_value: &[u8]) -> Result<BoundingBox, SedonaGeometryError> {
    let wkb =
        wkb::reader::read_wkb(wkb_value).map_err(|e| SedonaGeometryError::External(Box::new(e)))?;
    geo_traits_bounds_xy(wkb)
}

fn geo_traits_bounds_xy(
    geom: impl GeometryTrait<T = f64>,
) -> Result<BoundingBox, SedonaGeometryError> {
    let mut x = Interval::empty();
    let mut y = Interval::empty();
    geo_traits_update_xy_bounds(geom, &mut x, &mut y)?;
    Ok(BoundingBox::xy(x, y))
}

fn geo_traits_update_xy_bounds(
    geom: impl GeometryTrait<T = f64>,
    x: &mut Interval,
    y: &mut Interval,
) -> Result<(), SedonaGeometryError> {
    match geom.as_type() {
        GeometryType::Point(pt) => {
            if let Some(coord) = PointTrait::coord(pt) {
                x.update_value(coord.x());
                y.update_value(coord.y());
            }
        }
        GeometryType::LineString(ls) => {
            for coord in ls.coords() {
                x.update_value(coord.x());
                y.update_value(coord.y());
            }
        }
        GeometryType::Polygon(pl) => {
            if let Some(exterior) = pl.exterior() {
                for coord in exterior.coords() {
                    x.update_value(coord.x());
                    y.update_value(coord.y());
                }
            }

            for interior in pl.interiors() {
                for coord in interior.coords() {
                    x.update_value(coord.x());
                    y.update_value(coord.y());
                }
            }
        }
        GeometryType::MultiPoint(multi_pt) => {
            for pt in multi_pt.points() {
                geo_traits_update_xy_bounds(pt, x, y)?;
            }
        }
        GeometryType::MultiLineString(multi_ls) => {
            for ls in multi_ls.line_strings() {
                geo_traits_update_xy_bounds(ls, x, y)?;
            }
        }
        GeometryType::MultiPolygon(multi_pl) => {
            for pl in multi_pl.polygons() {
                geo_traits_update_xy_bounds(pl, x, y)?;
            }
        }
        GeometryType::GeometryCollection(collection) => {
            for geom in collection.geometries() {
                geo_traits_update_xy_bounds(geom, x, y)?;
            }
        }
        _ => {
            return Err(SedonaGeometryError::Invalid(
                "GeometryType not supported for XY bounds".to_string(),
            ))
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {

    use super::*;
    use std::str::FromStr;
    use wkt::Wkt;

    pub fn wkt_bounds_xy(wkt_value: &str) -> Result<BoundingBox, SedonaGeometryError> {
        let wkt: Wkt =
            Wkt::from_str(wkt_value).map_err(|e| SedonaGeometryError::Invalid(e.to_string()))?;
        geo_traits_bounds_xy(wkt)
    }

    #[test]
    fn test_wkt_bounds_xy() {
        assert_eq!(
            wkt_bounds_xy("POINT EMPTY").unwrap(),
            BoundingBox::xy(Interval::empty(), Interval::empty())
        );
        assert_eq!(
            wkt_bounds_xy("POINT (0 1)").unwrap(),
            BoundingBox::xy((0, 0), (1, 1))
        );
        assert_eq!(
            wkt_bounds_xy("LINESTRING (0 1, 2 3)").unwrap(),
            BoundingBox::xy((0, 2), (1, 3))
        );
        assert_eq!(
            wkt_bounds_xy("POLYGON ((0 1, 0 2, 1 1, 0 1))").unwrap(),
            BoundingBox::xy((0, 1), (1, 2))
        );
        // Not a well-behaved polygon (interior rings outside the exterior rings) but
        // we need to test that the interior rings are considered in the bounding
        assert_eq!(
            wkt_bounds_xy("POLYGON ((0 1, 0 2, 1 1, 0 1), (10 11, 11 11, 10 12, 10 11))").unwrap(),
            BoundingBox::xy((0, 11), (1, 12))
        );

        assert_eq!(
            wkt_bounds_xy("MULTIPOINT (0 1, 2 3)").unwrap(),
            BoundingBox::xy((0, 2), (1, 3))
        );

        assert_eq!(
            wkt_bounds_xy("MULTILINESTRING ((0 1, 2 3))").unwrap(),
            BoundingBox::xy((0, 2), (1, 3))
        );
        assert_eq!(
            wkt_bounds_xy("MULTIPOLYGON (((0 1, 0 2, 1 1, 0 1)))").unwrap(),
            BoundingBox::xy((0, 1), (1, 2))
        );

        assert_eq!(
            wkt_bounds_xy("GEOMETRYCOLLECTION (POINT (0 1), POINT (2 3))").unwrap(),
            BoundingBox::xy((0, 2), (1, 3))
        );
    }

    #[test]
    fn test_wkt_bounds_xy_with_zm_input() {
        // Ensure Z/M/ZM values don't cause an error and are ignored
        assert_eq!(
            wkt_bounds_xy("LINESTRING Z (0 1 2, 3 4 5)").unwrap(),
            BoundingBox::xy((0, 3), (1, 4))
        );

        assert_eq!(
            wkt_bounds_xy("LINESTRING M (0 1 2, 3 4 5)").unwrap(),
            BoundingBox::xy((0, 3), (1, 4))
        );

        assert_eq!(
            wkt_bounds_xy("LINESTRING ZM (0 1 2 3, 4 5 6 7)").unwrap(),
            BoundingBox::xy((0, 4), (1, 5))
        );
    }

    #[test]
    fn test_wkb_bounds_xy() {
        let wkt: Wkt = Wkt::from_str("POINT (0 1)").unwrap();
        let mut out = Vec::new();
        wkb::writer::write_geometry(&mut out, &wkt, wkb::Endianness::LittleEndian).unwrap();
        assert_eq!(
            wkb_bounds_xy(&out).unwrap(),
            BoundingBox::xy((0, 0), (1, 1))
        );
    }
}
