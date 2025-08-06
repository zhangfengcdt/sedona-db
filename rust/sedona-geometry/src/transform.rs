use std::io::Write;

use crate::bounding_box::BoundingBox;
use crate::error::SedonaGeometryError;
use crate::wkb_factory::{
    write_wkb_coord, write_wkb_empty_point, write_wkb_geometrycollection_header,
    write_wkb_linestring_header, write_wkb_multilinestring_header, write_wkb_multipoint_header,
    write_wkb_multipolygon_header, write_wkb_point_header, write_wkb_polygon_header,
    write_wkb_polygon_ring_header,
};
use geo_traits::{
    CoordTrait, Dimensions, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait,
};

/// Represents a coordinate reference system (CRS) transformation engine.
pub trait CrsEngine: Send + Sync {
    fn get_transform_crs_to_crs(
        &self,
        from: &str,
        to: &str,
        area_of_interest: Option<BoundingBox>,
        options: &str,
    ) -> Result<Box<dyn CrsTransform>, SedonaGeometryError>;
    fn get_transform_pipeline(
        &self,
        pipeline: &str,
        options: &str,
    ) -> Result<Box<dyn CrsTransform>, SedonaGeometryError>;
}

/// Trait for transforming coordinates in a geometry from one CRS to another.
pub trait CrsTransform: std::fmt::Debug {
    fn transform_coord(&mut self, coord: &mut (f64, f64)) -> Result<(), SedonaGeometryError>;
}

/// A boxed trait object for dynamic dispatch of CRS transformations.
impl CrsTransform for Box<dyn CrsTransform> {
    fn transform_coord(&mut self, coord: &mut (f64, f64)) -> Result<(), SedonaGeometryError> {
        self.as_mut().transform_coord(coord)
    }
}

/// Transforms a geometry from one CRS to another using the provided transformation.
pub fn transform(
    geom: impl GeometryTrait<T = f64>,
    trans: &mut dyn CrsTransform,
    out: &mut impl Write,
) -> Result<(), SedonaGeometryError> {
    let dims = geom.dim();
    match geom.as_type() {
        GeometryType::Point(pt) => {
            if pt.coord().is_some() {
                write_wkb_point_header(out, dims)?;
                transform_and_write_coords(out, trans, pt.coord().into_iter())?;
            } else {
                write_wkb_empty_point(out, dims)?;
            }
        }
        GeometryType::LineString(ls) => {
            write_wkb_linestring_header(out, ls.dim(), ls.coords().count())?;
            transform_and_write_coords(out, trans, ls.coords())?;
        }
        GeometryType::Polygon(pl) => {
            let num_rings = pl.interiors().count() + pl.exterior().is_some() as usize;
            write_wkb_polygon_header(out, pl.dim(), num_rings)?;

            if let Some(exterior) = pl.exterior() {
                transform_and_write_ring(out, trans, exterior)?;
            }

            for interior in pl.interiors() {
                transform_and_write_ring(out, trans, interior)?;
            }
        }
        GeometryType::MultiPoint(multi_pt) => {
            write_wkb_multipoint_header(out, dims, multi_pt.points().count())?;
            for pt in multi_pt.points() {
                transform(pt, trans, out)?;
            }
        }
        GeometryType::MultiLineString(multi_ls) => {
            write_wkb_multilinestring_header(out, dims, multi_ls.line_strings().count())?;
            for ls in multi_ls.line_strings() {
                transform(ls, trans, out)?;
            }
        }
        GeometryType::MultiPolygon(multi_pl) => {
            write_wkb_multipolygon_header(out, dims, multi_pl.polygons().count())?;
            for pl in multi_pl.polygons() {
                transform(pl, trans, out)?;
            }
        }
        GeometryType::GeometryCollection(collection) => {
            write_wkb_geometrycollection_header(out, dims, collection.geometries().count())?;
            for geom in collection.geometries() {
                transform(geom, trans, out)?;
            }
        }
        _ => {
            return Err(SedonaGeometryError::Invalid(
                "GeometryType not supported for transform".to_string(),
            ))
        }
    }

    Ok(())
}

fn transform_and_write_ring<'a, L>(
    buf: &mut impl Write,
    trans: &mut dyn CrsTransform,
    ring: L,
) -> Result<(), SedonaGeometryError>
where
    L: LineStringTrait<T = f64> + 'a,
{
    let num_points = ring.coords().count();
    write_wkb_polygon_ring_header(buf, num_points)?;
    transform_and_write_coords(buf, trans, ring.coords())?;
    Ok(())
}

fn transform_and_write_coords<'a, C, I>(
    buf: &mut impl Write,
    trans: &mut dyn CrsTransform,
    coords: I,
) -> Result<(), SedonaGeometryError>
where
    C: CoordTrait<T = f64> + 'a,
    I: Iterator<Item = C>,
{
    for coord in coords {
        let mut xy: (f64, f64) = (coord.x(), coord.y());
        trans.transform_coord(&mut xy)?;

        match coord.dim() {
            Dimensions::Xy => {
                write_wkb_coord(buf, (xy.0, xy.1))?;
            }
            Dimensions::Xyz => {
                write_wkb_coord(buf, (xy.0, xy.1, coord.nth_or_panic(2)))?;
            }
            Dimensions::Xym => {
                write_wkb_coord(buf, (xy.0, xy.1, coord.nth_or_panic(2)))?;
            }
            Dimensions::Xyzm => {
                write_wkb_coord(
                    buf,
                    (xy.0, xy.1, coord.nth_or_panic(2), coord.nth_or_panic(3)),
                )?;
            }
            _ => {
                return Err(SedonaGeometryError::Invalid(
                    "Unsupported dimensions for coordinate transformation".to_string(),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use std::str::FromStr;
    use wkb::reader::read_wkb;
    use wkt::Wkt;

    #[derive(Debug)]
    struct MockTransform {}
    impl CrsTransform for MockTransform {
        fn transform_coord(&mut self, coord: &mut (f64, f64)) -> Result<(), SedonaGeometryError> {
            coord.0 += 10.0;
            coord.1 += 20.0;
            Ok(())
        }
    }

    #[test]
    fn test_transform_point() {
        let point = geo_types::Point::new(1.0, 2.0);
        test_transform(point, "POINT(11 22)");

        let nan_point = geo_types::Point::new(f64::NAN, f64::NAN);
        test_transform(nan_point, "POINT EMPTY");
    }

    #[test]
    fn test_transform_linestring() {
        let linestring_xy = geo_types::LineString::from(vec![(1.0, 2.0), (3.0, 4.0)]);
        test_transform(linestring_xy, "LINESTRING(11 22,13 24)");

        let empty_linestring = geo_types::LineString::new(vec![]);
        test_transform(empty_linestring, "LINESTRING EMPTY");
    }

    #[test]
    fn test_transform_polygon() {
        let polygon = geo_types::Polygon::new(
            geo_types::LineString::from(vec![(1.0, 2.0), (3.0, 4.0), (5.0, 6.0), (7.0, 8.0)]),
            vec![],
        );
        test_transform(polygon, "POLYGON((11 22,13 24,15 26,17 28,11 22))");

        let polygon_multi_rings = geo_types::Polygon::new(
            geo_types::LineString::from(vec![(1.0, 2.0), (3.0, 4.0), (5.0, 6.0), (7.0, 8.0)]),
            vec![geo_types::LineString::from(vec![
                (9.0, 10.0),
                (11.0, 12.0),
                (13.0, 14.0),
                (15.0, 16.0),
            ])],
        );
        test_transform(
            polygon_multi_rings,
            "POLYGON((11 22,13 24,15 26,17 28,11 22),(19 30,21 32,23 34,25 36,19 30))",
        );

        let empty_polygon = geo_types::Polygon::new(geo_types::LineString::new(vec![]), vec![]);
        test_transform(empty_polygon, "POLYGON EMPTY");
    }

    #[test]
    fn test_transform_multipoint() {
        let multipoint = geo_types::MultiPoint::from(vec![
            geo_types::Point::new(1.0, 2.0),
            geo_types::Point::new(3.0, 4.0),
        ]);
        test_transform(multipoint, "MULTIPOINT((11 22),(13 24))");

        let empty_multipoint = geo_types::MultiPoint::new(vec![]);
        test_transform(empty_multipoint, "MULTIPOINT EMPTY");
    }

    #[test]
    fn test_transform_multilinestring() {
        let multilinestring = geo_types::MultiLineString(vec![
            geo_types::LineString::from(vec![(1.0, 2.0), (3.0, 4.0)]),
            geo_types::LineString::from(vec![(5.0, 6.0), (7.0, 8.0)]),
        ]);
        test_transform(
            multilinestring,
            "MULTILINESTRING((11 22,13 24),(15 26,17 28))",
        );

        let empty_multilinestring = geo_types::MultiLineString::new(vec![]);
        test_transform(empty_multilinestring, "MULTILINESTRING EMPTY");
    }

    #[test]
    fn test_transform_multipolygon() {
        let multipolygon = geo_types::MultiPolygon(vec![
            geo_types::Polygon::new(
                geo_types::LineString::from(vec![(1.0, 2.0), (3.0, 4.0), (5.0, 6.0), (7.0, 8.0)]),
                vec![],
            ),
            geo_types::Polygon::new(
                geo_types::LineString::from(vec![
                    (9.0, 10.0),
                    (11.0, 12.0),
                    (13.0, 14.0),
                    (15.0, 16.0),
                ]),
                vec![],
            ),
        ]);
        test_transform(
            multipolygon,
            "MULTIPOLYGON(((11 22,13 24,15 26,17 28,11 22)),((19 30,21 32,23 34,25 36,19 30)))",
        );

        let empty_multipolygon = geo_types::MultiPolygon::new(vec![]);
        test_transform(empty_multipolygon, "MULTIPOLYGON EMPTY");
    }

    #[test]
    fn test_transform_geometrycollection() {
        let geometry_collection = geo_types::GeometryCollection::from(vec![
            geo_types::Geometry::Point(geo_types::Point::new(1.0, 2.0)),
            geo_types::Geometry::LineString(geo_types::LineString::from(vec![
                (3.0, 4.0),
                (5.0, 6.0),
            ])),
        ]);
        test_transform(
            geometry_collection,
            "GEOMETRYCOLLECTION(POINT(11 22),LINESTRING(13 24,15 26))",
        );

        let empty_collection = geo_types::GeometryCollection::new_from(vec![]);
        test_transform(empty_collection, "GEOMETRYCOLLECTION EMPTY");
    }

    #[test]
    fn test_transform_dimensions() {
        let ls_xy_wkt = "LINESTRING(1.0 2.0, 3.0 4.0)";
        let ls_xy: Wkt = Wkt::from_str(ls_xy_wkt).unwrap();
        test_transform(ls_xy, "LINESTRING(11 22,13 24)");

        let ls_xyz_wkt = "LINESTRING Z(1.0 2.0 3.0, 4.0 5.0 6.0)";
        let ls_xyz: Wkt = Wkt::from_str(ls_xyz_wkt).unwrap();
        test_transform(ls_xyz, "LINESTRING Z(11 22 3,14 25 6)");

        let ls_xym_wkt = "LINESTRING M(1.0 2.0 3.0, 4.0 5.0 6.0)";
        let ls_xym: Wkt = Wkt::from_str(ls_xym_wkt).unwrap();
        test_transform(ls_xym, "LINESTRING M(11 22 3,14 25 6)");

        let ls_xyzm_wkt = "LINESTRING ZM(1.0 2.0 3.0 4.0, 5.0 6.0 7.0 8.0)";
        let ls_xyzm: Wkt = Wkt::from_str(ls_xyzm_wkt).unwrap();
        test_transform(ls_xyzm, "LINESTRING ZM(11 22 3 4,15 26 7 8)");
    }

    fn test_transform(geom: impl GeometryTrait<T = f64>, expected: &str) {
        let mut mock_transform = MockTransform {};
        let mut wkb_bytes = Vec::new();

        transform(geom, &mut mock_transform, &mut wkb_bytes).unwrap();
        let wkb_reader = read_wkb(&wkb_bytes).unwrap();
        let mut wkt = String::new();
        wkt::to_wkt::write_geometry(&mut wkt, &wkb_reader).unwrap();
        assert_eq!(wkt, expected);
    }
}
