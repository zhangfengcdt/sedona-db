use std::io::Write;

use crate::bounding_box::BoundingBox;
use crate::error::SedonaGeometryError;
use crate::wkb_factory::{
    write_wkb_geometrycollection_header, write_wkb_linestring, write_wkb_multilinestring_header,
    write_wkb_multipoint_header, write_wkb_multipolygon_header, write_wkb_point, write_wkb_polygon,
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
    fn transform_coords(&mut self, coords: &mut Vec<(f64, f64)>)
        -> Result<(), SedonaGeometryError>;
}

impl CrsTransform for Box<dyn CrsTransform> {
    fn transform_coords(
        &mut self,
        coords: &mut Vec<(f64, f64)>,
    ) -> Result<(), SedonaGeometryError> {
        self.as_mut().transform_coords(coords)
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
            let (x, y) = if let Some(coord) = pt.coord() {
                let mut xy_coord = vec![(coord.x(), coord.y())];
                trans.transform_coords(&mut xy_coord)?;
                xy_coord[0]
            } else {
                // NANs are the standard for empty points in WKB
                (f64::NAN, f64::NAN)
            };
            match dims {
                Dimensions::Xy => write_wkb_point(out, (x, y))?,
                _ => {
                    return Err(SedonaGeometryError::Invalid(
                        "Unsupported dimensions for transformation".to_string(),
                    ));
                }
            }
        }
        GeometryType::LineString(ls) => {
            let mut xy_coords: Vec<(f64, f64)> =
                ls.coords().map(|coord| (coord.x(), coord.y())).collect();
            trans.transform_coords(&mut xy_coords)?;

            match dims {
                Dimensions::Xy => write_wkb_linestring(out, xy_coords.into_iter())?,
                _ => {
                    return Err(SedonaGeometryError::Invalid(
                        "Unsupported dimensions for transformation".to_string(),
                    ));
                }
            }
        }
        GeometryType::Polygon(pl) => {
            if pl.interiors().count() > 0 {
                return Err(SedonaGeometryError::Invalid(
                    "Polygon with interior rings not yet supported for transformation".to_string(),
                ));
            }
            if pl.exterior().is_none() {
                write_wkb_polygon(out, [].into_iter())?;
                return Ok(());
            }
            let mut ex_xy_coords: Vec<(f64, f64)> = pl
                .exterior()
                .unwrap()
                .coords()
                .map(|coord| (coord.x(), coord.y()))
                .collect();
            trans.transform_coords(&mut ex_xy_coords)?;

            match dims {
                Dimensions::Xy => write_wkb_polygon(out, ex_xy_coords.into_iter())?,
                _ => {
                    return Err(SedonaGeometryError::Invalid(
                        "Unsupported dimensions for transformation".to_string(),
                    ));
                }
            }
        }
        GeometryType::MultiPoint(multi_pt) => {
            write_wkb_multipoint_header(out, multi_pt.points().count())?;
            for pt in multi_pt.points() {
                transform(pt, trans, out)?;
            }
        }
        GeometryType::MultiLineString(multi_ls) => {
            write_wkb_multilinestring_header(out, multi_ls.line_strings().count())?;
            for ls in multi_ls.line_strings() {
                transform(ls, trans, out)?;
            }
        }
        GeometryType::MultiPolygon(multi_pl) => {
            write_wkb_multipolygon_header(out, multi_pl.polygons().count())?;
            for pl in multi_pl.polygons() {
                transform(pl, trans, out)?;
            }
        }
        GeometryType::GeometryCollection(collection) => {
            write_wkb_geometrycollection_header(out, collection.geometries().count())?;
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

#[cfg(test)]
mod test {
    use super::*;
    use wkb::reader::read_wkb;

    #[derive(Debug)]
    struct MockTransform {}
    impl CrsTransform for MockTransform {
        fn transform_coords(
            &mut self,
            coords: &mut Vec<(f64, f64)>,
        ) -> Result<(), SedonaGeometryError> {
            // Apply a simple transformation to verify it was called
            for coord in coords.iter_mut() {
                coord.0 += 10.0;
                coord.1 += 20.0;
            }
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
        let linestring = geo_types::LineString::from(vec![(1.0, 2.0), (3.0, 4.0)]);
        test_transform(linestring, "LINESTRING(11 22,13 24)");

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
