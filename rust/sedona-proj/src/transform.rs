use proj::{Area, ProjCreateError};
use sedona_geometry::bounding_box::BoundingBox;
use sedona_geometry::error::SedonaGeometryError;
use sedona_geometry::interval::IntervalTrait;
use sedona_geometry::transform::{CrsEngine, CrsTransform};
use std::cell::RefCell;
use std::rc::Rc;

use crate::proj::Proj;

/// A [CrsEngine] implemented using the [proj] crate
pub struct ProjCrsEngine;

impl CrsEngine for ProjCrsEngine {
    fn get_transform_crs_to_crs(
        &self,
        from: &str,
        to: &str,
        area_of_interest: Option<BoundingBox>,
        options: &str,
    ) -> Result<Rc<dyn CrsTransform>, SedonaGeometryError> {
        if !options.is_empty() {
            return Err(SedonaGeometryError::Invalid(
                "Options for area of use not supported yet".to_string(),
            ));
        }

        let area = area_of_interest.map(|bbox| {
            Area::new(
                bbox.x().lo(), // west
                bbox.y().lo(), // south
                bbox.x().hi(), // east
                bbox.y().hi(), // north
            )
        });

        let transform = ProjCrsToCrsTransform::new(from, to, area).map_err(|e| {
            SedonaGeometryError::Invalid(format!(
                "PROJ creation for CRS Transform failed with error: {e}"
            ))
        })?;

        Ok(Rc::new(transform))
    }
    fn get_transform_pipeline(
        &self,
        pipeline: &str,
        options: &str,
    ) -> Result<Rc<dyn CrsTransform>, SedonaGeometryError> {
        if !options.is_empty() {
            return Err(SedonaGeometryError::Invalid(
                "Options for transform not supported yet".to_string(),
            ));
        }

        let transform = ProjPipelineTransform::new(pipeline)
            .map_err(|e| SedonaGeometryError::Invalid(format!("PROJ creation error: {e}")))?;

        Ok(Rc::new(transform))
    }
}

/// A `Transform` implemented using the [proj] crate
#[derive(Debug)]
pub struct ProjTransform {
    proj: RefCell<Proj>,
}

impl ProjTransform {
    fn new(proj: Proj) -> Self {
        Self {
            proj: RefCell::new(proj),
        }
    }
}

impl CrsTransform for ProjTransform {
    fn transform_coord(&self, coord: &mut (f64, f64)) -> Result<(), SedonaGeometryError> {
        let res = self.proj.borrow_mut().convert(*coord).map_err(|e| {
            SedonaGeometryError::Invalid(format!(
                "PROJ coordinate transformation failed with error: {e}"
            ))
        })?;
        coord.0 = res.0;
        coord.1 = res.1;
        Ok(())
    }
}

/// A [CrsTransform] that transforms coordinates from one CRS to another using PROJ
#[derive(Debug)]
pub struct ProjCrsToCrsTransform {
    transform: ProjTransform,
}

impl ProjCrsToCrsTransform {
    pub fn new(from: &str, to: &str, area: Option<Area>) -> Result<Self, ProjCreateError> {
        let source_crs = Proj::try_new(from)?;
        let target_crs = Proj::try_new(to)?;
        let proj = Proj::crs_to_crs(&source_crs, &target_crs, area)?;
        Ok(Self {
            transform: ProjTransform::new(proj),
        })
    }
}

impl CrsTransform for ProjCrsToCrsTransform {
    fn transform_coord(&self, coord: &mut (f64, f64)) -> Result<(), SedonaGeometryError> {
        self.transform.transform_coord(coord)
    }
}

/// A [CrsTransform] that applies a PROJ pipeline transformation
#[derive(Debug)]
pub struct ProjPipelineTransform {
    transform: ProjTransform,
}

impl ProjPipelineTransform {
    pub fn new(pipeline: &str) -> Result<Self, ProjCreateError> {
        let proj = Proj::try_new(pipeline)?;
        Ok(Self {
            transform: ProjTransform::new(proj),
        })
    }
}

impl CrsTransform for ProjPipelineTransform {
    fn transform_coord(&self, coord: &mut (f64, f64)) -> Result<(), SedonaGeometryError> {
        self.transform.transform_coord(coord)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use approx::assert_relative_eq;
    use geo_traits::{CoordTrait, GeometryTrait, GeometryType, PointTrait};
    use geo_types::Point;
    use sedona_geometry::transform::transform;
    use wkb::reader::read_wkb;
    #[test]
    fn proj_crs_to_crs() {
        let engine = ProjCrsEngine;
        let trans = engine
            .get_transform_crs_to_crs("EPSG:2230", "EPSG:26946", None, "")
            .unwrap();
        test_2230_to_26946(trans.as_ref());

        // use a different transformation where the AOI is valid
        let aoi = BoundingBox::xy((37, 38), (-122, -120));
        let trans_with_aoi = engine
            .get_transform_crs_to_crs("EPSG:2230", "EPSG:4326", Some(aoi), "")
            .unwrap();
        test_4269_to_4326(trans_with_aoi.as_ref());

        let trans_error = engine
            .get_transform_crs_to_crs("foo", "bar", None, "")
            .unwrap_err();
        assert_eq!(trans_error.to_string(), "PROJ creation for CRS Transform failed with error: The underlying PROJ call failed: Invalid PROJ string syntax");
    }

    #[test]
    fn proj_pipeline() {
        let engine = ProjCrsEngine;
        let trans = engine
            .get_transform_pipeline(
                "+proj=pipeline +step +inv +proj=lcc +lat_1=33.88333333333333 +lat_2=32.78333333333333 \
                 +lat_0=32.16666666666666 +lon_0=-116.25 +x_0=2000000.0001016 +y_0=500000.0001016001 \
                 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=us-ft +no_defs \
                 +step +proj=lcc +lat_1=33.88333333333333 +lat_2=32.78333333333333 \
                 +lat_0=32.16666666666666 +lon_0=-116.25 +x_0=2000000 +y_0=500000 \
                 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs",
                "",
            )
            .unwrap();
        test_2230_to_26946(trans.as_ref());

        let trans_error = engine.get_transform_pipeline("foo", "");
        assert!(
            trans_error.is_err(),
            "Expected an error for invalid pipeline"
        );
    }

    #[test]
    fn transform_coord() {
        let engine = ProjCrsEngine;
        let trans = engine
            .get_transform_crs_to_crs("EPSG:2230", "EPSG:26946", None, "")
            .unwrap();

        let mut coord = (4_760_096.4, 3_744_293.5);
        trans.as_ref().transform_coord(&mut coord).unwrap();
        assert_relative_eq!(coord.x(), 1_450_880.284_378, epsilon = 1e-6);
        assert_relative_eq!(coord.y(), 1_141_262.941_224, epsilon = 1e-6);

        coord = (f64::NAN, f64::NAN);
        trans.as_ref().transform_coord(&mut coord).unwrap();
        assert!(
            coord.x().is_nan() && coord.y().is_nan(),
            "Expected NaN coordinates"
        );
    }

    fn test_2230_to_26946(trans: &dyn CrsTransform) {
        let point = Point::new(4_760_096.421_921, 3_744_293.729_449);
        let mut wkb_bytes = Vec::new();
        transform(point, trans, &mut wkb_bytes).unwrap();

        let wkb_reader = read_wkb(&wkb_bytes).unwrap();
        assert!(
            matches!(wkb_reader.as_type(), GeometryType::Point(_)),
            "Expected a Point geometry type"
        );
        if let GeometryType::Point(point) = wkb_reader.as_type() {
            let x = point.coord().unwrap().x();
            let y = point.coord().unwrap().y();
            // expected values are from proj's example page
            assert_relative_eq!(x, 1_450_880.291_060_5, epsilon = 1e-6);
            assert_relative_eq!(y, 1_141_263.011_160_45, epsilon = 1e-6);
        }
    }
    fn test_4269_to_4326(trans: &dyn CrsTransform) {
        let point = Point::new(4760096.0, 5044293.0);
        let mut wkb_bytes = Vec::new();
        transform(point, trans, &mut wkb_bytes).unwrap();

        let wkb_reader = read_wkb(&wkb_bytes).unwrap();
        assert!(
            matches!(wkb_reader.as_type(), GeometryType::Point(_)),
            "Expected a Point geometry type"
        );
        if let GeometryType::Point(point) = wkb_reader.as_type() {
            let x = point.coord().unwrap().x();
            let y = point.coord().unwrap().y();
            assert_relative_eq!(x, -122.748_786_5, epsilon = 1e-6);
            assert_relative_eq!(y, 41.335_173_4, epsilon = 1e-6);
        }
    }
}
