use proj::{Area, Proj, ProjCreateError};
use sedona_geometry::bounding_box::BoundingBox;
use sedona_geometry::error::SedonaGeometryError;
use sedona_geometry::interval::IntervalTrait;
use sedona_geometry::transform::{CrsEngine, CrsTransform};

/// A [CrsEngine] implemented using the [proj] crate
pub struct ProjCrsEngine;

impl CrsEngine for ProjCrsEngine {
    fn get_transform_crs_to_crs(
        &self,
        from: &str,
        to: &str,
        area_of_interest: Option<BoundingBox>,
        options: &str,
    ) -> Result<Box<dyn CrsTransform>, SedonaGeometryError> {
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
        Ok(Box::new(transform))
    }
    fn get_transform_pipeline(
        &self,
        pipeline: &str,
        options: &str,
    ) -> Result<Box<dyn CrsTransform>, SedonaGeometryError> {
        if !options.is_empty() {
            return Err(SedonaGeometryError::Invalid(
                "Options for transform not supported yet".to_string(),
            ));
        }
        let transform = ProjPipelineTransform::new(pipeline)
            .map_err(|e| SedonaGeometryError::Invalid(format!("PROJ creation error: {e}")))?;
        Ok(Box::new(transform))
    }
}

/// A [Transform] implemented using the [proj] crate
#[derive(Debug)]
pub struct ProjTransform {
    proj: Proj,
}

impl ProjTransform {
    pub fn new(proj: Proj) -> Self {
        Self { proj }
    }
}

impl CrsTransform for ProjTransform {
    fn transform_coords(
        &mut self,
        coords: &mut Vec<(f64, f64)>,
    ) -> Result<(), SedonaGeometryError> {
        self.proj
            .convert_array(coords)
            .map_err(|e| SedonaGeometryError::Invalid(format!("PROJ transformation failed: {e}")))
            .map(|_| ())?;
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
        let proj = Proj::new_known_crs(from, to, area)?;
        Ok(Self {
            transform: ProjTransform::new(proj),
        })
    }
}

impl CrsTransform for ProjCrsToCrsTransform {
    fn transform_coords(
        &mut self,
        coords: &mut Vec<(f64, f64)>,
    ) -> Result<(), SedonaGeometryError> {
        self.transform.transform_coords(coords)
    }
}

/// A [CrsTransform] that applies a PROJ pipeline transformation
#[derive(Debug)]
pub struct ProjPipelineTransform {
    transform: ProjTransform,
}

impl ProjPipelineTransform {
    pub fn new(pipeline: &str) -> Result<Self, ProjCreateError> {
        let proj = Proj::new(pipeline)?;
        Ok(Self {
            transform: ProjTransform::new(proj),
        })
    }
}

impl CrsTransform for ProjPipelineTransform {
    fn transform_coords(
        &mut self,
        coords: &mut Vec<(f64, f64)>,
    ) -> Result<(), SedonaGeometryError> {
        self.transform.transform_coords(coords)
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
        let mut trans = engine
            .get_transform_crs_to_crs("EPSG:2230", "EPSG:26946", None, "")
            .unwrap();
        test_2230_to_26946(&mut trans);

        // use a different transformation where the AOI is valid
        let aoi = BoundingBox::xy((37, 38), (-122, -120));
        let mut trans_with_aoi = engine
            .get_transform_crs_to_crs("EPSG:2230", "EPSG:4326", Some(aoi), "")
            .unwrap();
        test_4269_to_4326(&mut trans_with_aoi);

        let trans_error = engine
            .get_transform_crs_to_crs("foo", "bar", None, "")
            .unwrap_err();
        assert_eq!(trans_error.to_string(), "PROJ creation for CRS Transform failed with error: The underlying PROJ call failed: Invalid PROJ string syntax");
    }

    #[test]
    fn proj_pipeline() {
        let engine = ProjCrsEngine;
        let mut trans = engine
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
        test_2230_to_26946(&mut trans);

        let trans_error = engine.get_transform_pipeline("foo", "");
        assert!(
            trans_error.is_err(),
            "Expected an error for invalid pipeline"
        );
    }

    #[test]
    fn transform_coords() {
        let engine = ProjCrsEngine;
        let mut trans = engine
            .get_transform_crs_to_crs("EPSG:2230", "EPSG:26946", None, "")
            .unwrap();

        let mut coords = vec![(4_760_096.4, 3_744_293.5), (4_760_096.6, 3_744_293.7)];
        trans.transform_coords(&mut coords).unwrap();
        assert_relative_eq!(coords[0].x(), 1_450_880.284_378, epsilon = 1e-6);
        assert_relative_eq!(coords[0].y(), 1_141_262.941_224, epsilon = 1e-6);
        assert_relative_eq!(coords[1].x(), 1_450_880.345_339, epsilon = 1e-6);
        assert_relative_eq!(coords[1].y(), 1_141_263.002_184, epsilon = 1e-6);

        coords = vec![(f64::NAN, f64::NAN)];
        trans.transform_coords(&mut coords).unwrap();
        assert!(
            coords[0].0.is_nan() && coords[0].1.is_nan(),
            "Expected NaN coordinates"
        );
    }

    fn test_2230_to_26946(trans: &mut dyn CrsTransform) {
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
    fn test_4269_to_4326(trans: &mut dyn CrsTransform) {
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
