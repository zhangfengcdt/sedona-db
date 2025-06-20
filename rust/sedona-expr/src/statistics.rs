use std::{collections::HashSet, str::FromStr};

use datafusion_common::{stats::Precision, ColumnStatistics, DataFusionError, Result, ScalarValue};
use sedona_geometry::{bounding_box::BoundingBox, types::GeometryTypeAndDimensions};
use serde::{Deserialize, Serialize};

/// Statistics specific to spatial data types
///
/// These statistics are an abstraction to provide sedonadb the ability to
/// perform generic pruning and optimization for datasources that have the
/// ability to provide this information. This may evolve to support more
/// fields; however, can currently express Parquet built-in GeoStatistics,
/// GeoParquet metadata, and GDAL OGR (via GetExtent() and GetGeomType()).
/// This struct can also represent partial or missing information.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GeoStatistics {
    bbox: Option<BoundingBox>,
    geometry_types: Option<HashSet<GeometryTypeAndDimensions>>,
}

impl GeoStatistics {
    /// Create statistics representing unspecified information
    pub fn unspecified() -> Self {
        Self {
            bbox: None,
            geometry_types: None,
        }
    }

    /// Try to deserialize GeoStatistics from DataFusion [ColumnStatistics]
    ///
    /// Various DataFusion APIs operate on [ColumnStatistics], which do not support
    /// spatial statistics natively. This function attempts to reconstruct an object
    /// that was canonically serialized into one of these objects for transport through
    /// DataFusion internals.
    pub fn try_from_column_statistics(stats: &ColumnStatistics) -> Result<Option<Self>> {
        let scalar = match &stats.sum_value {
            Precision::Exact(value) => value,
            _ => {
                return Ok(None);
            }
        };

        if let ScalarValue::Binary(Some(serialized)) = scalar {
            serde_json::from_slice(serialized).map_err(|e| DataFusionError::External(Box::new(e)))
        } else {
            Ok(None)
        }
    }

    /// Serialize this object into a [ColumnStatistics]
    ///
    /// Canonically place this object into a [ColumnStatistics] for transport through
    /// DataFusion APIs.
    pub fn to_column_statistics(&self) -> Result<ColumnStatistics> {
        let serialized =
            serde_json::to_vec(self).map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(ColumnStatistics::new_unknown()
            .with_sum_value(Precision::Exact(ScalarValue::Binary(Some(serialized)))))
    }

    /// Add a [BoundingBox] to these statistics
    pub fn with_bbox(self, bbox: Option<BoundingBox>) -> Self {
        Self {
            bbox,
            geometry_types: self.geometry_types,
        }
    }

    /// Add a definitive list of geometry type/dimension values
    pub fn with_geometry_types(self, geometry_types: Option<&[GeometryTypeAndDimensions]>) -> Self {
        Self {
            bbox: self.bbox,
            geometry_types: geometry_types.map(|types| types.iter().cloned().collect()),
        }
    }

    /// Add a definitive list of geometry type/dimension values specified as strings
    ///
    /// This accepts a list of strings like "point z" or "polygon zm". These strings
    /// are case insensitive.
    pub fn try_with_str_geometry_types(self, geometry_types: Option<&[&str]>) -> Result<Self> {
        match geometry_types {
            Some(strings) => {
                let new_geometry_types = strings
                    .iter()
                    .map(|string| {
                        GeometryTypeAndDimensions::from_str(string)
                            .map_err(|e| DataFusionError::External(Box::new(e)))
                    })
                    .collect::<Result<HashSet<GeometryTypeAndDimensions>>>()?;

                Ok(Self {
                    bbox: self.bbox,
                    geometry_types: Some(new_geometry_types),
                })
            }
            None => Ok(Self {
                bbox: self.bbox,
                geometry_types: None,
            }),
        }
    }

    /// Retrieve the [BoundingBox] for these statistics, if specified
    pub fn bbox(&self) -> &Option<BoundingBox> {
        &self.bbox
    }

    /// Retrieve the geometry/dimension values for these statistics, if specified
    pub fn geometry_types(&self) -> &Option<HashSet<GeometryTypeAndDimensions>> {
        &self.geometry_types
    }
}

#[cfg(test)]
mod test {

    use geo_traits::Dimensions;
    use sedona_geometry::types::GeometryTypeId;

    use super::*;

    #[test]
    fn unspecified() {
        let unspecified = GeoStatistics::unspecified();
        assert!(unspecified.bbox().is_none());
        assert!(unspecified.geometry_types().is_none());

        let regular_stats = unspecified.to_column_statistics().unwrap();
        assert_eq!(
            GeoStatistics::try_from_column_statistics(&regular_stats)
                .unwrap()
                .unwrap(),
            unspecified
        );
    }

    #[test]
    fn specified_bbox() {
        let specified =
            GeoStatistics::unspecified().with_bbox(Some(BoundingBox::xy((0, 1), (2, 3))));
        assert_eq!(
            specified.bbox().as_ref().unwrap(),
            &BoundingBox::xy((0, 1), (2, 3))
        );
        assert!(specified.geometry_types().is_none());

        let regular_stats = specified.to_column_statistics().unwrap();
        assert_eq!(
            GeoStatistics::try_from_column_statistics(&regular_stats)
                .unwrap()
                .unwrap(),
            specified
        );

        let removed_bbox = specified.with_bbox(None);
        assert!(removed_bbox.bbox().is_none());
    }

    #[test]
    fn specified_geometry_types() {
        use Dimensions::*;
        use GeometryTypeId::*;
        let geometry_types: Vec<GeometryTypeAndDimensions> = [(Point, Xy), (LineString, Xyzm)]
            .into_iter()
            .map(Into::into)
            .collect();

        let specified = GeoStatistics::unspecified().with_geometry_types(Some(&geometry_types));

        assert_eq!(
            specified.geometry_types(),
            &Some(geometry_types.into_iter().collect::<HashSet<_>>())
        );
        assert!(specified.bbox().is_none());

        let regular_stats = specified.to_column_statistics().unwrap();
        assert_eq!(
            GeoStatistics::try_from_column_statistics(&regular_stats)
                .unwrap()
                .unwrap(),
            specified
        );

        let removed_geometry_types = specified.with_geometry_types(None);
        assert!(removed_geometry_types.geometry_types().is_none());
    }

    #[test]
    fn specified_geometry_types_by_name() {
        use Dimensions::*;
        use GeometryTypeId::*;
        let geometry_types: Vec<GeometryTypeAndDimensions> = [(Point, Xy), (LineString, Xyzm)]
            .into_iter()
            .map(Into::into)
            .collect();

        let specified = GeoStatistics::unspecified()
            .try_with_str_geometry_types(Some(&["point", "linestring zm"]))
            .unwrap();

        assert_eq!(
            specified.geometry_types(),
            &Some(geometry_types.into_iter().collect::<HashSet<_>>())
        );

        let err = specified
            .try_with_str_geometry_types(Some(&["gazornenplat"]))
            .unwrap_err();
        assert_eq!(
            err.message(),
            "Invalid geometry type string: 'gazornenplat'"
        );
    }

    #[test]
    fn from_non_geometry_stats() {
        // Can't make geo stats from unknown
        let stats = ColumnStatistics::new_unknown();
        assert!(GeoStatistics::try_from_column_statistics(&stats)
            .unwrap()
            .is_none());

        // Can't make geo stats from binary null
        let stats = ColumnStatistics::new_unknown()
            .with_sum_value(Precision::Exact(ScalarValue::Binary(None)));
        assert!(GeoStatistics::try_from_column_statistics(&stats)
            .unwrap()
            .is_none());

        // Can't make geo stats from binary null
        let stats = ColumnStatistics::new_unknown()
            .with_sum_value(Precision::Exact(ScalarValue::Binary(Some(vec![]))));
        let err = GeoStatistics::try_from_column_statistics(&stats).unwrap_err();
        assert_eq!(
            err.message(),
            "EOF while parsing a value at line 1 column 0"
        )
    }
}
