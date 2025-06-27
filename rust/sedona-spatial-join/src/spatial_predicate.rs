use core::fmt;
use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, RecordBatch};
use datafusion_common::{DataFusionError, JoinSide, Result, ScalarValue};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;
use geo_generic_alg::{BoundingRect, Contains, Distance, Euclidean, Intersects, Relate, Within};
use geo_traits::to_geo::ToGeoGeometry;
use geo_types::Rect;
use sedona_functions::executor::IterGeo;
use sedona_schema::datatypes::SedonaType;
use wkb::reader::Wkb;

use crate::SpatialJoinOptions;

/// Spatial predicate is the join condition of a spatial join. It can be a distance predicate
/// or a relation predicate.
#[derive(Debug, Clone)]
pub enum SpatialPredicate {
    Distance(DistancePredicate),
    Relation(RelationPredicate),
}

impl SpatialPredicate {
    /// Create a spatial predicate evaluator for the spatial predicate.
    pub fn evaluator(&self, options: SpatialJoinOptions) -> Arc<dyn SpatialPredicateEvaluator> {
        match self {
            SpatialPredicate::Distance(predicate) => {
                Arc::new(DistancePredicateEvaluator::new(predicate.clone(), options))
            }
            SpatialPredicate::Relation(predicate) => {
                Arc::new(RelationPredicateEvaluator::new(predicate.clone(), options))
            }
        }
    }
}

impl std::fmt::Display for SpatialPredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpatialPredicate::Distance(predicate) => write!(f, "{predicate}"),
            SpatialPredicate::Relation(predicate) => write!(f, "{predicate}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DistancePredicate {
    /// The expression for evaluating the geometry value on the left side. The expression
    /// should be evaluated directly on the left side batches.
    pub left: Arc<dyn PhysicalExpr>,
    /// The expression for evaluating the geometry value on the right side. The expression
    /// should be evaluated directly on the right side batches.
    pub right: Arc<dyn PhysicalExpr>,
    /// The expression for evaluating the distance value. The expression
    /// should be evaluated directly on the left or right side batches according to distance_side.
    pub distance: Arc<dyn PhysicalExpr>,
    /// The side of the distance expression. It could be JoinSide::None if the distance expression
    /// is not a column reference. The most common case is that the distance expression is a
    /// literal value.
    pub distance_side: JoinSide,
}

impl DistancePredicate {
    pub fn new(
        left: Arc<dyn PhysicalExpr>,
        right: Arc<dyn PhysicalExpr>,
        distance: Arc<dyn PhysicalExpr>,
        distance_side: JoinSide,
    ) -> Self {
        Self {
            left,
            right,
            distance,
            distance_side,
        }
    }
}

impl std::fmt::Display for DistancePredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ST_Distance({}, {}) < {}",
            self.left, self.right, self.distance
        )
    }
}

/// Spatial relation predicate is the join condition of a spatial join.
#[derive(Debug, Clone)]
pub struct RelationPredicate {
    /// The expression for evaluating the geometry value on the left side. The expression
    /// should be evaluated directly on the left side batches.
    pub left: Arc<dyn PhysicalExpr>,
    /// The expression for evaluating the geometry value on the right side. The expression
    /// should be evaluated directly on the right side batches.
    pub right: Arc<dyn PhysicalExpr>,
    /// The spatial relation type.
    pub relation_type: SpatialRelationType,
}

impl RelationPredicate {
    pub fn new(
        left: Arc<dyn PhysicalExpr>,
        right: Arc<dyn PhysicalExpr>,
        relation_type: SpatialRelationType,
    ) -> Self {
        Self {
            left,
            right,
            relation_type,
        }
    }
}

impl std::fmt::Display for RelationPredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ST_{}({}, {})",
            self.relation_type, self.left, self.right
        )
    }
}

/// Type of spatial relation predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpatialRelationType {
    Intersects,
    Contains,
    Within,
    Covers,
    CoveredBy,
    Touches,
    Crosses,
    Overlaps,
    Equals,
}

impl SpatialRelationType {
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "st_intersects" => Some(SpatialRelationType::Intersects),
            "st_contains" => Some(SpatialRelationType::Contains),
            "st_within" => Some(SpatialRelationType::Within),
            "st_covers" => Some(SpatialRelationType::Covers),
            "st_coveredby" | "st_covered_by" => Some(SpatialRelationType::CoveredBy),
            "st_touches" => Some(SpatialRelationType::Touches),
            "st_crosses" => Some(SpatialRelationType::Crosses),
            "st_overlaps" => Some(SpatialRelationType::Overlaps),
            "st_equals" => Some(SpatialRelationType::Equals),
            _ => None,
        }
    }

    pub fn invert(&self) -> Self {
        match self {
            SpatialRelationType::Intersects => SpatialRelationType::Intersects,
            SpatialRelationType::Covers => SpatialRelationType::CoveredBy,
            SpatialRelationType::CoveredBy => SpatialRelationType::Covers,
            SpatialRelationType::Contains => SpatialRelationType::Within,
            SpatialRelationType::Within => SpatialRelationType::Contains,
            SpatialRelationType::Touches => SpatialRelationType::Touches,
            SpatialRelationType::Crosses => SpatialRelationType::Crosses,
            SpatialRelationType::Overlaps => SpatialRelationType::Overlaps,
            SpatialRelationType::Equals => SpatialRelationType::Equals,
        }
    }

    pub fn evaluator(&self) -> Arc<dyn RelationEvaluator> {
        match self {
            SpatialRelationType::Intersects => Arc::new(IntersectsEvaluator),
            SpatialRelationType::Contains => Arc::new(ContainsEvaluator),
            SpatialRelationType::Within => Arc::new(WithinEvaluator),
            SpatialRelationType::Covers => Arc::new(CoversEvaluator),
            SpatialRelationType::CoveredBy => Arc::new(CoveredByEvaluator),
            SpatialRelationType::Touches => Arc::new(TouchesEvaluator),
            SpatialRelationType::Crosses => Arc::new(CrossesEvaluator),
            SpatialRelationType::Overlaps => Arc::new(OverlapsEvaluator),
            SpatialRelationType::Equals => Arc::new(EqualsEvaluator),
        }
    }
}

impl std::fmt::Display for SpatialRelationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpatialRelationType::Intersects => write!(f, "intersects"),
            SpatialRelationType::Contains => write!(f, "contains"),
            SpatialRelationType::Within => write!(f, "within"),
            SpatialRelationType::Covers => write!(f, "covers"),
            SpatialRelationType::CoveredBy => write!(f, "coveredby"),
            SpatialRelationType::Touches => write!(f, "touches"),
            SpatialRelationType::Crosses => write!(f, "crosses"),
            SpatialRelationType::Overlaps => write!(f, "overlaps"),
            SpatialRelationType::Equals => write!(f, "equals"),
        }
    }
}

/// Evaluator for a relation predicate.
#[derive(Debug)]
pub struct RelationPredicateEvaluator {
    inner: RelationPredicate,
    relation_evaluator: Arc<dyn RelationEvaluator>,
    _options: SpatialJoinOptions,
}

impl RelationPredicateEvaluator {
    pub fn new(inner: RelationPredicate, options: SpatialJoinOptions) -> Self {
        let relation_evaluator = inner.relation_type.evaluator();
        Self {
            inner,
            relation_evaluator,
            _options: options,
        }
    }
}

/// Evaluator for a distance predicate.
#[derive(Debug)]
pub struct DistancePredicateEvaluator {
    inner: DistancePredicate,
    _options: SpatialJoinOptions,
}

impl DistancePredicateEvaluator {
    pub fn new(inner: DistancePredicate, options: SpatialJoinOptions) -> Self {
        Self {
            inner,
            _options: options,
        }
    }
}

/// Result of evaluating a geometry batch.
pub struct GeometryBatchResult {
    pub geometry_array: ArrayRef,
    pub rects: Vec<(usize, Rect)>,
    pub distance: Option<ColumnarValue>,
}

/// Spatial predicate evaluator is the evaluator for a spatial predicate. It can be a distance
/// predicate evaluator or a relation predicate evaluator.
pub trait SpatialPredicateEvaluator: fmt::Debug + Send + Sync {
    /// Evaluate the spatial predicate on the build side.
    fn evaluate_build(&self, batch: &RecordBatch) -> Result<GeometryBatchResult> {
        let geom_expr = self.build_side_expr()?;
        evaluate_with_rects(batch, &geom_expr)
    }

    /// Evaluate the spatial predicate on the probe side.
    fn evaluate_probe(&self, batch: &RecordBatch) -> Result<GeometryBatchResult> {
        let geom_expr = self.probe_side_expr()?;
        evaluate_with_rects(batch, &geom_expr)
    }

    /// Check if the spatial predicate is a distance predicate.
    fn is_distance_predicate(&self) -> bool {
        false
    }

    /// Resolve the distance value for a given row.
    #[allow(unused)]
    fn resolve_distance(
        &self,
        build_distance: &Option<ColumnarValue>,
        probe_distance: &Option<ColumnarValue>,
        row_idx: usize,
    ) -> Result<Option<f64>> {
        Ok(None)
    }

    /// Evaluate the spatial predicate given the geometry values and distance value.
    fn evaluate_predicate(&self, build: &Wkb, probe: &Wkb, distance: Option<f64>) -> Result<bool>;

    /// Get the expression for the build side.
    fn build_side_expr(&self) -> Result<Arc<dyn PhysicalExpr>>;

    /// Get the expression for the probe side.
    fn probe_side_expr(&self) -> Result<Arc<dyn PhysicalExpr>>;
}

fn evaluate_with_rects(
    batch: &RecordBatch,
    geom_expr: &Arc<dyn PhysicalExpr>,
) -> Result<GeometryBatchResult> {
    let geometry_columnar_value = geom_expr.evaluate(batch)?;
    let num_rows = batch.num_rows();
    let geometry_array = geometry_columnar_value.to_array(num_rows)?;

    let mut rect_vec = Vec::with_capacity(num_rows);
    let sedona_type: SedonaType = geometry_array.data_type().try_into()?;
    let wkb_array = sedona_type.unwrap_array(&geometry_array)?;
    wkb_array.iter_as_wkb(&sedona_type, num_rows, |idx, wkb_opt| {
        if let Some(wkb) = wkb_opt {
            if let Some(rect) = wkb.bounding_rect() {
                rect_vec.push((idx, rect));
            }
        }
        Ok(())
    })?;
    Ok(GeometryBatchResult {
        geometry_array,
        rects: rect_vec,
        distance: None,
    })
}

impl DistancePredicateEvaluator {
    fn evaluate_with_rects(
        &self,
        batch: &RecordBatch,
        geom_expr: &Arc<dyn PhysicalExpr>,
        side: JoinSide,
    ) -> Result<GeometryBatchResult> {
        let mut result = evaluate_with_rects(batch, geom_expr)?;

        let should_expand = match side {
            JoinSide::Left => self.inner.distance_side == JoinSide::Left,
            JoinSide::Right => self.inner.distance_side != JoinSide::Left,
            JoinSide::None => unreachable!(),
        };

        if !should_expand {
            return Ok(result);
        }

        // Expand the vec by distance
        let distance_columnar_value = self.inner.distance.evaluate(batch)?;
        match &distance_columnar_value {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(distance))) => {
                result.rects.iter_mut().for_each(|(_, rect)| {
                    expand_rect_in_place(rect, *distance);
                });
            }
            ColumnarValue::Scalar(ScalarValue::Float64(None)) => {
                // Distance expression evaluates to NULL, the resulting distance should be NULL as well.
                result.rects.clear();
            }
            ColumnarValue::Array(array) => {
                if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
                    array
                        .iter()
                        .zip(result.rects.iter_mut())
                        .for_each(|(distance, (_, rect))| {
                            if let Some(distance) = distance {
                                expand_rect_in_place(rect, distance);
                            }
                        });
                } else {
                    return Err(DataFusionError::Internal(
                        "Distance columnar value is not a Float64Array".to_string(),
                    ));
                }
            }
            _ => {
                return Err(DataFusionError::Internal(
                    "Distance columnar value is not a Float64".to_string(),
                ));
            }
        }

        result.distance = Some(distance_columnar_value);
        Ok(result)
    }
}

fn expand_rect_in_place(rect: &mut Rect, distance: f64) {
    let mut min = rect.min();
    let mut max = rect.max();
    min.x -= distance;
    min.y -= distance;
    max.x += distance;
    max.y += distance;
    rect.set_min(min);
    rect.set_max(max);
}

impl SpatialPredicateEvaluator for DistancePredicateEvaluator {
    fn evaluate_build(&self, batch: &RecordBatch) -> Result<GeometryBatchResult> {
        let geom_expr = self.build_side_expr()?;
        self.evaluate_with_rects(batch, &geom_expr, JoinSide::Left)
    }

    fn evaluate_probe(&self, batch: &RecordBatch) -> Result<GeometryBatchResult> {
        let geom_expr = self.probe_side_expr()?;
        self.evaluate_with_rects(batch, &geom_expr, JoinSide::Right)
    }

    fn build_side_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::clone(&self.inner.left))
    }

    fn probe_side_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::clone(&self.inner.right))
    }

    fn is_distance_predicate(&self) -> bool {
        true
    }

    fn resolve_distance(
        &self,
        build_distance: &Option<ColumnarValue>,
        probe_distance: &Option<ColumnarValue>,
        row_idx: usize,
    ) -> Result<Option<f64>> {
        let distance = match self.inner.distance_side {
            JoinSide::Left => build_distance,
            JoinSide::Right | JoinSide::None => probe_distance,
        };

        let Some(distance) = distance else {
            return Ok(None);
        };

        match distance {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(distance))) => Ok(Some(*distance)),
            ColumnarValue::Scalar(ScalarValue::Float64(None)) => Ok(None),
            ColumnarValue::Array(array) => {
                let array = array.as_any().downcast_ref::<Float64Array>().ok_or(
                    DataFusionError::Internal(
                        "Distance columnar value is not a Float64Array".to_string(),
                    ),
                )?;
                let distance = array.value(row_idx);
                Ok(Some(distance))
            }
            _ => Err(DataFusionError::Internal(
                "Distance columnar value is not a Float64".to_string(),
            )),
        }
    }

    fn evaluate_predicate(&self, build: &Wkb, probe: &Wkb, distance: Option<f64>) -> Result<bool> {
        let Some(distance) = distance else {
            return Ok(false);
        };

        let geom = build.to_geometry();
        let euc = Euclidean;
        let dist = euc.distance(&geom, &probe.to_geometry());
        Ok(dist <= distance)
    }
}

impl SpatialPredicateEvaluator for RelationPredicateEvaluator {
    fn build_side_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::clone(&self.inner.left))
    }

    fn probe_side_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::clone(&self.inner.right))
    }

    fn is_distance_predicate(&self) -> bool {
        false
    }

    fn evaluate_predicate(&self, build: &Wkb, probe: &Wkb, _distance: Option<f64>) -> Result<bool> {
        Ok(self.relation_evaluator.evaluate(build, probe))
    }
}

pub trait RelationEvaluator: fmt::Debug + Send + Sync {
    fn predicate_type(&self) -> SpatialRelationType;

    /// Evaluate the spatial predicate when both sides are not prepared.
    fn evaluate(&self, build: &Wkb, probe: &Wkb) -> bool;
}

#[derive(Debug)]
pub struct IntersectsEvaluator;

impl RelationEvaluator for IntersectsEvaluator {
    fn predicate_type(&self) -> SpatialRelationType {
        SpatialRelationType::Intersects
    }

    fn evaluate(&self, build: &Wkb, probe: &Wkb) -> bool {
        build.intersects(probe)
    }
}

#[derive(Debug)]
pub struct ContainsEvaluator;

impl RelationEvaluator for ContainsEvaluator {
    fn predicate_type(&self) -> SpatialRelationType {
        SpatialRelationType::Contains
    }

    fn evaluate(&self, build: &Wkb, probe: &Wkb) -> bool {
        let build_geom = build.to_geometry();
        let probe_geom = probe.to_geometry();
        build_geom.contains(&probe_geom)
    }
}

#[derive(Debug)]
pub struct WithinEvaluator;

impl RelationEvaluator for WithinEvaluator {
    fn predicate_type(&self) -> SpatialRelationType {
        SpatialRelationType::Within
    }

    fn evaluate(&self, build: &Wkb, probe: &Wkb) -> bool {
        let build_geom = build.to_geometry();
        let probe_geom = probe.to_geometry();
        build_geom.is_within(&probe_geom)
    }
}

/// Macro to generate relation evaluators that use the relate() method
macro_rules! impl_relate_evaluator {
    ($struct_name:ident, $relation_type:path, $method:ident) => {
        #[derive(Debug)]
        pub struct $struct_name;

        impl RelationEvaluator for $struct_name {
            fn predicate_type(&self) -> SpatialRelationType {
                $relation_type
            }

            fn evaluate(&self, build: &Wkb, probe: &Wkb) -> bool {
                let build_geom = build.to_geometry();
                let probe_geom = probe.to_geometry();
                build_geom.relate(&probe_geom).$method()
            }
        }
    };
}

// Generate relate-based evaluators using the macro
impl_relate_evaluator!(TouchesEvaluator, SpatialRelationType::Touches, is_touches);
impl_relate_evaluator!(CrossesEvaluator, SpatialRelationType::Crosses, is_crosses);
impl_relate_evaluator!(
    OverlapsEvaluator,
    SpatialRelationType::Overlaps,
    is_overlaps
);
impl_relate_evaluator!(CoversEvaluator, SpatialRelationType::Covers, is_covers);
impl_relate_evaluator!(
    CoveredByEvaluator,
    SpatialRelationType::CoveredBy,
    is_coveredby
);
impl_relate_evaluator!(EqualsEvaluator, SpatialRelationType::Equals, is_equal_topo);
