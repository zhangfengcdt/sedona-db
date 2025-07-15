use std::sync::{
    atomic::{AtomicUsize, Ordering},
    OnceLock,
};

use datafusion_common::{internal_err, Result};
use geo_generic_alg::{Contains, Distance, Euclidean, Intersects, Relate, Within};
use geo_traits::to_geo::ToGeoGeometry;
use sedona_common::{ExecutionMode, SpatialJoinOptions};
use sedona_expr::statistics::GeoStatistics;
use wkb::reader::Wkb;

use crate::{
    index::IndexQueryResult,
    init_once_array::InitOnceArray,
    refine::{
        exec_mode_selector::{
            create_exec_mode_selector, get_or_update_execution_mode, ExecModeSelector, SelectorFunc,
        },
        IndexQueryResultRefiner,
    },
    spatial_predicate::{SpatialPredicate, SpatialRelationType},
};

/// A refiner that uses the geo library to evaluate spatial predicates.
pub(crate) struct GeoRefiner {
    evaluator: Box<dyn GeoPredicateEvaluator>,
    prepared_geoms:
        InitOnceArray<Option<geo_generic_alg::PreparedGeometry<'static, geo_types::Geometry>>>,
    mem_usage: AtomicUsize,
    exec_mode: OnceLock<ExecutionMode>,
    exec_mode_selector: Option<ExecModeSelector<SelectorFunc>>,
}

impl GeoRefiner {
    pub fn new(
        predicate: &SpatialPredicate,
        options: SpatialJoinOptions,
        num_build_geoms: usize,
        build_stats: GeoStatistics,
    ) -> Self {
        let evaluator = create_evaluator(predicate);
        let exec_mode = OnceLock::new();
        if matches!(options.execution_mode, ExecutionMode::Speculative(_)) {
            // Automatically select the execution mode based on spatial predicate
            if matches!(predicate, SpatialPredicate::Distance(_)) {
                // Distance predicate cannot be optimized by prepared geometries
                exec_mode.set(ExecutionMode::PrepareNone).unwrap();
            }
        } else {
            exec_mode.set(options.execution_mode).unwrap();
        }

        let prepared_geom_array_size =
            if matches!(exec_mode.get(), Some(ExecutionMode::PrepareBuild) | None) {
                num_build_geoms
            } else {
                0
            };

        let prepared_geoms = InitOnceArray::new(prepared_geom_array_size);
        let mem_usage = prepared_geoms.allocated_size();
        let exec_mode_selector = if exec_mode.get().is_none() {
            create_exec_mode_selector(build_stats, options.execution_mode, select_optimal_mode)
        } else {
            None
        };
        Self {
            evaluator,
            prepared_geoms,
            mem_usage: AtomicUsize::new(mem_usage),
            exec_mode,
            exec_mode_selector,
        }
    }

    fn refine_prepare_none(
        &self,
        probe: &Wkb<'_>,
        index_query_results: &[IndexQueryResult],
    ) -> Result<Vec<(i32, i32)>> {
        let mut build_batch_positions = Vec::with_capacity(index_query_results.len());
        for index_result in index_query_results {
            if self
                .evaluator
                .evaluate(index_result.wkb, probe, index_result.distance)?
            {
                build_batch_positions.push(index_result.position);
            }
        }
        Ok(build_batch_positions)
    }

    fn refine_prepare_build(
        &self,
        probe: &Wkb<'_>,
        index_query_results: &[IndexQueryResult],
    ) -> Result<Vec<(i32, i32)>> {
        let mut build_batch_positions = Vec::with_capacity(index_query_results.len());
        let Some(probe_geom) = probe.try_to_geometry() else {
            return Ok(Vec::new());
        };

        for index_result in index_query_results {
            let (prepared_geom, is_newly_created) =
                self.prepared_geoms
                    .get_or_create(index_result.geom_idx, || {
                        let Some(build_geom) = index_result.wkb.try_to_geometry() else {
                            return Ok(None);
                        };
                        let prepared_geom = geo_generic_alg::PreparedGeometry::from(build_geom);
                        Ok(Some(prepared_geom))
                    })?;
            let Some(prepared_geom) = prepared_geom else {
                continue;
            };
            if is_newly_created {
                // TODO: This ia a rough estimate of the memory usage of the prepared geometry and
                // may not be accurate.
                let prep_geom_size = index_result.wkb.buf().len() * 4;
                self.mem_usage.fetch_add(prep_geom_size, Ordering::Relaxed);
            }
            if self.evaluator.evaluate_prepare_build(
                prepared_geom,
                &probe_geom,
                index_result.distance,
            )? {
                build_batch_positions.push(index_result.position);
            }
        }
        Ok(build_batch_positions)
    }

    fn refine_prepare_probe(
        &self,
        probe: &Wkb<'_>,
        index_query_results: &[IndexQueryResult],
    ) -> Result<Vec<(i32, i32)>> {
        let mut build_batch_positions = Vec::with_capacity(index_query_results.len());
        let Some(probe_geom) = probe.try_to_geometry() else {
            return Ok(Vec::new());
        };
        let probe_geom = geo_generic_alg::PreparedGeometry::from(probe_geom);

        for index_result in index_query_results {
            if self.evaluator.evaluate_prepare_probe(
                index_result.wkb,
                &probe_geom,
                index_result.distance,
            )? {
                build_batch_positions.push(index_result.position);
            }
        }
        Ok(build_batch_positions)
    }
}

fn select_optimal_mode(build_stats: &GeoStatistics, probe_stats: &GeoStatistics) -> ExecutionMode {
    let build_mean_points_per_geometry = build_stats.mean_points_per_geometry().unwrap_or(0.0);
    let probe_mean_points_per_geometry = probe_stats.mean_points_per_geometry().unwrap_or(0.0);
    let max_mean_points_per_geometry =
        build_mean_points_per_geometry.max(probe_mean_points_per_geometry);
    if max_mean_points_per_geometry <= 50.0 {
        // If the mean points per geometry is less than 50, the geometries are not complex enough to
        // benefit from prepared geometries.
        ExecutionMode::PrepareNone
    } else {
        // Choose a more complex side to prepare the geometries
        if build_mean_points_per_geometry > probe_mean_points_per_geometry {
            ExecutionMode::PrepareBuild
        } else {
            ExecutionMode::PrepareProbe
        }
    }
}

impl IndexQueryResultRefiner for GeoRefiner {
    fn refine(
        &self,
        probe: &Wkb<'_>,
        index_query_results: &[IndexQueryResult],
    ) -> Result<Vec<(i32, i32)>> {
        let exec_mode = self.actual_execution_mode();
        match exec_mode {
            ExecutionMode::PrepareNone => self.refine_prepare_none(probe, index_query_results),
            ExecutionMode::PrepareBuild => self.refine_prepare_build(probe, index_query_results),
            ExecutionMode::PrepareProbe => self.refine_prepare_probe(probe, index_query_results),
            ExecutionMode::Speculative(_) => {
                internal_err!(
                    "Speculative execution mode should be translated to other execution modes"
                )
            }
        }
    }

    fn mem_usage(&self) -> usize {
        self.mem_usage.load(Ordering::Relaxed)
    }

    fn actual_execution_mode(&self) -> ExecutionMode {
        get_or_update_execution_mode(
            &self.exec_mode,
            &self.exec_mode_selector,
            ExecutionMode::PrepareNone,
        )
    }

    fn need_more_probe_stats(&self) -> bool {
        self.exec_mode.get().is_none()
    }

    fn merge_probe_stats(&self, stats: GeoStatistics) {
        if let Some(selector) = self.exec_mode_selector.as_ref() {
            selector.merge_probe_stats(stats);
        }
    }
}

trait GeoPredicateEvaluator: Send + Sync {
    fn evaluate(&self, build: &Wkb, probe: &Wkb, distance: Option<f64>) -> Result<bool>;

    fn evaluate_prepare_build(
        &self,
        build: &geo_generic_alg::PreparedGeometry<'static, geo_types::Geometry>,
        probe: &geo_types::Geometry,
        distance: Option<f64>,
    ) -> Result<bool>;

    fn evaluate_prepare_probe(
        &self,
        build: &Wkb,
        probe: &geo_generic_alg::PreparedGeometry<'static, geo_types::Geometry>,
        distance: Option<f64>,
    ) -> Result<bool>;
}

fn create_evaluator(predicate: &SpatialPredicate) -> Box<dyn GeoPredicateEvaluator> {
    match predicate {
        SpatialPredicate::Distance(_) => Box::new(GeoDistance),
        SpatialPredicate::Relation(predicate) => match predicate.relation_type {
            SpatialRelationType::Intersects => Box::new(GeoIntersects),
            SpatialRelationType::Contains => Box::new(GeoContains),
            SpatialRelationType::Within => Box::new(GeoWithin),
            SpatialRelationType::Covers => Box::new(GeoCovers),
            SpatialRelationType::CoveredBy => Box::new(GeoCoveredBy),
            SpatialRelationType::Touches => Box::new(GeoTouches),
            SpatialRelationType::Crosses => Box::new(GeoCrosses),
            SpatialRelationType::Overlaps => Box::new(GeoOverlaps),
            SpatialRelationType::Equals => Box::new(GeoEquals),
        },
    }
}

struct GeoIntersects;

impl GeoPredicateEvaluator for GeoIntersects {
    fn evaluate(&self, build: &Wkb, probe: &Wkb, _distance: Option<f64>) -> Result<bool> {
        Ok(build.intersects(probe))
    }

    fn evaluate_prepare_build(
        &self,
        build: &geo_generic_alg::PreparedGeometry<'static, geo_types::Geometry>,
        probe: &geo_types::Geometry,
        _distance: Option<f64>,
    ) -> Result<bool> {
        Ok(build.relate(probe).is_intersects())
    }

    fn evaluate_prepare_probe(
        &self,
        build: &Wkb,
        probe: &geo_generic_alg::PreparedGeometry<'static, geo_types::Geometry>,
        _distance: Option<f64>,
    ) -> Result<bool> {
        let Some(build_geom) = build.try_to_geometry() else {
            return Ok(false);
        };
        Ok(probe.relate(&build_geom).is_intersects())
    }
}

struct GeoContains;

impl GeoPredicateEvaluator for GeoContains {
    fn evaluate(&self, build: &Wkb, probe: &Wkb, _distance: Option<f64>) -> Result<bool> {
        let Some(build_geom) = build.try_to_geometry() else {
            return Ok(false);
        };
        let Some(probe_geom) = probe.try_to_geometry() else {
            return Ok(false);
        };
        Ok(build_geom.contains(&probe_geom))
    }

    fn evaluate_prepare_build(
        &self,
        build: &geo_generic_alg::PreparedGeometry<'static, geo_types::Geometry>,
        probe: &geo_types::Geometry,
        _distance: Option<f64>,
    ) -> Result<bool> {
        Ok(build.relate(probe).is_contains())
    }

    fn evaluate_prepare_probe(
        &self,
        build: &Wkb,
        probe: &geo_generic_alg::PreparedGeometry<'static, geo_types::Geometry>,
        _distance: Option<f64>,
    ) -> Result<bool> {
        let Some(build_geom) = build.try_to_geometry() else {
            return Ok(false);
        };
        Ok(probe.relate(&build_geom).is_within())
    }
}

struct GeoWithin;

impl GeoPredicateEvaluator for GeoWithin {
    fn evaluate(&self, build: &Wkb, probe: &Wkb, _distance: Option<f64>) -> Result<bool> {
        let Some(build_geom) = build.try_to_geometry() else {
            return Ok(false);
        };
        let Some(probe_geom) = probe.try_to_geometry() else {
            return Ok(false);
        };
        Ok(build_geom.is_within(&probe_geom))
    }

    fn evaluate_prepare_build(
        &self,
        build: &geo_generic_alg::PreparedGeometry<'static, geo_types::Geometry>,
        probe: &geo_types::Geometry,
        _distance: Option<f64>,
    ) -> Result<bool> {
        Ok(build.relate(probe).is_within())
    }

    fn evaluate_prepare_probe(
        &self,
        build: &Wkb,
        probe: &geo_generic_alg::PreparedGeometry<'static, geo_types::Geometry>,
        _distance: Option<f64>,
    ) -> Result<bool> {
        let Some(build_geom) = build.try_to_geometry() else {
            return Ok(false);
        };
        Ok(probe.relate(&build_geom).is_contains())
    }
}

struct GeoDistance;

impl GeoPredicateEvaluator for GeoDistance {
    fn evaluate(&self, build: &Wkb, probe: &Wkb, distance: Option<f64>) -> Result<bool> {
        let Some(distance) = distance else {
            return Ok(false);
        };
        let Some(build_geom) = build.try_to_geometry() else {
            return Ok(false);
        };
        let Some(probe_geom) = probe.try_to_geometry() else {
            return Ok(false);
        };
        let euc = Euclidean;
        let dist = euc.distance(&build_geom, &probe_geom);
        Ok(dist <= distance)
    }

    fn evaluate_prepare_build(
        &self,
        build: &geo_generic_alg::PreparedGeometry<'static, geo_types::Geometry>,
        probe: &geo_types::Geometry,
        distance: Option<f64>,
    ) -> Result<bool> {
        let Some(distance) = distance else {
            return Ok(false);
        };
        let euc = Euclidean;
        let dist = euc.distance(build.geometry(), probe);
        Ok(dist <= distance)
    }

    fn evaluate_prepare_probe(
        &self,
        build: &Wkb,
        probe: &geo_generic_alg::PreparedGeometry<'static, geo_types::Geometry>,
        distance: Option<f64>,
    ) -> Result<bool> {
        let Some(distance) = distance else {
            return Ok(false);
        };
        let Some(build_geom) = build.try_to_geometry() else {
            return Ok(false);
        };
        let euc = Euclidean;
        let dist = euc.distance(&build_geom, probe.geometry());
        Ok(dist <= distance)
    }
}

/// Macro to generate relation evaluators that use the relate() method
macro_rules! impl_relate_evaluator {
    ($struct_name:ident, $geo_method:ident $(,)?) => {
        #[derive(Debug)]
        struct $struct_name;

        impl GeoPredicateEvaluator for $struct_name {
            fn evaluate(&self, build: &Wkb, probe: &Wkb, _distance: Option<f64>) -> Result<bool> {
                let Some(build_geom) = build.try_to_geometry() else {
                    return Ok(false);
                };
                let Some(probe_geom) = probe.try_to_geometry() else {
                    return Ok(false);
                };
                Ok(build_geom.relate(&probe_geom).$geo_method())
            }

            fn evaluate_prepare_build(
                &self,
                build: &geo_generic_alg::PreparedGeometry<'static, geo_types::Geometry>,
                probe: &geo_types::Geometry,
                _distance: Option<f64>,
            ) -> Result<bool> {
                Ok(build.relate(probe).$geo_method())
            }

            fn evaluate_prepare_probe(
                &self,
                build: &Wkb,
                probe: &geo_generic_alg::PreparedGeometry<'static, geo_types::Geometry>,
                _distance: Option<f64>,
            ) -> Result<bool> {
                let Some(build_geom) = build.try_to_geometry() else {
                    return Ok(false);
                };
                Ok(probe.relate(&build_geom).$geo_method())
            }
        }
    };
}

// Generate relate-based evaluators using the macro
impl_relate_evaluator!(GeoTouches, is_touches);
impl_relate_evaluator!(GeoCrosses, is_crosses);
impl_relate_evaluator!(GeoOverlaps, is_overlaps);
impl_relate_evaluator!(GeoCovers, is_covers);
impl_relate_evaluator!(GeoCoveredBy, is_coveredby);
impl_relate_evaluator!(GeoEquals, is_equal_topo);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spatial_predicate::{DistancePredicate, RelationPredicate, SpatialRelationType};
    use datafusion_common::JoinSide;
    use datafusion_common::ScalarValue;
    use datafusion_physical_expr::expressions::{Column, Literal};
    use datafusion_physical_expr::PhysicalExpr;
    use sedona_common::DEFAULT_SPECULATIVE_THRESHOLD;
    use std::sync::Arc;

    /// Helper function to create a dummy PhysicalExpr for testing
    fn create_dummy_column(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, index))
    }

    /// Helper function to create GeoStatistics with specific mean points per geometry
    fn create_geo_stats(total_geometries: i64, total_points: i64) -> GeoStatistics {
        GeoStatistics::empty()
            .with_total_geometries(total_geometries)
            .with_total_points(total_points)
    }

    /// Helper function to create a relation predicate
    fn create_relation_predicate(relation_type: SpatialRelationType) -> SpatialPredicate {
        SpatialPredicate::Relation(RelationPredicate::new(
            create_dummy_column("left_geom", 0),
            create_dummy_column("right_geom", 1),
            relation_type,
        ))
    }

    /// Helper function to create a distance predicate
    fn create_distance_predicate() -> SpatialPredicate {
        SpatialPredicate::Distance(DistancePredicate::new(
            create_dummy_column("left_geom", 0),
            create_dummy_column("right_geom", 1),
            Arc::new(Literal::new(ScalarValue::Float64(Some(100.0)))),
            JoinSide::None,
        ))
    }

    #[test]
    fn test_geo_refiner_distance_predicate_immediate_selection() {
        let predicate = create_distance_predicate();
        let build_stats = create_geo_stats(100, 1000);
        let options = SpatialJoinOptions {
            execution_mode: ExecutionMode::Speculative(DEFAULT_SPECULATIVE_THRESHOLD),
            ..Default::default()
        };

        let refiner = GeoRefiner::new(&predicate, options, 100, build_stats);

        // Distance predicate should immediately select PrepareNone
        assert_eq!(refiner.actual_execution_mode(), ExecutionMode::PrepareNone);
        assert!(!refiner.need_more_probe_stats());
    }

    #[test]
    fn test_geo_refiner_intersects_predicate_needs_stats() {
        let predicate = create_relation_predicate(SpatialRelationType::Intersects);
        let build_stats = create_geo_stats(100, 1000);
        let options = SpatialJoinOptions {
            execution_mode: ExecutionMode::Speculative(DEFAULT_SPECULATIVE_THRESHOLD),
            ..Default::default()
        };

        let refiner = GeoRefiner::new(&predicate, options, 100, build_stats);

        // Intersects predicate should need probe stats to determine execution mode
        assert!(refiner.need_more_probe_stats());

        // Before probe stats, should return default mode
        assert_eq!(refiner.actual_execution_mode(), ExecutionMode::PrepareNone);
    }

    #[test]
    fn test_geo_refiner_intersects_with_simple_geometries() {
        let predicate = create_relation_predicate(SpatialRelationType::Intersects);
        let build_stats = create_geo_stats(100, 1000); // 10 points per geometry (simple)
        let options = SpatialJoinOptions {
            execution_mode: ExecutionMode::Speculative(DEFAULT_SPECULATIVE_THRESHOLD),
            ..Default::default()
        };

        let refiner = GeoRefiner::new(&predicate, options, 100, build_stats);

        // Merge probe stats with simple geometries (20 points per geometry)
        let probe_stats = create_geo_stats(50, 1000); // 20 points per geometry
        refiner.merge_probe_stats(probe_stats);

        // With simple geometries (max 20 points per geometry < 50 threshold), should select PrepareNone
        assert_eq!(refiner.actual_execution_mode(), ExecutionMode::PrepareNone);
    }

    #[test]
    fn test_geo_refiner_intersects_with_complex_geometries_prefer_build() {
        let predicate = create_relation_predicate(SpatialRelationType::Intersects);
        let build_stats = create_geo_stats(100, 8000); // 80 points per geometry (complex)
        let options = SpatialJoinOptions {
            execution_mode: ExecutionMode::Speculative(50), // Use smaller threshold for testing
            ..Default::default()
        };

        let refiner = GeoRefiner::new(&predicate, options, 100, build_stats);

        // Merge probe stats with simpler geometries (30 points per geometry)
        let probe_stats = create_geo_stats(50, 1500); // 30 points per geometry
        refiner.merge_probe_stats(probe_stats);

        // Build side has more complex geometries (80 > 30), should select PrepareBuild
        assert_eq!(refiner.actual_execution_mode(), ExecutionMode::PrepareBuild);
    }

    #[test]
    fn test_geo_refiner_intersects_with_complex_geometries_prefer_probe() {
        let predicate = create_relation_predicate(SpatialRelationType::Intersects);
        let build_stats = create_geo_stats(100, 3000); // 30 points per geometry
        let options = SpatialJoinOptions {
            execution_mode: ExecutionMode::Speculative(50), // Use smaller threshold for testing
            ..Default::default()
        };

        let refiner = GeoRefiner::new(&predicate, options, 100, build_stats);

        // Merge probe stats with more complex geometries (80 points per geometry)
        let probe_stats = create_geo_stats(50, 4000); // 80 points per geometry
        refiner.merge_probe_stats(probe_stats);

        // Probe side has more complex geometries (80 > 30), should select PrepareProbe
        assert_eq!(refiner.actual_execution_mode(), ExecutionMode::PrepareProbe);
    }

    #[test]
    fn test_geo_refiner_non_speculative_mode() {
        let predicate = create_relation_predicate(SpatialRelationType::Intersects);
        let build_stats = create_geo_stats(100, 1000);
        let options = SpatialJoinOptions {
            execution_mode: ExecutionMode::PrepareBuild,
            ..Default::default()
        };

        let refiner = GeoRefiner::new(&predicate, options, 100, build_stats);

        // Non-speculative mode should immediately select the specified mode
        assert_eq!(refiner.actual_execution_mode(), ExecutionMode::PrepareBuild);
        assert!(!refiner.need_more_probe_stats());
    }

    #[test]
    fn test_geo_refiner_all_relation_types() {
        let relation_types = vec![
            SpatialRelationType::Intersects,
            SpatialRelationType::Contains,
            SpatialRelationType::Within,
            SpatialRelationType::Covers,
            SpatialRelationType::CoveredBy,
            SpatialRelationType::Touches,
            SpatialRelationType::Crosses,
            SpatialRelationType::Overlaps,
            SpatialRelationType::Equals,
        ];

        for relation_type in relation_types {
            let predicate = create_relation_predicate(relation_type);
            let build_stats = create_geo_stats(100, 1000);
            let options = SpatialJoinOptions {
                execution_mode: ExecutionMode::Speculative(DEFAULT_SPECULATIVE_THRESHOLD),
                ..Default::default()
            };

            let refiner = GeoRefiner::new(&predicate, options, 100, build_stats);

            // All relation types should need probe stats for GeoRefiner
            // Because GeoRefiner doesn't have predicate-specific optimizations
            assert!(
                refiner.need_more_probe_stats(),
                "Failed for relation type: {relation_type:?}"
            );
        }
    }

    #[test]
    fn test_geo_refiner_select_optimal_mode_function() {
        // Test the select_optimal_mode function directly
        let build_stats = create_geo_stats(100, 1000); // 10 points per geometry
        let probe_stats = create_geo_stats(50, 1000); // 20 points per geometry

        // Simple geometries (max 20 < 50 threshold) should select PrepareNone
        let result = select_optimal_mode(&build_stats, &probe_stats);
        assert_eq!(result, ExecutionMode::PrepareNone);

        // Complex geometries should select based on which side is more complex
        let complex_build_stats = create_geo_stats(100, 8000); // 80 points per geometry
        let simple_probe_stats = create_geo_stats(50, 1500); // 30 points per geometry

        let result = select_optimal_mode(&complex_build_stats, &simple_probe_stats);
        assert_eq!(result, ExecutionMode::PrepareBuild);

        let simple_build_stats = create_geo_stats(100, 3000); // 30 points per geometry
        let complex_probe_stats = create_geo_stats(50, 4000); // 80 points per geometry

        let result = select_optimal_mode(&simple_build_stats, &complex_probe_stats);
        assert_eq!(result, ExecutionMode::PrepareProbe);
    }

    #[test]
    fn test_geo_refiner_prepared_geom_array_sizing() {
        let predicate = create_relation_predicate(SpatialRelationType::Intersects);
        let build_stats = create_geo_stats(100, 1000);
        let num_build_geoms = 500;

        // Test PrepareBuild mode - should allocate array for all build geometries
        let options = SpatialJoinOptions {
            execution_mode: ExecutionMode::PrepareBuild,
            ..Default::default()
        };
        let refiner = GeoRefiner::new(&predicate, options, num_build_geoms, build_stats.clone());
        assert!(refiner.prepared_geoms.len() == num_build_geoms);

        // Test PrepareProbe mode - should not allocate array for build geometries
        let options = SpatialJoinOptions {
            execution_mode: ExecutionMode::PrepareProbe,
            ..Default::default()
        };
        let refiner = GeoRefiner::new(&predicate, options, num_build_geoms, build_stats.clone());
        assert!(refiner.prepared_geoms.is_empty());

        // Test PrepareNone mode - should not allocate array for build geometries
        let options = SpatialJoinOptions {
            execution_mode: ExecutionMode::PrepareNone,
            ..Default::default()
        };
        let refiner = GeoRefiner::new(&predicate, options, num_build_geoms, build_stats);
        assert!(refiner.prepared_geoms.is_empty());
    }
}
