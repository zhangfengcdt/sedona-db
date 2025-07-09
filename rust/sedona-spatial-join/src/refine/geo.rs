use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion_common::Result;
use geo_generic_alg::{Contains, Distance, Euclidean, Intersects, Relate, Within};
use geo_traits::to_geo::ToGeoGeometry;
use sedona_common::{ExecutionMode, SpatialJoinOptions};
use wkb::reader::Wkb;

use crate::{
    index::IndexQueryResult,
    init_once_array::InitOnceArray,
    refine::IndexQueryResultRefiner,
    spatial_predicate::{SpatialPredicate, SpatialRelationType},
};

/// A refiner that uses the geo library to evaluate spatial predicates.
pub(crate) struct GeoRefiner {
    evaluator: Box<dyn GeoPredicateEvaluator>,
    prepared_geoms:
        InitOnceArray<Option<geo_generic_alg::PreparedGeometry<'static, geo_types::Geometry>>>,
    options: SpatialJoinOptions,
    mem_usage: AtomicUsize,
}

impl GeoRefiner {
    pub fn new(
        predicate: &SpatialPredicate,
        options: SpatialJoinOptions,
        num_build_geoms: usize,
    ) -> Self {
        let evaluator: Box<dyn GeoPredicateEvaluator> = match predicate {
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
        };
        let prepared_geom_array_size = if matches!(
            options.execution_mode,
            ExecutionMode::PrepareBuild | ExecutionMode::Speculative(_)
        ) {
            num_build_geoms
        } else {
            0
        };

        let prepared_geoms = InitOnceArray::new(prepared_geom_array_size);
        let mem_usage = prepared_geoms.allocated_size();
        Self {
            evaluator,
            prepared_geoms,
            options,
            mem_usage: AtomicUsize::new(mem_usage),
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

impl IndexQueryResultRefiner for GeoRefiner {
    fn refine(
        &self,
        probe: &Wkb<'_>,
        index_query_results: &[IndexQueryResult],
    ) -> Result<Vec<(i32, i32)>> {
        match self.options.execution_mode {
            ExecutionMode::PrepareNone => self.refine_prepare_none(probe, index_query_results),
            ExecutionMode::PrepareBuild => self.refine_prepare_build(probe, index_query_results),
            ExecutionMode::PrepareProbe => self.refine_prepare_probe(probe, index_query_results),
            ExecutionMode::Speculative(_) => {
                unimplemented!("Speculative execution mode is not implemented")
            }
        }
    }

    fn mem_usage(&self) -> usize {
        self.mem_usage.load(Ordering::Relaxed)
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
