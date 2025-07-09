use std::{
    marker::PhantomData,
    sync::atomic::{AtomicUsize, Ordering},
};

use datafusion_common::{DataFusionError, Result};
use sedona_common::{ExecutionMode, SpatialJoinOptions, TgIndexType};
use sedona_tg::tg::{self, BinaryPredicate};
use wkb::reader::Wkb;

use crate::{
    index::IndexQueryResult,
    init_once_array::InitOnceArray,
    refine::IndexQueryResultRefiner,
    spatial_predicate::{SpatialPredicate, SpatialRelationType},
};

/// A refiner that uses the tiny geometry library to evaluate spatial predicates.
pub(crate) struct TgRefiner {
    evaluator: Box<dyn TgPredicateEvaluator>,
    prepared_geoms: InitOnceArray<Option<tg::Geom>>,
    options: SpatialJoinOptions,
    index_type: tg::IndexType,
    mem_usage: AtomicUsize,
}

impl TgRefiner {
    pub fn try_new(
        predicate: &SpatialPredicate,
        options: SpatialJoinOptions,
        num_build_geoms: usize,
    ) -> Result<Self> {
        let evaluator: Box<dyn TgPredicateEvaluator> = match predicate {
            SpatialPredicate::Distance(_) => {
                return Err(DataFusionError::Internal(
                    "Distance predicate is not supported for TG".to_string(),
                ))
            }
            SpatialPredicate::Relation(predicate) => match predicate.relation_type {
                SpatialRelationType::Intersects => {
                    Box::new(TgPredicateEvaluatorImpl::<tg::Intersects>::new())
                }
                SpatialRelationType::Contains => {
                    Box::new(TgPredicateEvaluatorImpl::<tg::Contains>::new())
                }
                SpatialRelationType::Within => {
                    Box::new(TgPredicateEvaluatorImpl::<tg::Within>::new())
                }
                SpatialRelationType::Covers => {
                    Box::new(TgPredicateEvaluatorImpl::<tg::Covers>::new())
                }
                SpatialRelationType::CoveredBy => {
                    Box::new(TgPredicateEvaluatorImpl::<tg::CoveredBy>::new())
                }
                SpatialRelationType::Touches => {
                    Box::new(TgPredicateEvaluatorImpl::<tg::Touches>::new())
                }
                SpatialRelationType::Equals => {
                    Box::new(TgPredicateEvaluatorImpl::<tg::Equals>::new())
                }
                _ => {
                    return Err(DataFusionError::Internal(
                        "Unsupported spatial relation type for TG".to_string(),
                    ))
                }
            },
        };
        let index_type = match options.tg.index_type {
            TgIndexType::Natural => tg::IndexType::Natural,
            TgIndexType::YStripes => tg::IndexType::YStripes,
        };
        let prepared_geoms = InitOnceArray::new(num_build_geoms);
        let mem_usage = prepared_geoms.allocated_size();
        Ok(Self {
            evaluator,
            prepared_geoms,
            options,
            index_type,
            mem_usage: AtomicUsize::new(mem_usage),
        })
    }

    fn refine_prepare_build(
        &self,
        probe: &wkb::reader::Wkb<'_>,
        index_query_results: &[IndexQueryResult],
    ) -> Result<Vec<(i32, i32)>> {
        let mut build_batch_positions = Vec::with_capacity(index_query_results.len());
        let probe_geom = tg::Geom::parse_wkb(probe.buf(), self.index_type)?;
        for index_result in index_query_results {
            let (build_geom, is_newly_created) =
                self.prepared_geoms
                    .get_or_create(index_result.geom_idx, || {
                        let geom = tg::Geom::parse_wkb(index_result.wkb.buf(), self.index_type)?;
                        Ok(Some(geom))
                    })?;
            let Some(build_geom) = build_geom else {
                continue;
            };
            if is_newly_created {
                let prep_geom_size = build_geom.memsize();
                self.mem_usage.fetch_add(prep_geom_size, Ordering::Relaxed);
            }
            if self
                .evaluator
                .evaluate(build_geom, &probe_geom, index_result.distance)?
            {
                build_batch_positions.push(index_result.position);
            }
        }
        Ok(build_batch_positions)
    }

    fn refine_not_prepare_build(
        &self,
        probe: &wkb::reader::Wkb<'_>,
        index_query_results: &[IndexQueryResult],
        probe_index_type: tg::IndexType,
    ) -> Result<Vec<(i32, i32)>> {
        let mut build_batch_positions = Vec::with_capacity(index_query_results.len());
        let probe_geom = tg::Geom::parse_wkb(probe.buf(), probe_index_type)?;
        for index_result in index_query_results {
            let build_geom = tg::Geom::parse_wkb(index_result.wkb.buf(), tg::IndexType::Unindexed)?;
            if self
                .evaluator
                .evaluate(&build_geom, &probe_geom, index_result.distance)?
            {
                build_batch_positions.push(index_result.position);
            }
        }
        Ok(build_batch_positions)
    }
}

impl IndexQueryResultRefiner for TgRefiner {
    fn refine(
        &self,
        probe: &Wkb<'_>,
        index_query_results: &[IndexQueryResult],
    ) -> Result<Vec<(i32, i32)>> {
        match self.options.execution_mode {
            ExecutionMode::PrepareNone => {
                self.refine_not_prepare_build(probe, index_query_results, tg::IndexType::Unindexed)
            }
            ExecutionMode::PrepareBuild => self.refine_prepare_build(probe, index_query_results),
            ExecutionMode::PrepareProbe => {
                self.refine_not_prepare_build(probe, index_query_results, self.index_type)
            }
            ExecutionMode::Speculative(_) => {
                unimplemented!("Speculative execution mode is not implemented")
            }
        }
    }

    fn mem_usage(&self) -> usize {
        // TODO: Implement memory usage accounting
        0
    }
}

trait TgPredicateEvaluator: Send + Sync {
    fn evaluate(&self, build: &tg::Geom, probe: &tg::Geom, distance: Option<f64>) -> Result<bool>;
}

struct TgPredicateEvaluatorImpl<Op: BinaryPredicate + Send + Sync> {
    _marker: PhantomData<Op>,
}

impl<Op: BinaryPredicate + Send + Sync> TgPredicateEvaluatorImpl<Op> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<Op: BinaryPredicate + Send + Sync> TgPredicateEvaluator for TgPredicateEvaluatorImpl<Op> {
    fn evaluate(&self, build: &tg::Geom, probe: &tg::Geom, _distance: Option<f64>) -> Result<bool> {
        Ok(Op::evaluate(build, probe))
    }
}
