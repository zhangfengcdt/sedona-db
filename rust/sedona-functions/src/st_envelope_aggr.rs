use std::{sync::Arc, vec};

use crate::executor::GenericExecutor;
use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use datafusion_common::{
    error::{DataFusionError, Result},
    internal_err, ScalarValue,
};
use datafusion_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, Accumulator, ColumnarValue, Documentation, Volatility,
};
use sedona_expr::{
    aggregate_udf::{SedonaAccumulator, SedonaAggregateUDF},
    scalar_udf::ArgMatcher,
};
use sedona_geometry::{
    bounds::geo_traits_update_xy_bounds,
    interval::{Interval, IntervalTrait},
    wkb_factory::{wkb_linestring, wkb_point, wkb_polygon},
};
use sedona_schema::datatypes::{Edges, SedonaType, WKB_GEOMETRY};

/// ST_Envelope_Aggr() aggregate UDF implementation
///
/// An implementation of envelope (bounding shape) calculation.
pub fn st_envelope_aggr_udf() -> SedonaAggregateUDF {
    SedonaAggregateUDF::new(
        "st_envelope_aggr",
        vec![Arc::new(STEnvelopeAggr {})],
        Volatility::Immutable,
        Some(st_envelope_aggr_doc()),
    )
}

fn st_envelope_aggr_doc() -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Return the entire envelope boundary of all geometries in geom",
        "SELECT ST_Envelope_Aggr(ST_GeomFromWKT('MULTIPOINT (0 1, 10 11)'))",
    )
    .with_argument("geom", "geometry: Input geometry or geography")
    .build()
}

#[derive(Debug)]
struct STEnvelopeAggr {}

impl SedonaAccumulator for STEnvelopeAggr {
    fn return_type(&self, args: &[SedonaType]) -> Result<Option<SedonaType>> {
        let matcher = ArgMatcher::new(vec![ArgMatcher::is_geometry()], WKB_GEOMETRY);
        matcher.match_args(args)
    }

    fn accumulator(
        &self,
        args: &[SedonaType],
        output_type: &SedonaType,
    ) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(BoundsAccumulator2D::new(
            args[0].clone(),
            output_type.clone(),
        )))
    }

    fn state_fields(&self, _args: &[SedonaType]) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(
            WKB_GEOMETRY.to_storage_field("envelope", true)?,
        )])
    }
}

#[derive(Debug)]
struct BoundsAccumulator2D {
    input_type: SedonaType,
    output_type: SedonaType,
    x: Interval,
    y: Interval,
}

impl BoundsAccumulator2D {
    pub fn new(input_type: SedonaType, output_type: SedonaType) -> Self {
        Self {
            input_type,
            output_type,
            x: Interval::empty(),
            y: Interval::empty(),
        }
    }

    // Create a WKB result based on the current state of the accumulator.
    fn make_wkb_result(&self) -> Result<Option<Vec<u8>>> {
        if self.x.is_empty() || self.y.is_empty() {
            return Ok(None);
        }

        let wkb = match (self.x.width() > 0.0, self.y.width() > 0.0) {
            (true, true) => {
                // Extent has height and width: return a POLYGON
                wkb_polygon(
                    [
                        (self.x.lo(), self.y.lo()),
                        (self.x.hi(), self.y.lo()),
                        (self.x.hi(), self.y.hi()),
                        (self.x.lo(), self.y.hi()),
                        (self.x.lo(), self.y.lo()),
                    ]
                    .into_iter(),
                )
                .map_err(|e| DataFusionError::External(Box::new(e)))?
            }
            (false, true) | (true, false) => {
                // Extent has only height or width: return a vertical or horizontal line
                wkb_linestring([(self.x.lo(), self.y.lo()), (self.x.hi(), self.y.hi())].into_iter())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
            }
            (false, false) => {
                // Extent has no height or width: return a point
                wkb_point((self.x.lo(), self.y.lo()))
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
            }
        };

        Ok(Some(wkb))
    }

    // Check the input length for update methods.
    fn check_update_input_len(input: &[ArrayRef], expected: usize, context: &str) -> Result<()> {
        if input.is_empty() {
            return Err(DataFusionError::Internal(format!(
                "No input arrays provided to accumulator in {context}"
            )));
        }
        if input.len() != expected {
            return internal_err!(
                "Unexpected input length in {} (expected {}, got {})",
                context,
                expected,
                input.len()
            );
        }
        Ok(())
    }

    // Execute the update operation for the accumulator.
    fn execute_update(&mut self, executor: GenericExecutor) -> Result<(), DataFusionError> {
        executor.execute_wkb_void(|_i, maybe_item| {
            if let Some(item) = maybe_item {
                geo_traits_update_xy_bounds(item, &mut self.x, &mut self.y)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            }
            Ok(())
        })?;
        Ok(())
    }
}

impl Accumulator for BoundsAccumulator2D {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        Self::check_update_input_len(values, 1, "update_batch")?;
        let arg_types = [SedonaType::Wkb(Edges::Planar, None)];
        let args = [ColumnarValue::Array(
            self.input_type.unwrap_array(&values[0])?,
        )];
        let executor = GenericExecutor::new(&arg_types, &args);
        self.execute_update(executor)?;
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let wkb = self.make_wkb_result()?;
        let scalar = ScalarValue::Binary(wkb);
        self.output_type.wrap_scalar(&scalar)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let wkb = self.make_wkb_result()?;
        Ok(vec![ScalarValue::Binary(wkb)])
    }

    fn size(&self) -> usize {
        size_of::<BoundsAccumulator2D>()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        Self::check_update_input_len(states, 1, "merge_batch")?;
        let array = &states[0];
        let args = [ColumnarValue::Array(array.clone())];
        let arg_types = [SedonaType::Wkb(Edges::Planar, None)];
        let executor = GenericExecutor::new(&arg_types, &args);
        self.execute_update(executor)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use datafusion_expr::AggregateUDF;
    use sedona_expr::aggregate_udf::AggregateTester;
    use sedona_testing::{
        compare::assert_scalar_equal,
        create::{create_array, create_scalar},
    };

    use super::*;

    #[test]
    fn udf_metadata() {
        let udf: AggregateUDF = st_envelope_aggr_udf().into();
        assert_eq!(udf.name(), "st_envelope_aggr");
        assert!(udf.documentation().is_some());
    }

    #[test]
    fn udf() {
        let tester = AggregateTester::new(st_envelope_aggr_udf().into(), vec![WKB_GEOMETRY]);
        assert_eq!(tester.return_type().unwrap(), WKB_GEOMETRY);

        // Finite input with nulls
        let batches = vec![
            create_array(
                &[Some("POINT (0 1)"), None, Some("POINT (2 3)")],
                &WKB_GEOMETRY,
            ),
            create_array(
                &[Some("POINT (4 5)"), None, Some("POINT (6 7)")],
                &WKB_GEOMETRY,
            ),
        ];

        assert_scalar_equal(
            &tester.aggregate(batches).unwrap(),
            &create_scalar(Some("POLYGON((0 1,6 1,6 7,0 7,0 1))"), &WKB_GEOMETRY),
        );

        // Empty input
        assert_scalar_equal(
            &tester.aggregate(vec![]).unwrap(),
            &create_scalar(None, &WKB_GEOMETRY),
        );

        // All coordinates empty
        assert_scalar_equal(
            &tester
                .aggregate(vec![create_array(&[Some("POINT EMPTY")], &WKB_GEOMETRY)])
                .unwrap(),
            &create_scalar(None, &WKB_GEOMETRY),
        );

        // Degenerate output: point
        assert_scalar_equal(
            &tester
                .aggregate(vec![create_array(&[Some("POINT (0 1)")], &WKB_GEOMETRY)])
                .unwrap(),
            &create_scalar(Some("POINT (0 1)"), &WKB_GEOMETRY),
        );

        // Degenerate output: vertical line
        assert_scalar_equal(
            &tester
                .aggregate(vec![create_array(
                    &[Some("MULTIPOINT (0 2, 0 1)")],
                    &WKB_GEOMETRY,
                )])
                .unwrap(),
            &create_scalar(Some("LINESTRING (0 1, 0 2)"), &WKB_GEOMETRY),
        );

        // Degenerate output: horizontal line
        assert_scalar_equal(
            &tester
                .aggregate(vec![create_array(
                    &[Some("MULTIPOINT (1 1, 0 1)")],
                    &WKB_GEOMETRY,
                )])
                .unwrap(),
            &create_scalar(Some("LINESTRING (0 1, 1 1)"), &WKB_GEOMETRY),
        );
    }
}
