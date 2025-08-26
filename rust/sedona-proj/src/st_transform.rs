use crate::transform::ProjCrsEngine;
use arrow_array::builder::BinaryBuilder;
use arrow_schema::DataType;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::ColumnarValue;
use geo_traits::to_geo::ToGeoGeometry;
use sedona_expr::scalar_udf::{ScalarKernelRef, SedonaScalarKernel};
use sedona_functions::executor::WkbExecutor;
use sedona_geometry::transform::{transform, CachingCrsEngine, CrsEngine, CrsTransform};
use sedona_geometry::wkb_factory::WKB_MIN_PROBABLE_BYTES;
use sedona_schema::crs::deserialize_crs;
use sedona_schema::datatypes::{Edges, SedonaType};
use std::rc::Rc;
use std::sync::Arc;
use wkb::reader::Wkb;

#[derive(Debug)]
struct STTransform {}

/// ST_Transform() implementation using the proj crate
pub fn st_transform_impl() -> ScalarKernelRef {
    Arc::new(STTransform {})
}

// CrsTransform backed by PROJ is not thread safe, so we define the cache as thread-local
// to avoid race conditions.
thread_local! {
    static PROJ_ENGINE: CachingCrsEngine<ProjCrsEngine> =
        CachingCrsEngine::new(ProjCrsEngine);
}

struct TransformArgIndexes {
    wkb: usize,
    first_crs: usize,
    second_crs: Option<usize>,
    lenient: Option<usize>,
}

impl TransformArgIndexes {
    fn new() -> Self {
        Self {
            wkb: 0,
            first_crs: 1,
            second_crs: None,
            lenient: None,
        }
    }
}

fn define_arg_indexes(arg_types: &[SedonaType], indexes: &mut TransformArgIndexes) {
    indexes.wkb = 0;
    indexes.first_crs = 1;

    for (i, arg_type) in arg_types.iter().enumerate().skip(2) {
        if *arg_type == SedonaType::Arrow(DataType::Utf8) {
            indexes.second_crs = Some(i);
        } else if *arg_type == SedonaType::Arrow(DataType::Boolean) {
            indexes.lenient = Some(i);
        }
    }
}

impl SedonaScalarKernel for STTransform {
    fn return_type(&self, _args: &[SedonaType]) -> Result<Option<SedonaType>, DataFusionError> {
        Err(DataFusionError::Internal(
            "Return type should only be called with args".to_string(),
        ))
    }
    fn return_type_from_args_and_scalars(
        &self,
        arg_types: &[SedonaType],
        scalar_args: &[Option<&ScalarValue>],
    ) -> Result<Option<SedonaType>> {
        let mut indexes = TransformArgIndexes::new();
        define_arg_indexes(arg_types, &mut indexes);

        let to_crs_opt = if let Some(second_crs_index) = indexes.second_crs {
            scalar_args.get(second_crs_index).unwrap()
        } else {
            scalar_args.get(indexes.first_crs).unwrap()
        };

        match to_crs_opt {
            Some(ScalarValue::Utf8(Some(to_crs))) => {
                let val = serde_json::Value::String(to_crs.to_string());
                let crs = deserialize_crs(&val)?;
                Ok(Some(SedonaType::Wkb(Edges::Planar, crs)))
            }
            _ => Ok(Some(SedonaType::Wkb(Edges::Planar, None))),
        }
    }

    fn invoke_batch(
        &self,
        arg_types: &[SedonaType],
        args: &[ColumnarValue],
    ) -> Result<ColumnarValue> {
        let executor = WkbExecutor::new(arg_types, args);
        let mut builder = BinaryBuilder::with_capacity(
            executor.num_iterations(),
            WKB_MIN_PROBABLE_BYTES * executor.num_iterations(),
        );

        let mut indexes = TransformArgIndexes::new();
        define_arg_indexes(arg_types, &mut indexes);

        let first_crs = get_scalar_str(args, indexes.first_crs).ok_or_else(|| {
            DataFusionError::Execution("First argument must be a scalar string".into())
        })?;

        let lenient = indexes
            .lenient
            .is_some_and(|i| get_scalar_bool(args, i).unwrap_or(false));

        let second_crs = if let Some(second_crs_index) = indexes.second_crs {
            get_scalar_str(args, second_crs_index)
        } else {
            None
        };

        let crs_from_geo = parse_source_crs(&arg_types[indexes.wkb])?;

        let transform = match &second_crs {
            Some(to_crs) => get_transform_crs_to_crs(&first_crs, to_crs)?,
            None => get_transform_to_crs(crs_from_geo, &first_crs, lenient)?,
        };

        executor.execute_wkb_void(|maybe_wkb| {
            match maybe_wkb {
                Some(wkb) => invoke_scalar(&wkb, transform.as_ref(), &mut builder)?,
                None => builder.append_null(),
            }

            Ok(())
        })?;

        executor.finish(Arc::new(builder.finish()))
    }
}

fn get_transform_to_crs(
    source_crs_opt: Option<String>,
    to_crs: &str,
    lenient: bool,
) -> Result<Rc<dyn CrsTransform>, DataFusionError> {
    let from_crs = match source_crs_opt {
        Some(crs) => crs,
        None if lenient => "EPSG:4326".to_string(),
        None => {
            return Err(DataFusionError::Execution(
                "Source CRS is required when transforming to a CRS".to_string(),
            ))
        }
    };
    get_transform_crs_to_crs(&from_crs, to_crs)
}

fn get_transform_crs_to_crs(
    from_crs: &str,
    to_crs: &str,
) -> Result<Rc<dyn CrsTransform>, DataFusionError> {
    PROJ_ENGINE
        .with(|engine| engine.get_transform_crs_to_crs(from_crs, to_crs, None, ""))
        .map_err(|err| DataFusionError::Execution(format!("Transform error: {err}")))
}

fn invoke_scalar(wkb: &Wkb, trans: &dyn CrsTransform, builder: &mut BinaryBuilder) -> Result<()> {
    let geo_geom = wkb.to_geometry();
    transform(&geo_geom, trans, builder)
        .map_err(|err| DataFusionError::Execution(format!("Transform error: {err}")))?;
    builder.append_value([]);
    Ok(())
}

fn parse_source_crs(source_type: &SedonaType) -> Result<Option<String>> {
    match source_type {
        SedonaType::Wkb(_, Some(crs)) | SedonaType::WkbView(_, Some(crs)) => {
            crs.to_authority_code()
        }
        _ => Ok(None),
    }
}

fn get_scalar_str(args: &[ColumnarValue], index: usize) -> Option<String> {
    if let Some(ColumnarValue::Scalar(ScalarValue::Utf8(opt_str))) = args.get(index) {
        opt_str.clone()
    } else {
        None
    }
}

fn get_scalar_bool(args: &[ColumnarValue], index: usize) -> Option<bool> {
    if let Some(ColumnarValue::Scalar(ScalarValue::Boolean(opt_bool))) = args.get(index) {
        *opt_bool
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::ArrayRef;
    use arrow_schema::{DataType, Field};
    use datafusion_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
    use rstest::rstest;
    use sedona_expr::scalar_udf::SedonaScalarUDF;
    use sedona_schema::crs::lnglat;
    use sedona_schema::crs::Crs;
    use sedona_schema::datatypes::WKB_GEOMETRY;
    use sedona_testing::compare::assert_value_equal;
    use sedona_testing::{create::create_array, create::create_array_value};

    const NAD83ZONE6PROJ: &str = "EPSG:2230";
    const WGS84: &str = "EPSG:4326";

    #[rstest]
    fn test_invoke_batch_with_geo_crs() {
        // From-CRS pulled from sedona type
        let arg_types = [
            SedonaType::Wkb(Edges::Planar, lnglat()),
            SedonaType::Arrow(DataType::Utf8),
        ];

        let wkb = create_array(&[None, Some("POINT (79.3871 43.6426)")], &arg_types[0]);

        let scalar_args = vec![ScalarValue::Utf8(Some(NAD83ZONE6PROJ.to_string()))];

        let expected = create_array_value(
            &[None, Some("POINT (-21508577.363421552 34067918.06097863)")],
            &SedonaType::Wkb(Edges::Planar, get_crs(NAD83ZONE6PROJ)),
        );

        let (result_type, result_col) =
            invoke_udf_test(wkb, scalar_args, arg_types.to_vec()).unwrap();
        assert_value_equal(&result_col, &expected);
        assert_eq!(
            result_type,
            SedonaType::Wkb(Edges::Planar, get_crs(NAD83ZONE6PROJ))
        );
    }

    #[rstest]
    fn test_invoke_batch_with_lenient() {
        let arg_types = [
            WKB_GEOMETRY,
            SedonaType::Arrow(DataType::Utf8),
            SedonaType::Arrow(DataType::Boolean),
        ];

        let wkb = create_array(&[None, Some("POINT (79.3871 43.6426)")], &WKB_GEOMETRY);
        let scalar_args = vec![
            ScalarValue::Utf8(Some(NAD83ZONE6PROJ.to_string())),
            ScalarValue::Boolean(Some(true)),
        ];

        let expected = create_array_value(
            &[None, Some("POINT (-21508577.363421552 34067918.06097863)")],
            &SedonaType::Wkb(Edges::Planar, Some(get_crs(NAD83ZONE6PROJ).unwrap())),
        );

        let (result_type, result_col) =
            invoke_udf_test(wkb, scalar_args, arg_types.to_vec()).unwrap();
        assert_value_equal(&result_col, &expected);
        assert_eq!(
            result_type,
            SedonaType::Wkb(Edges::Planar, Some(get_crs(NAD83ZONE6PROJ).unwrap()))
        );
    }

    #[rstest]
    fn test_invoke_batch_one_crs_no_lenient() {
        let arg_types = [WKB_GEOMETRY, SedonaType::Arrow(DataType::Utf8)];

        let wkb = create_array(&[None, Some("POINT (79.3871 43.6426)")], &WKB_GEOMETRY);
        let scalar_args = vec![ScalarValue::Utf8(Some(NAD83ZONE6PROJ.to_string()))];

        let err = invoke_udf_test(wkb, scalar_args, arg_types.to_vec());
        assert!(
            matches!(err, Err(DataFusionError::Execution(_))),
            "Expected an Execution error"
        );
    }

    #[rstest]
    fn test_invoke_batch_with_string_source() {
        let arg_types = [
            WKB_GEOMETRY,
            SedonaType::Arrow(DataType::Utf8),
            SedonaType::Arrow(DataType::Utf8),
        ];

        let wkb = create_array(&[None, Some("POINT (79.3871 43.6426)")], &WKB_GEOMETRY);

        let scalar_args = vec![
            ScalarValue::Utf8(Some(WGS84.to_string())),
            ScalarValue::Utf8(Some(NAD83ZONE6PROJ.to_string())),
        ];

        // Note: would be nice to have an epsilon of tolerance when validating
        let expected = create_array_value(
            &[None, Some("POINT (-21508577.363421552 34067918.06097863)")],
            &SedonaType::Wkb(Edges::Planar, Some(get_crs(NAD83ZONE6PROJ).unwrap())),
        );

        let (result_type, result_col) =
            invoke_udf_test(wkb, scalar_args, arg_types.to_vec()).unwrap();
        assert_value_equal(&result_col, &expected);
        assert_eq!(
            result_type,
            SedonaType::Wkb(Edges::Planar, Some(get_crs(NAD83ZONE6PROJ).unwrap()))
        );
    }

    fn get_crs(auth_code: &str) -> Crs {
        let crs_value = serde_json::Value::String(auth_code.to_string());
        deserialize_crs(&crs_value).unwrap()
    }

    fn invoke_udf_test(
        wkb: ArrayRef,
        scalar_args: Vec<ScalarValue>,
        arg_types: Vec<SedonaType>,
    ) -> Result<(SedonaType, ColumnarValue)> {
        let udf = SedonaScalarUDF::from_kernel("st_transform", st_transform_impl());

        let arg_fields: Vec<Arc<Field>> = arg_types
            .into_iter()
            .map(|arg_type| Arc::new(Field::new("", arg_type.data_type(), true)))
            .collect();
        let row_count = wkb.len();

        let mut scalar_args_fields: Vec<Option<&ScalarValue>> = vec![None];
        let mut arg_vals: Vec<ColumnarValue> = vec![ColumnarValue::Array(Arc::new(wkb))];

        for scalar_arg in &scalar_args {
            scalar_args_fields.push(Some(scalar_arg));
            arg_vals.push(scalar_arg.clone().into());
        }

        let return_field_args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_args_fields,
        };

        let return_field = udf.return_field_from_args(return_field_args)?;
        let return_type = SedonaType::from_data_type(return_field.data_type())?;

        let args = ScalarFunctionArgs {
            args: arg_vals,
            arg_fields: arg_fields.to_vec(),
            number_rows: row_count,
            return_field,
        };

        let value = udf.invoke_with_args(args)?;
        Ok((return_type, value))
    }
}
