use arrow_array::{ArrayRef, BinaryArray};
use datafusion_common::cast::as_binary_array;
use datafusion_common::error::Result;
use datafusion_common::{internal_err, DataFusionError, ScalarValue};
use geo_traits::GeometryTrait;
use sedona_schema::datatypes::SedonaPhysicalType;
use wkb::reader::read_wkb;

pub fn try_iter_wkb_array<'a>(
    physical_type: &SedonaPhysicalType,
    array: &'a ArrayRef,
) -> Result<impl ExactSizeIterator<Item = Option<Result<impl GeometryTrait<T = f64> + 'a>>>> {
    match physical_type {
        SedonaPhysicalType::Wkb(_, _) => {
            let binary_array = as_binary_array(array)?;
            Ok(iter_wkb_array(binary_array))
        }
        _ => internal_err!("Can't iterate over {:?} as GeometryTrait", physical_type),
    }
}

pub fn iter_wkb_array(
    array: &BinaryArray,
) -> impl ExactSizeIterator<Item = Option<Result<impl GeometryTrait<T = f64> + '_>>> {
    array.iter().map(parse_wkb_item)
}

pub fn try_iter_wkb_scalar(
    scalar: &ScalarValue,
    n: usize,
) -> Result<impl ExactSizeIterator<Item = Option<Result<impl GeometryTrait<T = f64> + '_>>>> {
    match scalar {
        ScalarValue::Binary(maybe_item) | ScalarValue::BinaryView(maybe_item) => {
            // This is a bummer, wkb and geo_traits are constructed with rather severe
            // constraints such that we have to parse this every time.
            Ok((0..n).map(move |_| match maybe_item {
                Some(vec_value) => parse_wkb_item(Some(vec_value)),
                None => None,
            }))
        }
        _ => {
            internal_err!("Can't iterate over {:?} as WKB", scalar)
        }
    }
}

fn parse_wkb_item(maybe_item: Option<&[u8]>) -> Option<Result<impl GeometryTrait<T = f64> + '_>> {
    match maybe_item {
        Some(item) => {
            let geometry = read_wkb(item).map_err(|err| DataFusionError::External(Box::new(err)));
            Some(geometry)
        }
        None => None,
    }
}
