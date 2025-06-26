use std::iter::zip;

use arrow_array::ArrayRef;
use datafusion_common::cast::{as_binary_array, as_binary_view_array};
use datafusion_common::error::Result;
use datafusion_common::{internal_err, DataFusionError, ScalarValue};
use datafusion_expr::ColumnarValue;
use sedona_schema::datatypes::SedonaType;
use wkb::reader::Wkb;

/// Helper for writing general kernel implementations with geometry
///
/// The GenericExecutor wraps a set of arguments and their types and provides helpers
/// to make writing general compute functions less verbose. Broadly, kernel implementations
/// must consider multiple input data types (e.g., Wkb/WkbView or Float32/Float64) and
/// multiple combinations of Array or ScalarValue inputs. The pattern supported by the
/// GenericExecutor is:
///
/// - Create a GenericExecutor with `new()`
/// - Create an Arrow builder of the appropriate output type using
///   `with_capacity(executor.num_iterations()`
/// - Use `execute_wkb()` with a lambda whose contents appends to the builder
/// - Use `finish()` to build the output [ColumnarValue].
///
/// When all arguments are scalars, `execute_wkb()` will perform one iteration
/// and `finish()` will return a [ColumnarValue::Scalar]. Otherwise, the output will
/// be a [ColumnarValue::Array] built from `num_iterations()` iterations. This is true
/// even if a geometry scalar is passed with a non-geometry argument. Non-geometry
/// arrays are typically cheaper to cast to a concrete type (e.g., cast a numeric
/// to float64 or an integer to int64) and cheaper to access by element or iterator
/// compared to most geometry operations that require them.
///
/// The GenericExecutor is not built to be completely general and UDF implementers are
/// free to use other mechanisms to implement UDFs with geometry arguments. The balance
/// between optimizing iteration speed, minimizing dispatch overhead, and maximizing
/// readability of kernel implementations is difficult to achieve and future utilities
/// may provide alternatives optimized for a different balance than was chosen here.
pub struct GenericExecutor<'a, 'b> {
    arg_types: &'a [SedonaType],
    args: &'b [ColumnarValue],
    num_iterations: usize,
}

impl<'a, 'b> GenericExecutor<'a, 'b> {
    /// Create a new [GenericExecutor]
    pub fn new(arg_types: &'a [SedonaType], args: &'b [ColumnarValue]) -> GenericExecutor<'a, 'b> {
        Self {
            arg_types,
            args,
            num_iterations: Self::calc_num_iterations(args),
        }
    }

    /// Return the number of iterations that will be performed `execute_*()` methods
    ///
    /// If all arguments are [ColumnarValue::Scalar]s, this will be one iteration.
    /// Otherwise, it will be the length of the first array.
    pub fn num_iterations(&self) -> usize {
        self.num_iterations
    }

    /// Execute a function by iterating over [Wkb] scalars in the first argument
    ///
    /// Provides a mechanism to iterate over a geometry array by converting each to
    /// a [Wkb] scalar. For [SedonaType::Wkb] and [SedonaType::WkbView] arrays, this
    /// is the conversion that would normally happen. For other future supported geometry
    /// array types, this may incur a cast of item-wise conversion overhead.
    pub fn execute_wkb_void<F: FnMut(usize, Option<&Wkb>) -> Result<()>>(
        &self,
        func: F,
    ) -> Result<()> {
        self.args[0].iter_as_wkb(&self.arg_types[0], self.num_iterations, func)
    }

    /// Execute a binary geometry function by iterating over [Wkb] scalars in the
    /// first two arguments
    ///
    /// Provides a mechanism to iterate over two geometry arrays as pairs of [Wkb]
    /// scalars. [SedonaType::Wkb] and [SedonaType::WkbView] arrays are iterated over
    /// in place; however, future supported geometry array types may incur conversion
    /// overhead.
    pub fn execute_wkb_wkb_void<F: FnMut(usize, Option<&Wkb>, Option<&Wkb>) -> Result<()>>(
        &self,
        mut func: F,
    ) -> Result<()> {
        match (&self.args[0], &self.args[1]) {
            (ColumnarValue::Array(array0), ColumnarValue::Array(array1)) => iter_wkb_wkb_array(
                (&self.arg_types[0], &self.arg_types[1]),
                (array0, array1),
                func,
            ),
            (ColumnarValue::Array(array), ColumnarValue::Scalar(scalar_value)) => {
                let wkb1 = scalar_value.scalar_as_wkb()?;
                array.iter_as_wkb(&self.arg_types[0], self.num_iterations(), |i, wkb0| {
                    func(i, wkb0, wkb1.as_ref())
                })
            }
            (ColumnarValue::Scalar(scalar_value), ColumnarValue::Array(array)) => {
                let wkb0 = scalar_value.scalar_as_wkb()?;
                array.iter_as_wkb(&self.arg_types[1], self.num_iterations(), |i, wkb1| {
                    func(i, wkb0.as_ref(), wkb1)
                })
            }
            (ColumnarValue::Scalar(scalar_value0), ColumnarValue::Scalar(scalar_value1)) => {
                let wkb0 = scalar_value0.scalar_as_wkb()?;
                let wkb1 = scalar_value1.scalar_as_wkb()?;
                func(0, wkb0.as_ref(), wkb1.as_ref())
            }
        }
    }

    /// Finish an [ArrayRef] output as the appropriate [ColumnarValue]
    ///
    /// Converts the output of `finish()`ing an Arrow builder into a
    /// [ColumnarValue::Scalar] if all arguments were scalars, or a
    /// [ColumnarValue::Array] otherwise.
    pub fn finish(&self, out: ArrayRef) -> Result<ColumnarValue> {
        for arg in self.args {
            match arg {
                // If any argument was an array, we return an array
                ColumnarValue::Array(_) => {
                    return Ok(ColumnarValue::Array(out));
                }
                ColumnarValue::Scalar(_) => {}
            }
        }

        // For all scalar arguments, we return a scalar
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
    }

    /// Calculates the number of iterations that should happen based on the
    /// argument ColumnarValue types
    fn calc_num_iterations(args: &[ColumnarValue]) -> usize {
        for arg in args {
            match arg {
                // If any argument is an array, we have to iterate array.len() times
                ColumnarValue::Array(array) => {
                    return array.len();
                }
                ColumnarValue::Scalar(_) => {}
            }
        }

        // For all scalar arguments, we iterate once
        1
    }
}

/// Trait for iterating over a container type as geometry scalars
///
/// Currently the only scalar type supported is [Wkb]; however, for future
/// geometry array types it may make sense to offer other scalar types over
/// which to iterate.
pub trait IterGeo {
    /// Apply a function for each element of self as an optional [Wkb]
    ///
    /// The function will always be called num_iteration types to support
    /// efficient iteration over scalar containers (e.g., so that implementations
    /// can parse the Wkb once and reuse the object).
    fn iter_as_wkb<F: FnMut(usize, Option<&Wkb>) -> Result<()>>(
        &self,
        sedona_type: &SedonaType,
        num_iterations: usize,
        func: F,
    ) -> Result<()>;
}

/// Trait for obtaining geometry scalars from containers
///
/// Currently this is internal and only implemented for the [ScalarValue].
trait ScalarGeo {
    fn scalar_as_wkb(&self) -> Result<Option<Wkb<'_>>>;
}

impl IterGeo for ArrayRef {
    fn iter_as_wkb<F: FnMut(usize, Option<&Wkb>) -> Result<()>>(
        &self,
        sedona_type: &SedonaType,
        num_iterations: usize,
        func: F,
    ) -> Result<()> {
        if num_iterations != self.len() {
            return internal_err!(
                "Expected {num_iterations} items but got Array with {} items",
                self.len()
            );
        }

        match sedona_type {
            SedonaType::Wkb(_, _) => iter_wkb_binary(as_binary_array(self)?, func),
            SedonaType::WkbView(_, _) => iter_wkb_binary(as_binary_view_array(self)?, func),
            _ => {
                // We could cast here as a fallback, iterate and cast per-element, or
                // implement iter_as_something_else()/supports_iter_xxx() when more geo array types
                // are supported.
                internal_err!("Can't iterate over {:?} as Wkb", sedona_type)
            }
        }
    }
}

impl IterGeo for ScalarValue {
    fn iter_as_wkb<F: FnMut(usize, Option<&Wkb>) -> Result<()>>(
        &self,
        sedona_type: &SedonaType,
        num_iterations: usize,
        mut func: F,
    ) -> Result<()> {
        if !matches!(
            sedona_type,
            SedonaType::Wkb(_, _) | SedonaType::WkbView(_, _)
        ) {
            // We could cast here as a fallback, iterate and cast per-element, or
            // implement iter_as_something_else()/supports_iter_xxx() when more geo array types
            // are supported.
            return internal_err!("Can't iterate over {:?} type as WKB", self);
        }

        let wkb = self.scalar_as_wkb()?;
        for i in 0..num_iterations {
            func(i, wkb.as_ref())?;
        }

        Ok(())
    }
}

impl ScalarGeo for ScalarValue {
    fn scalar_as_wkb(&self) -> Result<Option<Wkb<'_>>> {
        match self {
            ScalarValue::Binary(maybe_item) | ScalarValue::BinaryView(maybe_item) => {
                parse_wkb_item(maybe_item.as_deref())
            }
            _ => internal_err!(
                "Can't iterate over {:?} ScalarValue as Wkb or WkbView",
                self
            ),
        }
    }
}

impl IterGeo for ColumnarValue {
    fn iter_as_wkb<F: FnMut(usize, Option<&Wkb>) -> Result<()>>(
        &self,
        sedona_type: &SedonaType,
        num_rows: usize,
        func: F,
    ) -> Result<()> {
        match self {
            ColumnarValue::Array(array) => array.iter_as_wkb(sedona_type, num_rows, func),
            ColumnarValue::Scalar(scalar_value) => {
                scalar_value.iter_as_wkb(sedona_type, num_rows, func)
            }
        }
    }
}

/// Helper to dispatch binary iteration over two arrays. The Scalar/Array,
/// Array/Scalar, and Scalar/Scalar case are handled using the unary iteration
/// infrastructure.
fn iter_wkb_wkb_array<F: FnMut(usize, Option<&Wkb>, Option<&Wkb>) -> Result<()>>(
    types: (&SedonaType, &SedonaType),
    arrays: (&ArrayRef, &ArrayRef),
    func: F,
) -> Result<()> {
    let (array0, array1) = arrays;
    match types {
        (SedonaType::Wkb(_, _), SedonaType::Wkb(_, _)) => {
            iter_wkb_wkb_binary(as_binary_array(array0)?, as_binary_array(array1)?, func)
        }
        (SedonaType::Wkb(_, _), SedonaType::WkbView(_, _)) => iter_wkb_wkb_binary(
            as_binary_array(array0)?,
            as_binary_view_array(array1)?,
            func,
        ),
        (SedonaType::WkbView(_, _), SedonaType::Wkb(_, _)) => iter_wkb_wkb_binary(
            as_binary_view_array(array0)?,
            as_binary_array(array1)?,
            func,
        ),
        (SedonaType::WkbView(_, _), SedonaType::WkbView(_, _)) => iter_wkb_wkb_binary(
            as_binary_view_array(array0)?,
            as_binary_view_array(array1)?,
            func,
        ),
        _ => {
            // We could do casting of one or both sides to support other cases as they
            // arise to manage the complexity/performance balance
            internal_err!(
                "Can't iterate over {:?} and {:?} arrays as a pair of Wkb scalars",
                types.0,
                types.1
            )
        }
    }
}

/// Generic function to iterate over a pair of optional bytes providers
/// (e.g., various concrete array types)
fn iter_wkb_wkb_binary<
    'a,
    T0: IntoIterator<Item = Option<&'a [u8]>>,
    T1: IntoIterator<Item = Option<&'a [u8]>>,
    F: FnMut(usize, Option<&Wkb>, Option<&Wkb>) -> Result<()>,
>(
    iterable0: T0,
    iterable1: T1,
    mut func: F,
) -> Result<()> {
    for (i, (item0, item1)) in zip(iterable0, iterable1).enumerate() {
        let wkb0 = parse_wkb_item(item0)?;
        let wkb1 = parse_wkb_item(item1)?;
        func(i, wkb0.as_ref(), wkb1.as_ref())?;
    }

    Ok(())
}

/// Generic function to iterate over a single provider of optional wkb byte slices
/// (e.g., concrete array types)
fn iter_wkb_binary<
    'a,
    T: IntoIterator<Item = Option<&'a [u8]>>,
    F: FnMut(usize, Option<&Wkb>) -> Result<()>,
>(
    iterable: T,
    mut func: F,
) -> Result<()> {
    for (i, item) in iterable.into_iter().enumerate() {
        let wkb = parse_wkb_item(item)?;
        func(i, wkb.as_ref())?;
    }

    Ok(())
}

/// Parse a single wkb item
fn parse_wkb_item(maybe_item: Option<&[u8]>) -> Result<Option<Wkb<'_>>> {
    match maybe_item {
        Some(item) => {
            let geometry = wkb::reader::read_wkb(item)
                .map_err(|err| DataFusionError::External(Box::new(err)))?;
            Ok(Some(geometry))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;
    use std::sync::Arc;

    use arrow_array::{builder::BinaryBuilder, create_array};
    use arrow_schema::DataType;
    use datafusion_common::{cast::as_binary_view_array, scalar::ScalarValue};
    use datafusion_expr::ColumnarValue;
    use rstest::rstest;
    use sedona_schema::datatypes::{WKB_GEOMETRY, WKB_VIEW_GEOMETRY};

    use super::*;

    const POINT: [u8; 21] = [
        0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
    ];

    fn write_to_test_output(
        wkt_out: &mut String,
        i: usize,
        maybe_geom: Option<&Wkb>,
    ) -> Result<()> {
        write!(wkt_out, " {i}: ").unwrap();
        match maybe_geom {
            Some(geom) => wkt::to_wkt::write_geometry(wkt_out, &geom).unwrap(),
            None => write!(wkt_out, "None").unwrap(),
        };

        Ok(())
    }

    #[test]
    fn wkb_array() {
        let mut builder = BinaryBuilder::new();
        builder.append_value(POINT);
        builder.append_null();
        builder.append_value(POINT);
        let wkb_array = builder.finish();

        let mut out = Vec::new();
        iter_wkb_binary(&wkb_array, |_i, x| {
            out.push(x.is_some());
            Ok(())
        })
        .unwrap();
        assert_eq!(out, vec![true, false, true]);

        let mut wkt_out = String::new();
        ColumnarValue::Array(Arc::new(wkb_array.clone()))
            .iter_as_wkb(&WKB_GEOMETRY, 3, |i, maybe_geom| {
                write_to_test_output(&mut wkt_out, i, maybe_geom).unwrap();
                Ok(())
            })
            .unwrap();
        assert_eq!(wkt_out, " 0: POINT(1 2) 1: None 2: POINT(1 2)");

        let wkb_view_array_ref = ColumnarValue::Array(Arc::new(wkb_array))
            .cast_to(&DataType::BinaryView, None)
            .unwrap()
            .to_array(3)
            .unwrap();
        let wkb_view_array = as_binary_view_array(&wkb_view_array_ref).unwrap();

        let mut out = Vec::new();
        iter_wkb_binary(wkb_view_array, |_i, x| {
            out.push(x.is_some());
            Ok(())
        })
        .unwrap();
        assert_eq!(out, vec![true, false, true]);

        let mut wkt_out = String::new();
        ColumnarValue::Array(Arc::new(wkb_view_array.clone()))
            .iter_as_wkb(&WKB_VIEW_GEOMETRY, 3, |i, maybe_geom| {
                write_to_test_output(&mut wkt_out, i, maybe_geom).unwrap();
                Ok(())
            })
            .unwrap();
        assert_eq!(wkt_out, " 0: POINT(1 2) 1: None 2: POINT(1 2)");
    }

    #[rstest]
    fn wkb_wkb_types(
        #[values(
            (WKB_GEOMETRY, WKB_GEOMETRY),
            (WKB_GEOMETRY, WKB_VIEW_GEOMETRY),
            (WKB_VIEW_GEOMETRY, WKB_GEOMETRY),
            (WKB_VIEW_GEOMETRY, WKB_VIEW_GEOMETRY))
        ]
        types: (SedonaType, SedonaType),
    ) {
        let (left_type, right_type) = types;

        let mut builder = BinaryBuilder::new();
        builder.append_value(POINT);
        builder.append_null();
        builder.append_value(POINT);
        builder.append_null();
        let binary_array0 = builder.finish();

        let mut builder = BinaryBuilder::new();
        builder.append_value(POINT);
        builder.append_value(POINT);
        builder.append_null();
        builder.append_null();
        let binary_array1 = builder.finish();

        let value0 = ColumnarValue::Array(Arc::new(binary_array0.clone()))
            .cast_to(left_type.storage_type(), None)
            .unwrap();

        let value1 = ColumnarValue::Array(Arc::new(binary_array1.clone()))
            .cast_to(right_type.storage_type(), None)
            .unwrap();

        let arg_types = [left_type, right_type];
        let args = [value0, value1];
        let executor = GenericExecutor::new(&arg_types, &args);

        let mut wkt_out = String::new();
        executor
            .execute_wkb_wkb_void(|i, maybe_geom0, maybe_geom1| {
                write_to_test_output(&mut wkt_out, i, maybe_geom0)?;
                write_to_test_output(&mut wkt_out, i, maybe_geom1)?;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            wkt_out,
            " 0: POINT(1 2) 0: POINT(1 2) 1: None 1: POINT(1 2) 2: POINT(1 2) 2: None 3: None 3: None"
        );
    }

    #[test]
    fn wkb_wkb_scalar_array() {
        let mut builder = BinaryBuilder::new();
        builder.append_value(POINT);
        builder.append_null();
        let wkb_array = builder.finish();
        let wkb_array_value = ColumnarValue::Array(Arc::new(wkb_array));
        let wkb_scalar_value = ColumnarValue::Scalar(ScalarValue::Binary(Some(POINT.to_vec())));
        let wkb_scalar_null_value = ColumnarValue::Scalar(ScalarValue::Binary(None));

        let arg_types = [WKB_GEOMETRY, WKB_GEOMETRY];
        let args = [wkb_array_value.clone(), wkb_scalar_value.clone()];
        let executor = GenericExecutor::new(&arg_types, &args);

        let mut wkt_out = String::new();
        executor
            .execute_wkb_wkb_void(|i, maybe_geom0, maybe_geom1| {
                write_to_test_output(&mut wkt_out, i, maybe_geom0)?;
                write_to_test_output(&mut wkt_out, i, maybe_geom1)?;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            wkt_out,
            " 0: POINT(1 2) 0: POINT(1 2) 1: None 1: POINT(1 2)"
        );

        let args = [wkb_array_value.clone(), wkb_scalar_value.clone()];
        let executor = GenericExecutor::new(&arg_types, &args);

        let mut wkt_out = String::new();
        executor
            .execute_wkb_wkb_void(|i, maybe_geom0, maybe_geom1| {
                write_to_test_output(&mut wkt_out, i, maybe_geom0)?;
                write_to_test_output(&mut wkt_out, i, maybe_geom1)?;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            wkt_out,
            " 0: POINT(1 2) 0: POINT(1 2) 1: None 1: POINT(1 2)"
        );

        let args = [wkb_array_value.clone(), wkb_array_value.clone()];
        let executor = GenericExecutor::new(&arg_types, &args);

        let mut wkt_out = String::new();
        executor
            .execute_wkb_wkb_void(|i, maybe_geom0, maybe_geom1| {
                write_to_test_output(&mut wkt_out, i, maybe_geom0)?;
                write_to_test_output(&mut wkt_out, i, maybe_geom1)?;
                Ok(())
            })
            .unwrap();
        assert_eq!(wkt_out, " 0: POINT(1 2) 0: POINT(1 2) 1: None 1: None");

        let args = [wkb_scalar_null_value.clone(), wkb_scalar_value.clone()];
        let executor = GenericExecutor::new(&arg_types, &args);

        let mut wkt_out = String::new();
        executor
            .execute_wkb_wkb_void(|i, maybe_geom0, maybe_geom1| {
                write_to_test_output(&mut wkt_out, i, maybe_geom0)?;
                write_to_test_output(&mut wkt_out, i, maybe_geom1)?;
                Ok(())
            })
            .unwrap();
        assert_eq!(wkt_out, " 0: None 0: POINT(1 2)");
    }

    #[test]
    fn wkb_array_errors() {
        let not_geometry_array: ArrayRef = create_array!(Int32, [0, 1, 2]);
        let err = ColumnarValue::Array(not_geometry_array.clone())
            .iter_as_wkb(
                &SedonaType::Arrow(DataType::Int32),
                3,
                |_, _| unreachable!(),
            )
            .unwrap_err();
        assert!(err.message().contains("Can't iterate over"));

        let err = ColumnarValue::Array(not_geometry_array.clone())
            .iter_as_wkb(
                &SedonaType::Arrow(DataType::Int32),
                1000000,
                |_, _| unreachable!(),
            )
            .unwrap_err();
        assert!(err
            .message()
            .contains("Expected 1000000 items but got Array with 3 items"));

        let err = iter_wkb_wkb_array(
            (
                &SedonaType::Arrow(DataType::Int32),
                &SedonaType::Arrow(DataType::Int32),
            ),
            (&not_geometry_array, &not_geometry_array),
            |_, _, _| unreachable!(),
        )
        .unwrap_err();
        assert!(err.message().contains(
            "Can't iterate over Arrow(Int32) and Arrow(Int32) arrays as a pair of Wkb scalars"
        ));
    }

    #[test]
    fn wkb_scalar() {
        assert!(ScalarValue::Binary(None).scalar_as_wkb().unwrap().is_none());

        let mut wkt_out = String::new();
        let binary_scalar = ScalarValue::Binary(Some(POINT.to_vec()));

        let wkb_item = binary_scalar.scalar_as_wkb().unwrap().unwrap();
        wkt::to_wkt::write_geometry(&mut wkt_out, &wkb_item).unwrap();
        assert_eq!(wkt_out, "POINT(1 2)");
        drop(wkb_item);

        let mut wkt_out = String::new();
        ColumnarValue::Scalar(binary_scalar)
            .iter_as_wkb(&WKB_GEOMETRY, 1, |i, maybe_geom| {
                write_to_test_output(&mut wkt_out, i, maybe_geom).unwrap();
                Ok(())
            })
            .unwrap();
        assert_eq!(wkt_out, " 0: POINT(1 2)");

        let mut wkt_out = String::new();
        let binary_scalar = ScalarValue::BinaryView(Some(POINT.to_vec()));

        let wkb_item = binary_scalar.scalar_as_wkb().unwrap().unwrap();
        wkt::to_wkt::write_geometry(&mut wkt_out, &wkb_item).unwrap();
        assert_eq!(wkt_out, "POINT(1 2)");
        drop(wkb_item);

        let mut wkt_out = String::new();
        ColumnarValue::Scalar(binary_scalar)
            .iter_as_wkb(&WKB_VIEW_GEOMETRY, 1, |i, maybe_geom| {
                write_to_test_output(&mut wkt_out, i, maybe_geom).unwrap();
                Ok(())
            })
            .unwrap();
        assert_eq!(wkt_out, " 0: POINT(1 2)");

        let err = ScalarValue::Date32(Some(1)).scalar_as_wkb().unwrap_err();
        assert!(err.message().contains("Can't iterate over"));
    }

    #[test]
    fn wkb_item() {
        assert!(parse_wkb_item(None).unwrap().is_none());

        let wkb_item = parse_wkb_item(Some(&POINT)).unwrap().unwrap();
        let mut wkt_out = String::new();
        wkt::to_wkt::write_geometry(&mut wkt_out, &wkb_item).unwrap();
        assert_eq!(wkt_out, "POINT(1 2)");

        let err = parse_wkb_item(Some(&[])).unwrap_err();
        assert_eq!(err.message(), "failed to fill whole buffer")
    }
}
