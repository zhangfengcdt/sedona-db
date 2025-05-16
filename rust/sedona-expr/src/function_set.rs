use crate::scalar_udf::{ScalarKernelRef, SedonaScalarUDF};
use datafusion_common::{error::Result, internal_err};
use datafusion_expr::ScalarUDFImpl;
use std::collections::HashMap;

/// Helper for managing groups of functions
///
/// Sedona coordinates the assembly of a large number of spatial functions with potentially
/// different sets of dependencies (e.g., geography vs. geometry), multiple implementations,
/// and/or implementations that live in different crates. This structure helps coordinate
/// these implementations.
pub struct FunctionSet {
    scalar_udfs: HashMap<String, SedonaScalarUDF>,
}

impl FunctionSet {
    /// Create a new, empty FunctionSet
    pub fn new() -> Self {
        Self {
            scalar_udfs: HashMap::new(),
        }
    }

    /// Iterate over references to all [SedonaScalarUDF]s
    pub fn scalar_udfs(&self) -> impl Iterator<Item = &SedonaScalarUDF> + '_ {
        self.scalar_udfs.values()
    }

    /// Return a reference to the function corresponding to the name
    pub fn scalar_udf(&self, name: &str) -> Option<&SedonaScalarUDF> {
        self.scalar_udfs.get(name)
    }

    /// Return a mutable reference to the function corresponding to the name
    pub fn scalar_udf_mut(&mut self, name: &str) -> Option<&mut SedonaScalarUDF> {
        self.scalar_udfs.get_mut(name)
    }

    /// Insert a new ScalarUDF and return the UDF that had previously been added, if any
    pub fn insert_scalar_udf(&mut self, udf: SedonaScalarUDF) -> Option<SedonaScalarUDF> {
        self.scalar_udfs.insert(udf.name().to_string(), udf)
    }

    /// Consume another function set and merge its contents into this one
    pub fn merge(&mut self, other: FunctionSet) {
        for (k, v) in other.scalar_udfs.into_iter() {
            self.scalar_udfs.insert(k, v);
        }
    }

    /// Add a kernel to a function in this set
    ///
    /// This errors if a function of that name does not exist in this set. A reference
    /// to the matching function is returned.
    pub fn add_scalar_udf_kernel(
        &mut self,
        name: &str,
        kernel: ScalarKernelRef,
    ) -> Result<&SedonaScalarUDF> {
        if let Some(function) = self.scalar_udf_mut(name) {
            function.add_kernel(kernel);
            Ok(self.scalar_udf(name).unwrap())
        } else {
            internal_err!("Can't register kernel for function '{}'", name)
        }
    }
}

impl Default for FunctionSet {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use arrow_schema::DataType;
    use datafusion_common::scalar::ScalarValue;

    use datafusion_expr::{ColumnarValue, Volatility};
    use sedona_schema::datatypes::SedonaType;

    use crate::scalar_udf::{ArgMatcher, SimpleSedonaScalarKernel};

    use super::*;

    #[test]
    fn function_set() {
        let mut functions = FunctionSet::new();
        assert_eq!(functions.scalar_udfs().collect::<Vec<_>>().len(), 0);
        assert!(functions.scalar_udf("simple_udf").is_none());
        assert!(functions.scalar_udf_mut("simple_udf").is_none());

        let kernel = SimpleSedonaScalarKernel::new_ref(
            ArgMatcher::new(
                vec![ArgMatcher::is_arrow(DataType::Boolean)],
                SedonaType::Arrow(DataType::Boolean),
            ),
            Arc::new(|_, _| Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)))),
        );

        let udf = SedonaScalarUDF::new(
            "simple_udf",
            vec![kernel.clone()],
            Volatility::Immutable,
            None,
        );

        functions.insert_scalar_udf(udf);
        assert_eq!(functions.scalar_udfs().collect::<Vec<_>>().len(), 1);
        assert!(functions.scalar_udf("simple_udf").is_some());
        assert!(functions.scalar_udf_mut("simple_udf").is_some());
        assert_eq!(
            functions
                .add_scalar_udf_kernel("simple_udf", kernel.clone())
                .unwrap()
                .name(),
            "simple_udf"
        );
        let err = functions
            .add_scalar_udf_kernel("function that does not exist", kernel.clone())
            .unwrap_err();
        assert_eq!(
            err.message().lines().next().unwrap(),
            "Can't register kernel for function 'function that does not exist'."
        );

        let kernel2 = SimpleSedonaScalarKernel::new_ref(
            ArgMatcher::new(
                vec![ArgMatcher::is_arrow(DataType::Utf8)],
                SedonaType::Arrow(DataType::Utf8),
            ),
            Arc::new(|_, _| Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))),
        );

        let udf2 = SedonaScalarUDF::new("simple_udf2", vec![kernel2], Volatility::Immutable, None);
        let mut functions2 = FunctionSet::new();
        functions2.insert_scalar_udf(udf2);
        functions.merge(functions2);
        assert_eq!(
            functions
                .scalar_udfs()
                .map(|s| s.name())
                .collect::<HashSet<_>>(),
            vec!["simple_udf", "simple_udf2"]
                .into_iter()
                .collect::<HashSet<_>>()
        );
    }
}
