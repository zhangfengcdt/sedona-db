use arrow_array::{Array, ArrayRef, BinaryArray, BinaryViewArray};
use datafusion_common::{
    cast::{as_binary_array, as_binary_view_array},
    internal_err, DataFusionError, Result,
};
use sedona_schema::datatypes::SedonaType;
use wkb::reader::Wkb;

pub(crate) trait GetGeo {
    fn get_wkb(&self, sedona_type: &SedonaType, row_idx: usize) -> Result<Option<Wkb<'_>>>;
}

impl GetGeo for ArrayRef {
    fn get_wkb(&self, sedona_type: &SedonaType, row_idx: usize) -> Result<Option<Wkb<'_>>> {
        match sedona_type {
            SedonaType::Wkb(_, _) => {
                let byte_array = as_binary_array(self)?;
                if byte_array.is_null(row_idx) {
                    return Ok(None);
                }
                let wkb = Wkb::try_new(byte_array.value(row_idx))
                    .map_err(|e| DataFusionError::Internal(format!("Failed to parse WKB: {e}")))?;
                Ok(Some(wkb))
            }
            SedonaType::WkbView(_, _) => {
                let byte_array = as_binary_view_array(self)?;
                if byte_array.is_null(row_idx) {
                    return Ok(None);
                }
                let wkb = Wkb::try_new(byte_array.value(row_idx))
                    .map_err(|e| DataFusionError::Internal(format!("Failed to parse WKB: {e}")))?;
                Ok(Some(wkb))
            }
            _ => {
                // We could cast here as a fallback, iterate and cast per-element, or
                // implement iter_as_something_else()/supports_iter_xxx() when more geo array types
                // are supported.
                internal_err!("Can't iterate over {:?} as Wkb", sedona_type)
            }
        }
    }
}

/// Trait for indexed WKB access with minimal dispatch overhead
pub(crate) trait WkbArrayAccess: Send + Sync + std::fmt::Debug {
    /// Get the WKB item at the specified index, returns None if index is beyond bounds
    fn get_wkb_at(&self, index: usize) -> Result<Option<Wkb<'_>>>;

    /// Get the total number of items
    fn len(&self) -> usize;
}

/// Indexed access for Binary arrays (WKB)
#[derive(Debug)]
pub(crate) struct BinaryWkbAccess {
    array: BinaryArray,
}

impl BinaryWkbAccess {
    pub(crate) fn new(array: BinaryArray) -> Self {
        Self { array }
    }
}

impl WkbArrayAccess for BinaryWkbAccess {
    fn get_wkb_at(&self, index: usize) -> Result<Option<Wkb<'_>>> {
        if self.array.is_null(index) {
            return Ok(None);
        }

        let bytes = self.array.value(index);
        let geometry =
            wkb::reader::read_wkb(bytes).map_err(|err| DataFusionError::External(Box::new(err)))?;
        Ok(Some(geometry))
    }

    fn len(&self) -> usize {
        self.array.len()
    }
}

/// Indexed access for BinaryView arrays (WKB_VIEW)
#[derive(Debug)]
pub(crate) struct BinaryViewWkbAccess {
    array: BinaryViewArray,
}

impl BinaryViewWkbAccess {
    pub(crate) fn new(array: BinaryViewArray) -> Self {
        Self { array }
    }
}

impl WkbArrayAccess for BinaryViewWkbAccess {
    fn get_wkb_at(&self, index: usize) -> Result<Option<Wkb<'_>>> {
        if self.array.is_null(index) {
            return Ok(None);
        }

        let bytes = self.array.value(index);
        let geometry =
            wkb::reader::read_wkb(bytes).map_err(|err| DataFusionError::External(Box::new(err)))?;
        Ok(Some(geometry))
    }

    fn len(&self) -> usize {
        self.array.len()
    }
}

/// Create a WKB array accessor based on the sedona type and array
pub(crate) fn create_wkb_array_access(
    array: &arrow::array::ArrayRef,
    sedona_type: &SedonaType,
) -> Result<Box<dyn WkbArrayAccess>> {
    match sedona_type {
        SedonaType::Wkb(_, _) => {
            let binary_array = as_binary_array(array)?;
            Ok(Box::new(BinaryWkbAccess::new(binary_array.clone())))
        }
        SedonaType::WkbView(_, _) => {
            let binary_view_array = as_binary_view_array(array)?;
            Ok(Box::new(BinaryViewWkbAccess::new(
                binary_view_array.clone(),
            )))
        }
        _ => {
            internal_err!(
                "Unsupported geometry type for WKB array access: {:?}",
                sedona_type
            )
        }
    }
}
