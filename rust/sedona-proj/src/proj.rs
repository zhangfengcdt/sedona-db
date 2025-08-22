/// This module is our own reimplemented PROJ binding. We are not using georust/proj directly
/// because it is broken in the following aspects:
///
/// - It creates a dedicated proj context each time we call proj functions, this is an anti-pattern.
/// - It leaks memory when error happens in some functions.
/// - it will call proj_cleanup when some proj objects are still being used in multi-threaded environment
///
/// Our implementation creates a proj context for each thread and reuse it, this avoids
/// the repeated sqlite database connection initialization when creating proj objects
/// from CRS.
///
/// A significant amount of code were taken from
/// https://github.com/georust/proj/blob/95716f6cf90135150e313222bf296eb32e9e4493/src/proj.rs
/// The original work has MIT/Apache-2.0 license.
use core::str;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;

use proj::{Area, ProjCreateError, ProjError};
use proj_sys::{
    proj_area_create, proj_area_destroy, proj_area_set_bbox, proj_context_create,
    proj_context_destroy, proj_context_errno, proj_context_errno_string, proj_create,
    proj_create_crs_to_crs_from_pj, proj_destroy, proj_errno, proj_errno_reset,
    proj_normalize_for_visualization, proj_trans, PJconsts, PJ_AREA, PJ_CONTEXT, PJ_COORD,
    PJ_DIRECTION_PJ_FWD, PJ_XYZT,
};

pub(crate) struct ProjContext {
    c_ctx: *mut PJ_CONTEXT,
}

impl ProjContext {
    fn new() -> Self {
        let ctx = unsafe { proj_context_create() };
        Self { c_ctx: ctx }
    }
}

impl Drop for ProjContext {
    fn drop(&mut self) {
        unsafe {
            proj_context_destroy(self.c_ctx);
        }
    }
}

thread_local! {
  /// A PROJ Context instance used for coordinate transformations.
  ///
  /// This provides a thread-local context that manages the lifecycle of PROJ operations.
  /// Each thread gets its own PROJ context to ensure thread safety.
  static PROJ_CONTEXT: ProjContext = ProjContext::new();
}

/// Execute a function with access to the thread-local PROJ context.
///
/// This ensures that all PROJ operations use the same context within a thread,
/// which is required for proper error handling and resource management.
fn with_proj_context<F, R>(f: F) -> R
where
    F: FnOnce(*mut PJ_CONTEXT) -> R,
{
    PROJ_CONTEXT.with(|ctx| f(ctx.c_ctx))
}

/// An error number returned from a PROJ call.
pub(crate) struct Errno(pub ::std::os::raw::c_int);

impl Errno {
    /// Return the error message associated with the error number.
    pub fn message(&self, context: *mut PJ_CONTEXT) -> String {
        let ptr = unsafe { proj_context_errno_string(context, self.0) };
        if ptr.is_null() {
            "<unknown error>".to_string()
        } else {
            unsafe { _string(ptr).expect("PROJ provided an invalid error string") }
        }
    }
}

/// Safely convert a C string pointer from PROJ to a Rust String.
///
/// # Safety
/// This function is unsafe because it dereferences a raw pointer.
/// The caller must ensure that:
/// - `raw_ptr` is not null
/// - `raw_ptr` points to a valid, null-terminated C string
/// - The string data remains valid for the duration of this call
unsafe fn _string(raw_ptr: *const c_char) -> Result<String, str::Utf8Error> {
    assert!(!raw_ptr.is_null());
    let c_str = CStr::from_ptr(raw_ptr);
    Ok(str::from_utf8(c_str.to_bytes())?.to_string())
}

/// Construct a `Result` from the result of a `proj_create*` call.
///
/// PROJ functions that create objects return null pointers on failure.
/// This function checks for null and retrieves the error code if needed.
fn result_from_create<T>(context: *mut PJ_CONTEXT, ptr: *mut T) -> Result<*mut T, Errno> {
    if ptr.is_null() {
        Err(Errno(unsafe { proj_context_errno(context) }))
    } else {
        Ok(ptr)
    }
}

/// A wrapper around PROJ's PJ_AREA structure.
///
/// This represents a bounding box area of use for coordinate transformations.
/// Areas of use help PROJ select the most appropriate transformation parameters
/// for a given geographic region.
#[derive(Debug)]
pub(crate) struct ProjArea {
    c_area: *mut PJ_AREA,
}

impl ProjArea {
    /// Create a new ProjArea from an optional Area.
    ///
    /// If an area is provided, sets the bounding box. Otherwise creates
    /// an empty area which means no geographic restriction.
    fn new(area: Option<Area>) -> Self {
        let parea = unsafe { proj_area_create() };
        if let Some(narea) = area {
            unsafe {
                proj_area_set_bbox(parea, narea.west, narea.south, narea.east, narea.north);
            }
        }
        Self { c_area: parea }
    }
}

impl Drop for ProjArea {
    fn drop(&mut self) {
        unsafe {
            proj_area_destroy(self.c_area);
        }
    }
}

/// A coordinate transformation object using PROJ.
///
/// This wraps PROJ's PJ structure and provides coordinate transformation capabilities.
/// It can represent various types of coordinate operations:
/// - Projections (geodetic to projected coordinates)
/// - Conversions (between different projected coordinate systems)
/// - Transformations (between different datums/coordinate reference systems)
#[derive(Debug)]
pub(crate) struct Proj {
    c_proj: *mut PJconsts,
}

impl Proj {
    /// Try to create a new transformation object from a PROJ definition string.
    ///
    /// **Note:** for projection operations, `definition` specifies
    /// the **output** projection; input coordinates are assumed to be geodetic
    /// in radians, unless an inverse projection is intended.
    ///
    /// For conversion operations, `definition` defines input, output, and
    /// any intermediate steps that are required.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let transformer = Proj::try_new(
    ///     "+proj=merc +lat_ts=56.5 +ellps=GRS80"
    /// ).unwrap();
    /// ```
    ///
    /// # Safety
    /// This method contains unsafe code when interfacing with PROJ C library.
    pub(crate) fn try_new(definition: &str) -> Result<Self, ProjCreateError> {
        let c_definition = CString::new(definition).map_err(ProjCreateError::ArgumentNulError)?;
        with_proj_context(|ctx| {
            let ptr = result_from_create(ctx, unsafe { proj_create(ctx, c_definition.as_ptr()) })
                .map_err(|e| ProjCreateError::ProjError(e.message(ctx)))?;
            Ok(Proj { c_proj: ptr })
        })
    }

    /// Create a transformation between two coordinate reference systems.
    ///
    /// This creates a transformation pipeline that converts coordinates from
    /// the source CRS to the target CRS. The transformation is optimized
    /// for the specified area of use.
    ///
    /// ## A Note on Coordinate Order
    /// The required input **and** output coordinate order is **normalised** to
    /// `Longitude, Latitude` / `Easting, Northing`.
    ///
    /// This overrides the expected order of the specified input and/or output CRS if necessary.
    /// For example: per its definition, EPSG:4326 has an axis order of Latitude, Longitude.
    /// Without normalisation, users would have to remember to reverse the coordinates.
    ///
    /// # Safety
    /// This method contains unsafe code when interfacing with PROJ C library.
    pub(crate) fn crs_to_crs(
        source_crs: &Proj,
        target_crs: &Proj,
        area: Option<Area>,
    ) -> Result<Self, ProjCreateError> {
        let proj_area = ProjArea::new(area);

        let ptr = with_proj_context(|ctx| {
            let ptr = result_from_create(ctx, unsafe {
                proj_create_crs_to_crs_from_pj(
                    ctx,
                    source_crs.c_proj,
                    target_crs.c_proj,
                    proj_area.c_area,
                    ptr::null(),
                )
            })
            .map_err(|e| ProjCreateError::ProjError(e.message(ctx)))?;

            // Normalise input and output order to Lon, Lat / Easting Northing by inserting
            // An axis swap operation if necessary
            let normalised = unsafe {
                let normalised = proj_normalize_for_visualization(ctx, ptr);
                // deallocate stale PJ pointer
                proj_destroy(ptr);
                normalised
            };

            Ok(normalised)
        })?;

        Ok(Proj { c_proj: ptr })
    }

    /// Convert projected coordinates between coordinate reference systems.
    ///
    /// Input and output CRS should have been defined when creating this Proj instance.
    /// The coordinate order depends on how the Proj was created:
    /// - If created with `crs_to_crs`, input and output order are **normalised**
    ///   to Longitude, Latitude / Easting, Northing.
    /// - If created with `try_new`, coordinate order follows the definition string.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let point = (4760096.421921, 3744293.729449);
    /// let result = transform.convert(point)?;
    /// ```
    ///
    /// # Safety
    /// This method contains unsafe code when interfacing with PROJ C library.
    pub(crate) fn convert(&mut self, point: (f64, f64)) -> Result<(f64, f64), ProjError> {
        let (c_x, c_y) = point;

        // This doesn't seem strictly correct, but if we set PJ_XY or PJ_LP here, the
        // other two values remain uninitialized and we can't be sure that libproj
        // doesn't try to read them. proj_trans_generic does the same thing.
        let xyzt = PJ_XYZT {
            x: c_x,
            y: c_y,
            z: 0.0,
            t: f64::INFINITY,
        };

        let (transformed, err) = unsafe {
            proj_errno_reset(self.c_proj);
            let trans = proj_trans(self.c_proj, PJ_DIRECTION_PJ_FWD, PJ_COORD { xyzt });
            let err = proj_errno(self.c_proj);
            ((trans.xy.x, trans.xy.y), err)
        };

        if err != 0 {
            let err_str_raw =
                with_proj_context(|ctx| unsafe { proj_context_errno_string(ctx, err) });
            return Err(ProjError::Conversion(unsafe { _string(err_str_raw)? }));
        }

        Ok(transformed)
    }
}

impl Drop for Proj {
    fn drop(&mut self) {
        unsafe {
            proj_destroy(self.c_proj);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use approx::assert_relative_eq;

    /// Test conversion from NAD83 US Survey Feet (EPSG 2230) to NAD83 Metres (EPSG 26946)
    #[test]
    fn test_crs_to_crs_conversion() {
        let from = Proj::try_new("EPSG:2230").unwrap();
        let to = Proj::try_new("EPSG:26946").unwrap();
        let mut transformer = Proj::crs_to_crs(&from, &to, None).unwrap();

        let result = transformer
            .convert((4760096.421921, 3744293.729449))
            .unwrap();
        assert_relative_eq!(result.0, 1450880.2910605022, epsilon = 1e-8);
        assert_relative_eq!(result.1, 1141263.0111604782, epsilon = 1e-8);
    }

    /// Test error handling with invalid CRS definitions
    #[test]
    fn test_invalid_crs_error() {
        match Proj::try_new("invalid_crs") {
            Err(ProjCreateError::ProjError(_)) => (),
            _ => unreachable!(),
        }
    }

    /// Test that PROJ pipeline definition works
    #[test]
    fn test_pipeline_conversion() {
        // Generated by PROJ by specifying "from" and "to" EPSG codes
        let projstring = "
            proj=pipeline step proj=unitconvert xy_in=us-ft
            xy_out=m step inv proj=lcc lat_0=32.1666666666667
            lon_0=-116.25 lat_1=33.8833333333333 lat_2=32.7833333333333
            x_0=2000000.0001016 y_0=500000.0001016 ellps=GRS80 step proj=lcc lat_0=32.1666666666667
            lon_0=-116.25 lat_1=33.8833333333333 lat_2=32.7833333333333 x_0=2000000 y_0=500000
            ellps=GRS80
            ";
        let mut nad83_m = Proj::try_new(projstring).unwrap();

        // Presidio, San Francisco
        let result = nad83_m.convert((4760096.421921, 3744293.729449)).unwrap();
        assert_relative_eq!(result.0, 1450880.2910605022, epsilon = 1e-8);
        assert_relative_eq!(result.1, 1141263.0111604782, epsilon = 1e-8);
    }

    /// Test conversion error handling
    #[test]
    fn test_conversion_error() {
        // GEOS projection expecting lon/lat input but getting large coordinate values
        let mut transformer = Proj::try_new(
            "+proj=geos +lon_0=0.00 +lat_0=0.00 +a=6378169.00 +b=6356583.80 +h=35785831.0",
        )
        .unwrap();

        let err = transformer
            .convert((4760096.421921, 3744293.729449))
            .unwrap_err();

        match err {
            ProjError::Conversion(_) => (),
            _ => unreachable!(),
        }
    }

    /// Test error recovery - subsequent valid conversions should work after an error
    #[test]
    fn test_error_recovery() {
        let mut transformer = Proj::try_new(
            "+proj=geos +lon_0=0.00 +lat_0=0.00 +a=6378169.00 +b=6356583.80 +h=35785831.0",
        )
        .unwrap();

        // First conversion should fail
        assert!(transformer
            .convert((4760096.421921, 3744293.729449))
            .is_err());

        // But a subsequent valid conversion should still work
        assert!(transformer.convert((0.0, 0.0)).is_ok());
    }

    /// Test coordinate transformation with area of use
    #[test]
    fn test_area_of_use() {
        let from = Proj::try_new("EPSG:2230").unwrap();
        let to = Proj::try_new("EPSG:4326").unwrap();

        // Define an area of use (California region)
        let area = Area::new(-124.0, 32.0, -114.0, 42.0); // west, south, east, north
        let mut transformer = Proj::crs_to_crs(&from, &to, Some(area)).unwrap();

        let result = transformer
            .convert((4760096.421921, 3744293.729449))
            .unwrap();

        // Should get reasonable latitude/longitude values for California
        assert!(result.0 >= -125.0 && result.0 <= -113.0); // longitude
        assert!(result.1 >= 31.0 && result.1 <= 43.0); // latitude
    }

    /// Test thread safety by ensuring each thread gets its own context
    #[test]
    fn test_multi_thread() {
        use std::thread;

        let handles: Vec<_> = (0..4)
            .map(|_| {
                thread::spawn(|| {
                    let from = Proj::try_new("EPSG:2230").unwrap();
                    let to = Proj::try_new("EPSG:26946").unwrap();
                    let mut transformer = Proj::crs_to_crs(&from, &to, None).unwrap();

                    // Each thread should be able to perform transformations independently
                    transformer
                        .convert((4760096.421921, 3744293.729449))
                        .unwrap()
                })
            })
            .collect();

        for handle in handles {
            let result = handle.join().unwrap();
            assert_relative_eq!(result.0, 1450880.2910605022, epsilon = 1e-8);
            assert_relative_eq!(result.1, 1141263.0111604782, epsilon = 1e-8);
        }
    }
}
