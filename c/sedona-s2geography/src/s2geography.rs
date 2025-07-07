use crate::geography_glue_bindgen::*;

/// Dependency versions for underlying libraries
pub struct Versions {}

impl Versions {
    /// Return the statically linked nanoarrow version as a string
    pub fn nanoarrow() -> String {
        unsafe {
            let raw_c_str = SedonaGeographyGlueNanoarrowVersion();
            let c_str = std::ffi::CStr::from_ptr(raw_c_str);
            c_str.to_string_lossy().into_owned()
        }
    }

    /// Return the statically linked geoarrow version as a string
    pub fn geoarrow() -> String {
        unsafe {
            let raw_c_str = SedonaGeographyGlueGeoArrowVersion();
            let c_str = std::ffi::CStr::from_ptr(raw_c_str);
            c_str.to_string_lossy().into_owned()
        }
    }

    /// Return the statically linked s2 version as a string
    pub fn s2geometry() -> String {
        unsafe {
            let raw_c_str = SedonaGeographyGlueS2GeometryVersion();
            let c_str = std::ffi::CStr::from_ptr(raw_c_str);
            c_str.to_string_lossy().into_owned()
        }
    }

    /// Return the linked Abseil version as a string
    ///
    /// Depending on build-time settings, this may have been statically
    /// or dynamically linked.
    pub fn abseil() -> String {
        unsafe {
            let raw_c_str = SedonaGeographyGlueAbseilVersion();
            let c_str = std::ffi::CStr::from_ptr(raw_c_str);
            c_str.to_string_lossy().into_owned()
        }
    }

    /// Return the linked OpenSSL version as a string
    ///
    /// Depending on build-time settings, this may have been statically
    /// or dynamically linked.
    pub fn openssl() -> String {
        unsafe {
            let raw_c_str = SedonaGeographyGlueOpenSSLVersion();
            let c_str = std::ffi::CStr::from_ptr(raw_c_str);
            c_str.to_string_lossy().into_owned()
        }
    }

    /// A simple function that performs a non-trivial operation
    ///
    /// This is needed as a smoke check to ensure required libraries are linked.
    pub fn test_linkage() -> f64 {
        unsafe { SedonaGeographyGlueTestLinkage() }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_versions() {
        assert_eq!(Versions::nanoarrow(), "0.7.0-SNAPSHOT");
        assert_eq!(Versions::geoarrow(), "0.2.0-SNAPSHOT");
        assert_eq!(Versions::s2geometry(), "0.11.1");
        assert!(Versions::abseil().starts_with("20"));
        assert!(Versions::openssl().contains("."));
        assert!(Versions::test_linkage() > 0.0);
    }
}
