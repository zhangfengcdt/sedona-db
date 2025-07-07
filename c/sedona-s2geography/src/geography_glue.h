
#ifdef __cplusplus
extern "C" {
#endif

/// \file geography_glue.h
///
/// This file exposes C functions and/or data structures used to call
/// s2geography from Rust. Ideally most logic is implemented in
/// s2geography; however, this header is the one that is parsed by
/// bindgen and wrapped in the Rust bindings.
///
/// These functions are internal. See s2geography.rs for the
/// user-facing documentation of these functions.

const char* SedonaGeographyGlueNanoarrowVersion(void);

const char* SedonaGeographyGlueGeoArrowVersion(void);

const char* SedonaGeographyGlueOpenSSLVersion(void);

const char* SedonaGeographyGlueS2GeometryVersion(void);

const char* SedonaGeographyGlueAbseilVersion(void);

double SedonaGeographyGlueTestLinkage(void);

#ifdef __cplusplus
}
#endif
