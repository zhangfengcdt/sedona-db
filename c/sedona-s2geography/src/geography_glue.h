
#include <stdint.h>

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

struct SedonaGeographyArrowUdf {
  int (*init)(struct SedonaGeographyArrowUdf* self, struct ArrowSchema* arg_schema,
              const char* options, struct ArrowSchema* out);
  int (*execute)(struct SedonaGeographyArrowUdf* self, struct ArrowArray** args,
                 int64_t n_args, struct ArrowArray* out);
  const char* (*get_last_error)(struct SedonaGeographyArrowUdf* self);
  void (*release)(struct SedonaGeographyArrowUdf* self);

  void* private_data;
};

#define DECLARE_UDF_IMPL(name) \
  void SedonaGeographyInitUdf##name(struct SedonaGeographyArrowUdf* out)

DECLARE_UDF_IMPL(Area);
DECLARE_UDF_IMPL(Centroid);
DECLARE_UDF_IMPL(ClosestPoint);
DECLARE_UDF_IMPL(Contains);
DECLARE_UDF_IMPL(ConvexHull);
DECLARE_UDF_IMPL(Difference);
DECLARE_UDF_IMPL(Distance);
DECLARE_UDF_IMPL(Equals);
DECLARE_UDF_IMPL(Intersection);
DECLARE_UDF_IMPL(Intersects);
DECLARE_UDF_IMPL(Length);
DECLARE_UDF_IMPL(LineInterpolatePoint);
DECLARE_UDF_IMPL(LineLocatePoint);
DECLARE_UDF_IMPL(MaxDistance);
DECLARE_UDF_IMPL(Perimeter);
DECLARE_UDF_IMPL(ShortestLine);
DECLARE_UDF_IMPL(SymDifference);
DECLARE_UDF_IMPL(Union);

#undef DECLARE_UDF_IMPL

#ifdef __cplusplus
}
#endif
