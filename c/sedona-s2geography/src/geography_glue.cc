
#include "geography_glue.h"

#include "absl/base/config.h"

#include <geoarrow/geoarrow.h>
#include <nanoarrow/nanoarrow.h>
#include <openssl/opensslv.h>
#include <s2geography.h>

#include <s2/s2earth.h>

using namespace s2geography;

const char* SedonaGeographyGlueNanoarrowVersion(void) { return ArrowNanoarrowVersion(); }

const char* SedonaGeographyGlueGeoArrowVersion(void) { return GeoArrowVersion(); }

const char* SedonaGeographyGlueOpenSSLVersion(void) {
  static std::string version = std::string() + std::to_string(OPENSSL_VERSION_MAJOR) +
                               "." + std::to_string(OPENSSL_VERSION_MINOR) + "." +
                               std::to_string(OPENSSL_VERSION_PATCH);
  return version.c_str();
}

const char* SedonaGeographyGlueS2GeometryVersion(void) {
  static std::string version = std::string() + std::to_string(S2_VERSION_MAJOR) + "." +
                               std::to_string(S2_VERSION_MINOR) + "." +
                               std::to_string(S2_VERSION_PATCH);
  return version.c_str();
}

const char* SedonaGeographyGlueAbseilVersion(void) {
#if defined(ABSL_LTS_RELEASE_VERSION)
  static std::string version = std::string() + std::to_string(ABSL_LTS_RELEASE_VERSION) +
                               "." + std::to_string(ABSL_LTS_RELEASE_PATCH_LEVEL);
  return version.c_str();
#else
  return "<live at head>";
#endif
}

double SedonaGeographyGlueTestLinkage(void) {
  PointGeography geog1(S2LatLng::FromDegrees(45, -64).ToPoint());
  PointGeography geog2(S2LatLng::FromDegrees(4, -97).ToPoint());

  ShapeIndexGeography index1(geog1);
  ShapeIndexGeography index2(geog2);

  return S2Earth::RadiusMeters() * s2_distance(index1, index2);
}
