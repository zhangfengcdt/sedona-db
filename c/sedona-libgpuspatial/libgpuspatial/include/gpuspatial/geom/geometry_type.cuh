#ifndef GPUSPATIAL_GEOM_GEOMETRY_TYPE_CUH
#define GPUSPATIAL_GEOM_GEOMETRY_TYPE_CUH
#include <string>
#include "geoarrow/geoarrow.hpp"

namespace gpuspatial {
enum class GeometryType {
  kPoint,
  kMultiPoint,
  kLineString,
  kMultiLineString,
  kPolygon,
  kMultiPolygon,
  kGeometryCollection,
  kBox,
  kNumGeometryTypes
};

inline std::string GeometryTypeToString(GeometryType type) {
  switch (type) {
    case GeometryType::kPoint:
      return "Point";
    case GeometryType::kMultiPoint:
      return "MultiPoint";
    case GeometryType::kLineString:
      return "LineString";
    case GeometryType::kMultiLineString:
      return "MultiLineString";
    case GeometryType::kPolygon:
      return "Polygon";
    case GeometryType::kMultiPolygon:
      return "MultiPolygon";
    case GeometryType::kGeometryCollection:
      return "GeometryCollection";
    case GeometryType::kBox:
      return "Box";
    default:
      return "Unknown";
  }
}

inline GeometryType FromGeoArrowGeometryType(GeoArrowGeometryType geo_arrow_type) {
  GeometryType type = GeometryType::kNumGeometryTypes;
  switch (geo_arrow_type) {
    case GEOARROW_GEOMETRY_TYPE_POINT: {
      type = GeometryType::kPoint;
      break;
    }
    case GEOARROW_GEOMETRY_TYPE_LINESTRING: {
      type = GeometryType::kLineString;
      break;
    }
    case GEOARROW_GEOMETRY_TYPE_POLYGON: {
      type = GeometryType::kPolygon;
      break;
    }
    case GEOARROW_GEOMETRY_TYPE_MULTIPOINT: {
      type = GeometryType::kMultiPoint;
      break;
    }
    case GEOARROW_GEOMETRY_TYPE_MULTILINESTRING: {
      type = GeometryType::kMultiLineString;
      break;
    }
    case GEOARROW_GEOMETRY_TYPE_MULTIPOLYGON: {
      type = GeometryType::kMultiPolygon;
      break;
    }
    case GEOARROW_GEOMETRY_TYPE_GEOMETRYCOLLECTION: {
      type = GeometryType::kGeometryCollection;
      break;
    }
    default: {
      throw std::runtime_error("Unsupported type " +
                               std::string(GeoArrowGeometryTypeString(geo_arrow_type)));
    }
  }
  return type;
}

namespace detail {
inline bool IsPointType(GeometryType type) {
  return type == GeometryType::kPoint || type == GeometryType::kMultiPoint;
}

inline bool IsLineType(GeometryType type) {
  return type == GeometryType::kLineString || type == GeometryType::kMultiLineString;
}

inline bool IsPolygonType(GeometryType type) {
  return type == GeometryType::kPolygon || type == GeometryType::kMultiPolygon;
}
}  // namespace detail

inline GeometryType GetCompatibleGeometryType(GeometryType type1, GeometryType type2) {
  if (type1 != type2) {
    if (detail::IsPointType(type1) && detail::IsPointType(type2)) {
      return GeometryType::kMultiPoint;
    } else if (detail::IsLineType(type1) && detail::IsLineType(type2)) {
      return GeometryType::kMultiLineString;
    } else if (detail::IsPolygonType(type1) && detail::IsPolygonType(type2)) {
      return GeometryType::kMultiPolygon;
    } else {
      return GeometryType::kGeometryCollection;
    }
  }
  return type1;
}
}  // namespace gpuspatial
#endif  // GPUSPATIAL_GEOM_GEOMETRY_TYPE_CUH
