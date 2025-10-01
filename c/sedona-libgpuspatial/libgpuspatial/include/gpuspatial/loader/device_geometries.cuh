#ifndef GPU_SPATIAL_LOADER_DEVICE_GEOMETRIES_CUH
#define GPU_SPATIAL_LOADER_DEVICE_GEOMETRIES_CUH
#include "gpuspatial/geom/box.cuh"
#include "gpuspatial/geom/geometry_type.cuh"
#include "gpuspatial/geom/multi_line_string.cuh"
#include "gpuspatial/geom/multi_point.cuh"
#include "gpuspatial/geom/multi_polygon.cuh"
#include "gpuspatial/geom/polygon.cuh"
#include "gpuspatial/utils/array_view.h"
#include "rmm/device_uvector.hpp"

namespace gpuspatial {
template <typename POINT_T>
class PointSegment;

template <typename POINT_T, typename INDEX_T>
class MultiPointSegment;

template <typename POINT_T, typename INDEX_T>
class LineStringSegment;

template <typename POINT_T, typename INDEX_T>
class MultiLineStringSegment;

template <typename POINT_T, typename INDEX_T>
class PolygonSegment;

template <typename POINT_T, typename INDEX_T>
class MultiPolygonSegment;

template <typename POINT_T, typename INDEX_T>
class BoxSegment;

template <typename POINT_T, typename INDEX_T>
struct DeviceGeometries {
  using point_t = POINT_T;
  using box_t = Box<point_t>;

  DeviceGeometries() : type_(GeometryType::kNumGeometryTypes) {}

  template <typename GeometryArrayView_T>
  GeometryArrayView_T GetGeometryArrayView() const {
    // The const version has identical logic
    if constexpr (std::is_same_v<GeometryArrayView_T, PointArrayView<POINT_T, INDEX_T>>) {
      if (points_ != nullptr) {
        return {ArrayView<POINT_T>(*points_)};
      }
    } else if constexpr (std::is_same_v<GeometryArrayView_T,
                                        MultiPointArrayView<POINT_T, INDEX_T>>) {
      if (offsets_.multi_point_offsets.prefix_sum != nullptr) {
        return {ArrayView<INDEX_T>(*offsets_.multi_point_offsets.prefix_sum),
                ArrayView<POINT_T>(*points_), ArrayView<box_t>(*mbrs_)};
      }
    } else if constexpr (std::is_same_v<GeometryArrayView_T,
                                        LineStringArrayView<POINT_T, INDEX_T>>) {
      if (offsets_.line_string_offsets.prefix_sum != nullptr) {
        return {ArrayView<INDEX_T>(*offsets_.line_string_offsets.prefix_sum),
                ArrayView<POINT_T>(*points_), ArrayView<box_t>(*mbrs_)};
      }
    } else if constexpr (std::is_same_v<GeometryArrayView_T,
                                        MultiLineStringArrayView<POINT_T, INDEX_T>>) {
      if (offsets_.multi_line_string_offsets.prefix_sum_geoms != nullptr) {
        return {ArrayView<INDEX_T>(*offsets_.multi_line_string_offsets.prefix_sum_geoms),
                ArrayView<INDEX_T>(*offsets_.multi_line_string_offsets.prefix_sum_parts),
                ArrayView<POINT_T>(*points_), ArrayView<box_t>(*mbrs_)};
      }
    } else if constexpr (std::is_same_v<GeometryArrayView_T,
                                        PolygonArrayView<point_t, INDEX_T>>) {
      if (offsets_.polygon_offsets.prefix_sum_polygons != nullptr) {
        return {ArrayView<INDEX_T>(*offsets_.polygon_offsets.prefix_sum_polygons),
                ArrayView<INDEX_T>(*offsets_.polygon_offsets.prefix_sum_rings),
                ArrayView<POINT_T>(*points_), ArrayView<box_t>(*mbrs_)};
      }
    } else if constexpr (std::is_same_v<GeometryArrayView_T,
                                        MultiPolygonArrayView<point_t, INDEX_T>>) {
      if (offsets_.multi_polygon_offsets.prefix_sum_geoms != nullptr) {
        return {ArrayView<INDEX_T>(*offsets_.multi_polygon_offsets.prefix_sum_geoms),
                ArrayView<INDEX_T>(*offsets_.multi_polygon_offsets.prefix_sum_parts),
                ArrayView<INDEX_T>(*offsets_.multi_polygon_offsets.prefix_sum_rings),
                ArrayView<POINT_T>(*points_), ArrayView<box_t>(*mbrs_)};
      }
    } else if constexpr (std::is_same_v<GeometryArrayView_T,
                                        BoxArrayView<point_t, INDEX_T>>) {
      return {ArrayView<box_t>(*mbrs_)};
    } else {
      static_assert(sizeof(GeometryArrayView_T) == 0,
                    "Unsupported GeometryView type requested.");
    }
    return {};
  }

  struct MultiPointOffsets {
    // content is the index to points_
    std::unique_ptr<rmm::device_uvector<INDEX_T>> prefix_sum;
  };

  struct LineStringOffsets {
    // content is the index to points
    std::unique_ptr<rmm::device_uvector<INDEX_T>> prefix_sum;
  };

  struct MultiLineStringOffsets {
    // content is the index to prefix_sum_parts
    std::unique_ptr<rmm::device_uvector<INDEX_T>> prefix_sum_geoms;
    // content is the index to points
    std::unique_ptr<rmm::device_uvector<INDEX_T>> prefix_sum_parts;
  };

  struct PolygonOffsets {
    // content is the index to prefix_sum_rings
    std::unique_ptr<rmm::device_uvector<INDEX_T>> prefix_sum_polygons;
    // content is the index to points
    std::unique_ptr<rmm::device_uvector<INDEX_T>> prefix_sum_rings;
  };

  struct MultiPolygonOffsets {
    // content is the index to prefix_sum_parts
    std::unique_ptr<rmm::device_uvector<INDEX_T>> prefix_sum_geoms;
    // content is the index to prefix_sum_rings
    std::unique_ptr<rmm::device_uvector<INDEX_T>> prefix_sum_parts;
    // content is the index to points
    std::unique_ptr<rmm::device_uvector<INDEX_T>> prefix_sum_rings;
  };

  struct Offsets {
    MultiPointOffsets multi_point_offsets;
    LineStringOffsets line_string_offsets;
    MultiLineStringOffsets multi_line_string_offsets;
    PolygonOffsets polygon_offsets;
    MultiPolygonOffsets multi_polygon_offsets;
  };

  ArrayView<box_t> get_mbrs() const {
    if (mbrs_ != nullptr) {
      return ArrayView<box_t>(*mbrs_);
    }
    return {};
  }

  ArrayView<point_t> get_points() const {
    if (points_ != nullptr) {
      return ArrayView<point_t>(const_cast<point_t*>(points_->data()), points_->size());
    }
    return {};
  }

  Offsets& get_offsets() { return offsets_; }

  const Offsets& get_offsets() const { return offsets_; }

  GeometryType get_geometry_type() const { return type_; }

 private:
  friend class PointSegment<POINT_T>;
  friend class MultiPointSegment<POINT_T, INDEX_T>;
  friend class LineStringSegment<POINT_T, INDEX_T>;
  friend class MultiLineStringSegment<POINT_T, INDEX_T>;
  friend class PolygonSegment<POINT_T, INDEX_T>;
  friend class MultiPolygonSegment<POINT_T, INDEX_T>;
  friend class BoxSegment<POINT_T, INDEX_T>;

  GeometryType type_;
  // used by all the geometries
  std::unique_ptr<rmm::device_uvector<point_t>> points_;
  Offsets offsets_;
  // is null for points
  std::unique_ptr<rmm::device_uvector<box_t>> mbrs_;
};

}  // namespace gpuspatial
#endif  //  GPU_SPATIAL_LOADER_DEVICE_GEOMETRIES_CUH
