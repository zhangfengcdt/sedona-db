#ifndef GPUSPATIAL_INDEX_SHADERS_LAUNCH_PARAMETERS_H
#define GPUSPATIAL_INDEX_SHADERS_LAUNCH_PARAMETERS_H
#include "gpuspatial/geom/box.cuh"
#include "gpuspatial/geom/point.cuh"
#include "gpuspatial/geom/polygon.cuh"
#include "gpuspatial/geom/multi_polygon.cuh"
#include "gpuspatial/utils/array_view.h"
#include "gpuspatial/utils/queue_view.h"

#include <thrust/pair.h>

namespace gpuspatial {
namespace detail {

template <typename POINT_T>
struct LaunchParamsPointQuery {
  using box_t = Box<POINT_T>;
  // Input
  // Data structures of geometries1
  ArrayView<OptixAabb> aabbs1;  // MBRs of grouped geometries1
  ArrayView<uint32_t> prefix_sum;
  ArrayView<uint32_t> reordered_indices;
  ArrayView<box_t> mbrs1;
  OptixTraversableHandle handle;
  //  Data structures of geometries2
  ArrayView<POINT_T> points2;
  // Output: Geom1 ID, Geom2 ID
  QueueView<thrust::pair<uint32_t, uint32_t>> ids;
};

template <typename POINT_T>
struct LaunchParamsBoxQuery {
  using box_t = Box<POINT_T>;

  // Input
  // Data structures of geometries1
  ArrayView<OptixAabb> aabbs1;
  ArrayView<uint32_t> prefix_sum;
  ArrayView<uint32_t> reordered_indices;
  ArrayView<box_t> mbrs1;
  //  Data structures of geometries2
  ArrayView<box_t> mbrs2;
  // can be either geometries 1 or 2
  OptixTraversableHandle handle;
  // Output: Geom2 ID, Geom2 ID
  QueueView<thrust::pair<uint32_t, uint32_t>> ids;
};


template<typename POINT_T, typename INDEX_T>
struct LaunchParamsPolygonPointQuery {
  using point_t = POINT_T;
  using index_t = INDEX_T;
  PolygonArrayView<point_t, index_t> polygons;
  PointArrayView<point_t, index_t> points;
  ArrayView<uint32_t> polygon_ids; // sorted
  ArrayView<thrust::pair<index_t, index_t>> ids;
  ArrayView<index_t> seg_begins;
  ArrayView<index_t> seg_polygon_ids;
  OptixTraversableHandle handle;
  ArrayView<PointLocation> locations;
};


}  // namespace detail

}  // namespace gpuspatial
#endif  // GPUSPATIAL_INDEX_SHADERS_LAUNCH_PARAMETERS_H
