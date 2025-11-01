#include <cuda_runtime.h>
#include <optix_device.h>
#include <cfloat>

#include "gpuspatial/geom/id_encoder.cuh"
#include "gpuspatial/geom/line_segment.cuh"
#include "gpuspatial/index/detail/launch_parameters.h"
#include "shader_config.h"

enum { SURFACE_RAY_TYPE = 0, RAY_TYPE_COUNT };
// FLOAT_TYPE is defined by CMakeLists.txt
extern "C" __constant__ gpuspatial::detail::LaunchParamsPolygonPointQuery<
    gpuspatial::ShaderPointType, uint32_t>
    params;

extern "C" __global__ void __intersection__gpuspatial() {
  using namespace gpuspatial;
  auto aabb_id = optixGetPrimitiveIndex();
  auto query_idx = optixGetPayload_0();
  auto reordered_polygon_idx = optixGetPayload_1();
  uint32_t v_offset = optixGetPayload_2();
  auto ring_idx = optixGetPayload_3();
  auto crossing_count = optixGetPayload_4();
  auto point_on_seg = optixGetPayload_5();
  const auto& polygons = params.polygons;
  auto polygon_idx = params.ids[query_idx].first;
  auto point_idx = params.ids[query_idx].second;
  auto hit_polygon_idx = params.aabb_poly_ids[aabb_id];
  auto hit_ring_idx = params.aabb_ring_ids[aabb_id];
  // the seg being hit is not from the query polygon
  if (hit_polygon_idx != polygon_idx || hit_ring_idx != ring_idx) {
    return;
  }

  uint32_t local_v1_idx = aabb_id - params.seg_begins[reordered_polygon_idx];
  uint32_t global_v1_idx = v_offset + local_v1_idx;
  uint32_t global_v2_idx = global_v1_idx + 1;

  auto vertices = polygons.get_vertices();
  // segment being hit
  const auto& v1 = vertices[global_v1_idx];
  const auto& v2 = vertices[global_v2_idx];
  const auto& p = params.points[point_idx];

  RayCrossingCounter locator(crossing_count, point_on_seg);
  locator.countSegment(p, v1, v2);
  optixSetPayload_4(locator.get_crossing_count());
  optixSetPayload_5(locator.get_point_on_segment());
}

extern "C" __global__ void __raygen__gpuspatial() {
  using namespace gpuspatial;
  float tmin = 0;
  float tmax = FLT_MAX;  // use a very large value
  const auto& ids = params.ids;
  const auto& polygons = params.polygons;
  RayCrossingCounter locator;

  for (uint32_t i = optixGetLaunchIndex().x; i < ids.size();
       i += optixGetLaunchDimensions().x) {
    auto polygon_idx = ids[i].first;
    auto point_idx = ids[i].second;

    auto it = thrust::lower_bound(thrust::seq, params.polygon_ids.begin(),
                                  params.polygon_ids.end(), polygon_idx);
    assert(it != params.polygon_ids.end());
    uint32_t reordered_polygon_idx = thrust::distance(params.polygon_ids.begin(), it);
    assert(params.polygon_ids[reordered_polygon_idx] == polygon_idx);

    const auto& p = params.points[point_idx];

    float3 origin;
    // each polygon takes a z-plane
    origin.x = p.x();
    origin.y = p.y();
    origin.z = reordered_polygon_idx;
    // cast ray toward positive x-axis
    float3 dir = {1, 0, 0};
    const auto& polygon = polygons[polygon_idx];
    const auto& mbr = polygon.get_mbr();
    auto width = mbr.get_max().x() - mbr.get_min().x();
    tmax = 2 * width;

    // first polygon offset
    uint32_t ring_offset = polygons.get_prefix_sum_polygons()[polygon_idx];
    // first vertex offset of the ring
    uint32_t v_offset = polygons.get_prefix_sum_rings()[ring_offset];

    uint32_t ring = 0;
    locator.Init();

    // test exterior
    optixTrace(params.handle, origin, dir, tmin, tmax, 0, OptixVisibilityMask(255),
               OPTIX_RAY_FLAG_NONE,            // OPTIX_RAY_FLAG_NONE,
               SURFACE_RAY_TYPE,               // SBT offset
               RAY_TYPE_COUNT,                 // SBT stride
               SURFACE_RAY_TYPE,               // missSBTIndex
               i,                              // 0
               reordered_polygon_idx,          // 1
               v_offset,                       // 2
               ring,                           // 3
               locator.get_crossing_count(),   // 4
               locator.get_point_on_segment()  // 5
    );
    auto location = locator.location();
    PointLocation final_location = PointLocation::kError;
    if (location == PointLocation::kInside) {
      final_location = location;
      // test interior
      for (ring = 1; ring < polygon.num_rings(); ring++) {
        locator.Init();
        optixTrace(params.handle, origin, dir, tmin, tmax, 0, OptixVisibilityMask(255),
                   OPTIX_RAY_FLAG_NONE,            // OPTIX_RAY_FLAG_NONE,
                   SURFACE_RAY_TYPE,               // SBT offset
                   RAY_TYPE_COUNT,                 // SBT stride
                   SURFACE_RAY_TYPE,               // missSBTIndex
                   i,                              // 0
                   reordered_polygon_idx,          // 1
                   v_offset,                       // 2
                   ring,                           // 3
                   locator.get_crossing_count(),   // 4
                   locator.get_point_on_segment()  // 5
        );
        location = locator.location();
        if (location == PointLocation::kBoundary) {
          final_location = PointLocation::kBoundary;
          break;
        } else if (location == PointLocation::kInside) {
          final_location = PointLocation::kOutside;
          break;
        }
      }
    } else {
      // outside or boundary
      final_location = location;
    }
    assert(final_location != PointLocation::kError);
    params.locations[i] = final_location;
#ifndef NDEBUG
    auto ref_loc = polygon.locate_point(params.points[point_idx]);
    if (ref_loc != final_location) {
      printf(
          "reorder %u, poly %u, point %u (%lf, %lf), num rings %u, point %u, loc %d, ref loc %d\n",
          reordered_polygon_idx, polygon_idx, point_idx, p.x(), p.y(),
          polygon.num_rings(), point_idx, (int)final_location, (int)ref_loc);
      assert(false);
    }
#endif
  }
}
