#include <cuda_runtime.h>
#include <optix_device.h>
#include <cfloat>

#include "gpuspatial/geom/id_encoder.cuh"
#include "gpuspatial/geom/line_segment.cuh"
#include "gpuspatial/index/detail/launch_parameters.h"
#include "gpuspatial/utils/floating_point.h"
#include "shader_config.h"

enum { SURFACE_RAY_TYPE = 0, RAY_TYPE_COUNT };
// FLOAT_TYPE is defined by CMakeLists.txt
extern "C" __constant__ gpuspatial::detail::LaunchParamsMultiPolygonPointQuery<
    gpuspatial::ShaderPointType, uint32_t>
    params;

extern "C" __global__ void __intersection__gpuspatial() {
  using namespace gpuspatial;
  using point_t = ShaderPointType;
  using scalar_t = typename point_t::scalar_t;
  auto aabb_id = optixGetPrimitiveIndex();
  auto query_idx = optixGetPayload_0();
  auto reordered_multi_polygon_idx = optixGetPayload_1();
  uint32_t v_offset = optixGetPayload_2();
  auto part_idx = optixGetPayload_3();
  auto ring_idx = optixGetPayload_4();
  auto crossing_count = optixGetPayload_5();
  auto point_on_seg = optixGetPayload_6();
  const auto& multi_polygons = params.multi_polygons;
  auto multi_polygon_idx = params.ids[query_idx].first;
  auto point_idx = params.ids[query_idx].second;

  // the seg being hit is not from the query polygon
  if (params.seg_multi_polygon_ids[aabb_id] != multi_polygon_idx) {
    return;
  }

  uint32_t local_v1_idx = aabb_id - params.seg_begins[reordered_multi_polygon_idx];
  uint32_t global_v1_idx = v_offset + local_v1_idx;
  uint32_t global_v2_idx = global_v1_idx + 1;
  uint32_t hit_geom_idx, hit_part_idx, hit_ring_idx;

  // bool found = multi_polygons.locate_vertex(global_v1_idx, hit_geom_idx, hit_part_idx,
                                            // hit_ring_idx);

  // assert(params.geom_ids[aabb_id] == hit_geom_idx);
  // assert(params.part_ids[aabb_id] == hit_part_idx);
  // assert(params.ring_ids[aabb_id] == hit_ring_idx);
  // assert(found);

  hit_geom_idx = params.geom_ids[aabb_id];
  hit_part_idx = params.part_ids[aabb_id];
  hit_ring_idx = params.ring_ids[aabb_id];

  if (hit_geom_idx == multi_polygon_idx && hit_part_idx == part_idx &&
      hit_ring_idx == ring_idx) {
    auto vertices = multi_polygons.get_vertices();
    // segment being hit
    const auto& v1 = vertices[global_v1_idx];
    const auto& v2 = vertices[global_v2_idx];
    const auto& p = params.points[point_idx];

    RayCrossingCounter locator(crossing_count, point_on_seg);
    locator.countSegment(p, v1, v2);
    optixSetPayload_5(locator.get_crossing_count());
    optixSetPayload_6(locator.get_point_on_segment());
  }
}

extern "C" __global__ void __raygen__gpuspatial() {
  using namespace gpuspatial;
  float tmin = 0;
  float tmax = FLT_MAX;  // use a very large value
  const auto& ids = params.ids;
  using point_t = ShaderPointType;
  const auto& multi_polygons = params.multi_polygons;
  RayCrossingCounter locator;

  for (uint32_t i = optixGetLaunchIndex().x; i < ids.size();
       i += optixGetLaunchDimensions().x) {
    auto multi_polygon_idx = ids[i].first;
    auto point_idx = ids[i].second;

    auto it = thrust::lower_bound(thrust::seq, params.multi_polygon_ids.begin(),
                                  params.multi_polygon_ids.end(), multi_polygon_idx);
    assert(it != params.multi_polygon_ids.end());
    uint32_t reordered_multi_polygon_idx =
        thrust::distance(params.multi_polygon_ids.begin(), it);
    assert(params.multi_polygon_ids[reordered_multi_polygon_idx] == multi_polygon_idx);

    const auto& p = params.points[point_idx];

    float3 origin;
    // each polygon takes a z-plane
    origin.x = p.x();
    origin.y = p.y();
    // origin.z = reordered_multi_polygon_idx;
    // cast ray toward positive x-axis
    float3 dir = {1, 0, 0};
    auto part_begin = params.part_begins[i];
    const auto& multi_polygon = multi_polygons[multi_polygon_idx];
    const auto& mbr = multi_polygon.get_mbr();
    auto width = mbr.get_max().x() - mbr.get_min().x();
    tmax = 2 * width;

    // first polygon offset
    uint32_t part_offset = multi_polygons.get_prefix_sum_geoms()[multi_polygon_idx];
    // first ring offset of the polygon
    uint32_t ring_offset = multi_polygons.get_prefix_sum_parts()[part_offset];
    // first vertex offset of the ring
    uint32_t v_offset = multi_polygons.get_prefix_sum_rings()[ring_offset];

    for (uint32_t part = 0; part < multi_polygon.num_polygons(); part++) {
      auto polygon = multi_polygon.get_polygon(part);
      uint32_t ring = 0;
      locator.Init();
      uint32_t encoded_z = ENCODE_UINT32_T_3(reordered_multi_polygon_idx, part, ring);
      origin.z = *reinterpret_cast<float*>(&encoded_z);
      uint32_t n_hits = 0;
      // test exterior
      optixTrace(params.handle, origin, dir, tmin, tmax, 0, OptixVisibilityMask(255),
                 OPTIX_RAY_FLAG_NONE,            // OPTIX_RAY_FLAG_NONE,
                 SURFACE_RAY_TYPE,               // SBT offset
                 RAY_TYPE_COUNT,                 // SBT stride
                 SURFACE_RAY_TYPE,               // missSBTIndex
                 i,                              // 0
                 reordered_multi_polygon_idx,    // 1
                 v_offset,                       // 2
                 part,                           // 3
                 ring,                           // 4
                 locator.get_crossing_count(),   // 5
                 locator.get_point_on_segment()  // 6
      );
      auto location = locator.location();
      PointLocation final_location = PointLocation::kError;
      if (location == PointLocation::kInside) {
        final_location = location;
        // test interior
        for (ring = 1; ring < polygon.num_rings(); ring++) {
          n_hits = 0;
          locator.Init();
          encoded_z = ENCODE_UINT32_T_3(reordered_multi_polygon_idx, part, ring);
          origin.z = *reinterpret_cast<float*>(&encoded_z);
          optixTrace(params.handle, origin, dir, tmin, tmax, 0, OptixVisibilityMask(255),
                     OPTIX_RAY_FLAG_NONE,            // OPTIX_RAY_FLAG_NONE,
                     SURFACE_RAY_TYPE,               // SBT offset
                     RAY_TYPE_COUNT,                 // SBT stride
                     SURFACE_RAY_TYPE,               // missSBTIndex
                     i,                              // 0
                     reordered_multi_polygon_idx,    // 1
                     v_offset,                       // 2
                     part,                           // 3
                     ring,                           // 4
                     locator.get_crossing_count(),   // 5
                     locator.get_point_on_segment()  // 6
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
      params.locations[part_begin + part] = final_location;
#ifndef NDEBUG
      auto ref_loc =
          multi_polygon.get_polygon(part).locate_point(params.points[point_idx]);
      if (ref_loc != final_location) {
        printf(
            "reorder %u, multi poly %u, point %u (%lf, %lf), num parts %u, num rings %u, part %u, point %u, loc %d, ref loc %d\n",
            reordered_multi_polygon_idx, multi_polygon_idx, point_idx, p.x(), p.y(),
            multi_polygon.num_polygons(), multi_polygon.get_polygon(0).num_rings(), part,
            point_idx, (int)final_location, (int)ref_loc);
        assert(false);
      }
#endif
    }
  }
}
