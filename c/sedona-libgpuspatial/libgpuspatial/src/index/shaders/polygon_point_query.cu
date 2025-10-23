#include <cuda_runtime.h>
#include <optix_device.h>
#include <cfloat>

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
  using point_t = ShaderPointType;
  using equation_t = EdgeEquation<point_t>;
  auto aabb_id = optixGetPrimitiveIndex();
  auto query_idx = optixGetPayload_0();
  auto point_idx = optixGetPayload_1();
  auto polygon_idx = optixGetPayload_2();            // polygon id in the polygon array
  auto reordered_polygon_idx = optixGetPayload_3();  // ith polygon from "ids"

  if (params.seg_polygon_ids[aabb_id] != polygon_idx) {
    return;
  }

  auto local_v1_idx = aabb_id - params.seg_begins[reordered_polygon_idx];
  const auto& polygons = params.polygons;
  // first ring offset of the polygon
  auto ring_offset = polygons.get_prefix_sum_polygons()[polygon_idx];
  // first vertex offset of the ring
  auto v_offset = polygons.get_prefix_sum_rings()[ring_offset];
  auto global_v1_idx = v_offset + local_v1_idx;
  auto global_v2_idx = global_v1_idx + 1;
  uint32_t v1_polygon_idx, v1_ring_idx;
  bool found = polygons.locate_vertex(global_v1_idx, v1_polygon_idx, v1_ring_idx);
  assert(found);
  assert(v1_polygon_idx == polygon_idx);

  auto vertices = polygons.get_vertices();
  const auto& v1 = vertices[global_v1_idx];
  const auto& v2 = vertices[global_v2_idx];
  auto x_min = std::min(v1.x(), v2.x());
  auto x_max = std::max(v1.x(), v2.x());

  // query point
  const auto& p = params.points[point_idx];

  // out of range, the point cannot hit the seg
  if (p.x() < x_min || p.x() > x_max) {
    return;
  }


  // this is a vertical seg
  if (x_min == x_max) {
    auto y_min = std::min(v1.y(), v2.y());
    auto y_max = std::max(v1.y(), v2.y());
    // p is on the seg
    if (x_min == p.x() && p.y() >= y_min && p.y() <= y_max) {
      params.locations[query_idx] = PointLocation::kBoundary;
    }
    return;
  }

  equation_t e(v1, v2);
  // now, the seg should not be vertical
  assert(e.b != 0);

  auto xsect_y = (-e.a * p.x() - e.c) / e.b;

  if (e.a * p.x() + e.b * p.y() + e.c == 0) {
    params.locations[query_idx] = PointLocation::kBoundary;
  }

  // printf("hit, x min %lf, max %lf, x %lf, xsect y %lf, y %lf\n", x_min, x_max, p.x(),
    // xsect_y, p.y());


  auto diff_y = xsect_y - p.y();
  // current point is above the current edge
  if (diff_y < 0) {
    return;
  }

  uint2 best_y_storage{optixGetPayload_4(), optixGetPayload_5()};
  double best_y;
  unpack64(best_y_storage.x, best_y_storage.y, &best_y);

  // printf("xsect y %lf\n", xsect_y);

  // find closest line seg to the query point
  if (xsect_y < best_y) {
    PointLocation location = PointLocation::kError;
    // exterior ring is counterclockwise
    // interior ring is clockwise , so they have consistent faces
    PointLocation left_face = PointLocation::kInside;
    PointLocation right_face = PointLocation::kOutside;

    if (v1.x() < v2.x()) {
      location = right_face;
    } else {
      location = left_face;
    }
    // printf("location %d\n", (int)location);
    params.locations[query_idx] = location;
    best_y = xsect_y;
    pack64(&best_y, best_y_storage.x, best_y_storage.y);
    optixSetPayload_4(best_y_storage.x);
    optixSetPayload_5(best_y_storage.y);
    optixReportIntersection(diff_y, 0);
  }
}

extern "C" __global__ void __raygen__gpuspatial() {
  using namespace gpuspatial;
  float tmin = 0;
  float tmax = 1e10; // use a very large value
  const auto& ids = params.ids;
  for (uint32_t i = optixGetLaunchIndex().x; i < ids.size();
       i += optixGetLaunchDimensions().x) {
    auto polygon_idx = ids[i].first;
    auto point_idx = ids[i].second;

    // if (!(polygon_idx == 947 && point_idx == 486)) {
    //   return;
    // }


    // auto pip = params.polygons[polygon_idx].Contains(params.points[point_idx]);
    // if (pip) {
    //   printf("polygon %u, point %u\n", polygon_idx, point_idx);
    // }

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
    // cast ray toward positive y-axis
    float3 dir = {0, 1, 0};

    auto best_y = std::numeric_limits<double>::max();
    // best means closest line seg from the query point
    uint2 best_y_storage;

    pack64(&best_y, best_y_storage.x, best_y_storage.y);

    // printf("ray origin (%lf, %lf), reorder %u\n", params.points[point_idx].x(), params.points[point_idx].y(), reordered_polygon_idx);


    optixTrace(params.handle, origin, dir, tmin, tmax, 0, OptixVisibilityMask(255),
               OPTIX_RAY_FLAG_NONE,  // OPTIX_RAY_FLAG_NONE,
               SURFACE_RAY_TYPE,     // SBT offset
               RAY_TYPE_COUNT,       // SBT stride
               SURFACE_RAY_TYPE,     // missSBTIndex
               i, point_idx, polygon_idx, reordered_polygon_idx, best_y_storage.x,
               best_y_storage.y);
    unpack64(best_y_storage.x, best_y_storage.y, &best_y);
    // the ray hits nothing, so point is outside of the polygon
    if (best_y == std::numeric_limits<double>::max()) {
      params.locations[i] = PointLocation::kOutside;
    }

    auto loc = params.locations[i];
    auto ref_loc = params.polygons[polygon_idx].locate_point(params.points[point_idx]);
    // if (loc != ref_loc) {
    //   printf("poly %u, point %u, loc %d, ref loc %d\n", polygon_idx, point_idx, (int)loc,
    //          (int)ref_loc);
    // }
  }
}
