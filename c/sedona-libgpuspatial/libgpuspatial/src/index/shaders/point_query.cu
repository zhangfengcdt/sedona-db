#include <cuda_runtime.h>
#include <optix_device.h>
#include <cfloat>

#include "gpuspatial/index/detail/launch_parameters.h"
#include "shader_config.h"

enum { SURFACE_RAY_TYPE = 0, RAY_TYPE_COUNT };
// FLOAT_TYPE is defined by CMakeLists.txt
extern "C" __constant__
    gpuspatial::detail::LaunchParamsPointQuery<gpuspatial::ShaderPointType>
        params;

extern "C" __global__ void __intersection__gpuspatial() {
  auto aabb_id = optixGetPrimitiveIndex();
  auto geom2_id = optixGetPayload_0();
  const auto& point = params.points2[geom2_id];
  const auto& aabb = params.aabbs1[aabb_id];
  const auto& mbrs1 = params.mbrs1;

  if (point.covered_by(aabb)) {
    auto begin = params.prefix_sum[aabb_id];
    auto end = params.prefix_sum[aabb_id + 1];

    for (auto offset = begin; offset < end; offset++) {
      auto geom1_id = params.reordered_indices[offset];
      if (mbrs1.empty()) {
        params.ids.Append(thrust::make_pair(geom1_id, geom2_id));
      } else {
        const auto& mbr1 = mbrs1[geom1_id];

        if (mbr1.covers(point)) {
          params.ids.Append(thrust::make_pair(geom1_id, geom2_id));
        }
      }
    }
  }
}

extern "C" __global__ void __raygen__gpuspatial() {
  float tmin = 0;
  float tmax = FLT_MIN;

  for (uint32_t i = optixGetLaunchIndex().x; i < params.points2.size();
       i += optixGetLaunchDimensions().x) {
    const auto& p = params.points2[i];

    float3 origin;

    origin.x = p.get_coordinate(0);
    origin.y = p.get_coordinate(1);
    origin.z = 0;
    float3 dir = {0, 0, 1};

    optixTrace(params.handle, origin, dir, tmin, tmax, 0, OptixVisibilityMask(255),
               OPTIX_RAY_FLAG_NONE,  // OPTIX_RAY_FLAG_NONE,
               SURFACE_RAY_TYPE,     // SBT offset
               RAY_TYPE_COUNT,       // SBT stride
               SURFACE_RAY_TYPE,     // missSBTIndex
               i);
  }
}
