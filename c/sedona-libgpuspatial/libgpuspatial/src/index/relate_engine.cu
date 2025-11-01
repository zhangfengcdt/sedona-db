#include "gpuspatial/geom/id_encoder.cuh"
#include "gpuspatial/index/detail/launch_parameters.h"
#include "gpuspatial/index/geometry_grouper.hpp"
#include "gpuspatial/index/relate_engine.cuh"
#include "gpuspatial/loader/device_geometries.cuh"
#include "gpuspatial/relate/predicate.cuh"
#include "gpuspatial/relate/relate.cuh"
#include "gpuspatial/utils/array_view.h"
#include "gpuspatial/utils/helpers.h"
#include "gpuspatial/utils/launcher.h"
#include "gpuspatial/utils/queue.h"
#include "gpuspatial/utils/stopwatch.h"
#include "index/shaders/shader_id.hpp"

#include <thrust/remove.h>
#include <thrust/sort.h>
#include <thrust/unique.h>
#include <rmm/cuda_stream_view.hpp>
#include <rmm/exec_policy.hpp>

#include "gpuspatial/utils/stopwatch.h"

namespace gpuspatial {
namespace detail {
DEV_HOST_INLINE bool EvaluatePredicate(Predicate p, int32_t im) {
  switch (p) {
    case Predicate::kEquals: {
      return (im & IM__INTER_INTER_2D) != 0 && (im & IM__INTER_EXTER_2D) == 0 &&
             (im & IM__BOUND_EXTER_2D) == 0 && (im & IM__EXTER_INTER_2D) == 0 &&
             (im & IM__EXTER_BOUND_2D) == 0;
    }
    case Predicate::kDisjoint: {
      return (im & IM__INTER_INTER_2D) == 0 && (im & IM__INTER_BOUND_2D) == 0 &&
             (im & IM__BOUND_INTER_2D) == 0 && (im & IM__BOUND_BOUND_2D) == 0;
    }
    case Predicate::kTouches: {
      return (im & IM__INTER_INTER_2D) == 0 &&
             ((im & IM__INTER_BOUND_2D) != 0 || (im & IM__BOUND_INTER_2D) != 0 ||
              (im & IM__BOUND_BOUND_2D) != 0);
    }
    case Predicate::kContains: {
      return (im & IM__INTER_INTER_2D) != 0 && (im & IM__EXTER_INTER_2D) == 0 &&
             (im & IM__EXTER_BOUND_2D) == 0;
    }
    case Predicate::kCovers: {
      return (im & IM__EXTER_INTER_2D) == 0 && (im & IM__EXTER_BOUND_2D) == 0 &&
             ((im & IM__INTER_INTER_2D) != 0 || (im & IM__INTER_BOUND_2D) != 0 ||
              (im & IM__BOUND_INTER_2D) != 0 || (im & IM__BOUND_BOUND_2D) != 0);
    }
    case Predicate::kIntersects: {
      return (im & IM__INTER_INTER_2D) != 0 || (im & IM__INTER_BOUND_2D) != 0 ||
             (im & IM__BOUND_INTER_2D) != 0 || (im & IM__BOUND_BOUND_2D) != 0;
    }
    case Predicate::kWithin: {
      return (im & IM__INTER_INTER_2D) != 0 && (im & IM__INTER_EXTER_2D) == 0 &&
             (im & IM__BOUND_EXTER_2D) == 0;
    }
    case Predicate::kCoveredBy: {
      return (im & IM__INTER_EXTER_2D) == 0 && (im & IM__BOUND_EXTER_2D) == 0 &&
             ((im & IM__INTER_INTER_2D) != 0 || (im & IM__INTER_BOUND_2D) != 0 ||
              (im & IM__BOUND_INTER_2D) != 0 || (im & IM__BOUND_BOUND_2D) != 0);
    }
    default:
      assert(false);
  }
  return false;
}
}  // namespace detail

template <typename POINT_T, typename INDEX_T>
RelateEngine<POINT_T, INDEX_T>::RelateEngine(
    const DeviceGeometries<POINT_T, INDEX_T>* geoms1)
    : geoms1_(geoms1) {}

template <typename POINT_T, typename INDEX_T>
RelateEngine<POINT_T, INDEX_T>::RelateEngine(
    const DeviceGeometries<POINT_T, INDEX_T>* geoms1, const details::RTEngine* rt_engine)
    : geoms1_(geoms1), rt_engine_(rt_engine) {}

template <typename POINT_T, typename INDEX_T>
void RelateEngine<POINT_T, INDEX_T>::Evaluate(
    const rmm::cuda_stream_view& stream, const DeviceGeometries<POINT_T, INDEX_T>& geoms2,
    Predicate predicate, Queue<thrust::pair<uint32_t, uint32_t>>& ids) {
  switch (geoms2.get_geometry_type()) {
    case GeometryType::kPoint: {
      using geom2_array_view_t = PointArrayView<POINT_T, INDEX_T>;
      Evaluate(stream, geoms2.template GetGeometryArrayView<geom2_array_view_t>(),
               predicate, ids);
      break;
    }
    case GeometryType::kMultiPoint: {
      using geom2_array_view_t = MultiPointArrayView<POINT_T, INDEX_T>;
      Evaluate(stream, geoms2.template GetGeometryArrayView<geom2_array_view_t>(),
               predicate, ids);
      break;
    }
    case GeometryType::kLineString: {
      using geom2_array_view_t = LineStringArrayView<POINT_T, INDEX_T>;
      Evaluate(stream, geoms2.template GetGeometryArrayView<geom2_array_view_t>(),
               predicate, ids);
      break;
    }
    case GeometryType::kMultiLineString: {
      using geom2_array_view_t = MultiLineStringArrayView<POINT_T, INDEX_T>;
      Evaluate(stream, geoms2.template GetGeometryArrayView<geom2_array_view_t>(),
               predicate, ids);
      break;
    }
    case GeometryType::kPolygon: {
      using geom2_array_view_t = PolygonArrayView<POINT_T, INDEX_T>;
      Evaluate(stream, geoms2.template GetGeometryArrayView<geom2_array_view_t>(),
               predicate, ids);
      break;
    }
    case GeometryType::kMultiPolygon: {
      using geom2_array_view_t = MultiPolygonArrayView<POINT_T, INDEX_T>;
      Evaluate(stream, geoms2.template GetGeometryArrayView<geom2_array_view_t>(),
               predicate, ids);
      break;
    }
    default:
      assert(false);
  }
}

template <typename POINT_T, typename INDEX_T>
template <typename GEOM2_ARRAY_VIEW_T>
void RelateEngine<POINT_T, INDEX_T>::Evaluate(
    const rmm::cuda_stream_view& stream, const GEOM2_ARRAY_VIEW_T& geom_array2,
    Predicate predicate, Queue<thrust::pair<uint32_t, uint32_t>>& ids) {
  switch (geoms1_->get_geometry_type()) {
    case GeometryType::kPoint: {
      using geom1_array_view_t = PointArrayView<POINT_T, INDEX_T>;
      Evaluate(stream, geoms1_->template GetGeometryArrayView<geom1_array_view_t>(),
               geom_array2, predicate, ids);
      break;
    }
    case GeometryType::kMultiPoint: {
      using geom1_array_view_t = MultiPointArrayView<POINT_T, INDEX_T>;
      Evaluate(stream, geoms1_->template GetGeometryArrayView<geom1_array_view_t>(),
               geom_array2, predicate, ids);
      break;
    }
    case GeometryType::kLineString: {
      using geom1_array_view_t = LineStringArrayView<POINT_T, INDEX_T>;
      Evaluate(stream, geoms1_->template GetGeometryArrayView<geom1_array_view_t>(),
               geom_array2, predicate, ids);
      break;
    }
    case GeometryType::kMultiLineString: {
      using geom1_array_view_t = MultiLineStringArrayView<POINT_T, INDEX_T>;
      Evaluate(stream, geoms1_->template GetGeometryArrayView<geom1_array_view_t>(),
               geom_array2, predicate, ids);
      break;
    }
    case GeometryType::kPolygon: {
      using geom1_array_view_t = PolygonArrayView<POINT_T, INDEX_T>;
      Evaluate(stream, geoms1_->template GetGeometryArrayView<geom1_array_view_t>(),
               geom_array2, predicate, ids);
      break;
    }
    case GeometryType::kMultiPolygon: {
      using geom1_array_view_t = MultiPolygonArrayView<POINT_T, INDEX_T>;
      Evaluate(stream, geoms1_->template GetGeometryArrayView<geom1_array_view_t>(),
               geom_array2, predicate, ids);
      break;
    }
    default:
      assert(false);
  }
}

template <typename POINT_T, typename INDEX_T>
template <typename GEOM1_ARRAY_VIEW_T, typename GEOM2_ARRAY_VIEW_T>
void RelateEngine<POINT_T, INDEX_T>::Evaluate(
    const rmm::cuda_stream_view& stream, const GEOM1_ARRAY_VIEW_T& geom_array1,
    const GEOM2_ARRAY_VIEW_T& geom_array2, Predicate predicate,
    Queue<thrust::pair<uint32_t, uint32_t>>& ids) {
  auto ids_size = ids.size(stream);

  auto end = thrust::remove_if(
      rmm::exec_policy_nosync(stream), ids.data(), ids.data() + ids_size,
      [=] __device__(const thrust::pair<uint32_t, uint32_t>& pair) {
        auto geom1_id = pair.first;
        auto geom2_id = pair.second;
        const auto& geom1 = geom_array1[geom1_id];
        const auto& geom2 = geom_array2[geom2_id];

        auto IM = relate(geom1, geom2);
        return !detail::EvaluatePredicate(predicate, IM);
      });
  auto new_size = end - ids.data();
  ids.set_size(stream, end - ids.data());
}

template <typename POINT_T, typename INDEX_T>
void RelateEngine<POINT_T, INDEX_T>::Evaluate(
    const rmm::cuda_stream_view& stream,
    const PolygonArrayView<POINT_T, INDEX_T>& geom_array1,
    const PointArrayView<POINT_T, INDEX_T>& geom_array2, Predicate predicate,
    Queue<thrust::pair<uint32_t, uint32_t>>& ids) {
  auto ids_size = ids.size(stream);

  if (ids_size == 0) {
    return;
  }
  // sort by polygon id
  rmm::device_uvector<uint32_t> geom1_ids(ids_size, stream);

  thrust::transform(
      rmm::exec_policy_nosync(stream), ids.data(), ids.data() + ids_size,
      geom1_ids.data(),
      [] __device__(const thrust::pair<uint32_t, uint32_t>& pair) { return pair.first; });

  thrust::sort(rmm::exec_policy_nosync(stream), geom1_ids.begin(), geom1_ids.end());

  auto geom1_ids_end =
      thrust::unique(rmm::exec_policy_nosync(stream), geom1_ids.begin(), geom1_ids.end());
  geom1_ids.resize(thrust::distance(geom1_ids.begin(), geom1_ids_end), stream);
  geom1_ids.shrink_to_fit(stream);

  rmm::device_uvector<INDEX_T> seg_begins(0, stream);
  rmm::device_buffer bvh_buffer(0, stream);
  rmm::device_uvector<PointLocation> locations(ids_size, stream);
  rmm::device_uvector<INDEX_T> aabb_poly_ids(0, stream), aabb_ring_ids(0, stream);

  // aabb id -> vertex begin[polygon] + ith point in this polygon
  auto handle = BuildBVH(stream, geom_array1, ArrayView<INDEX_T>(geom1_ids), seg_begins,
                         bvh_buffer, aabb_poly_ids, aabb_ring_ids);

  using params_t = detail::LaunchParamsPolygonPointQuery<POINT_T, INDEX_T>;

  params_t params;

  params.polygons = geom_array1;
  params.points = geom_array2;
  params.polygon_ids = ArrayView<INDEX_T>(geom1_ids);
  params.ids = ArrayView<thrust::pair<uint32_t, uint32_t>>(ids.data(), ids_size);
  params.seg_begins = ArrayView<INDEX_T>(seg_begins);
  params.locations = ArrayView<PointLocation>(locations);
  params.handle = handle;
  params.aabb_poly_ids = ArrayView<INDEX_T>(aabb_poly_ids);
  params.aabb_ring_ids = ArrayView<INDEX_T>(aabb_ring_ids);

  rmm::device_buffer params_buffer(sizeof(params_t), stream);

  CUDA_CHECK(cudaMemcpyAsync(params_buffer.data(), &params, sizeof(params_t),
                             cudaMemcpyHostToDevice, stream.value()));

  rt_engine_->Render(stream, GetPolygonPointQueryShaderId<POINT_T>(),
                     dim3{ids_size, 1, 1},
                     ArrayView<char>((char*)params_buffer.data(), params_buffer.size()));

  auto* p_locations = locations.data();
  auto* p_ids = ids.data();

  auto invalid_pair = thrust::make_pair(std::numeric_limits<uint32_t>::max(),
                                        std::numeric_limits<uint32_t>::max());

  thrust::transform(rmm::exec_policy_nosync(stream),
                    thrust::make_counting_iterator<uint32_t>(0),
                    thrust::make_counting_iterator<uint32_t>(ids_size), ids.data(),
                    [=] __device__(uint32_t i) {
                      const auto& pair = p_ids[i];
                      auto geom1_id = pair.first;
                      auto geom2_id = pair.second;
                      const auto& geom1 = geom_array1[geom1_id];
                      const auto& geom2 = geom_array2[geom2_id];

                      auto IM = relate(geom1, geom2, p_locations[i]);
                      if (detail::EvaluatePredicate(predicate, IM)) {
                        return pair;
                      } else {
                        return invalid_pair;
                      }
                    });

  auto end = thrust::remove_if(
      rmm::exec_policy_nosync(stream), ids.data(), ids.data() + ids_size,
      [=] __device__(const thrust::pair<uint32_t, uint32_t>& pair) {
        return pair == invalid_pair;
      });
  ids.set_size(stream, end - ids.data());
}

template <typename POINT_T, typename INDEX_T>
void RelateEngine<POINT_T, INDEX_T>::Evaluate(
    const rmm::cuda_stream_view& stream,
    const MultiPolygonArrayView<POINT_T, INDEX_T>& geom_array1,
    const PointArrayView<POINT_T, INDEX_T>& geom_array2, Predicate predicate,
    Queue<thrust::pair<uint32_t, uint32_t>>& ids) {
  Stopwatch sw;
  sw.start();
  auto ids_size = ids.size(stream);

  if (ids_size == 0) {
    return;
  }
  // sort by polygon id
  rmm::device_uvector<uint32_t> geom1_ids(ids_size, stream);

  thrust::transform(
      rmm::exec_policy_nosync(stream), ids.data(), ids.data() + ids_size,
      geom1_ids.data(),
      [] __device__(const thrust::pair<uint32_t, uint32_t>& pair) { return pair.first; });

  thrust::sort(rmm::exec_policy_nosync(stream), geom1_ids.begin(), geom1_ids.end());

  auto geom1_ids_end =
      thrust::unique(rmm::exec_policy_nosync(stream), geom1_ids.begin(), geom1_ids.end());
  geom1_ids.resize(thrust::distance(geom1_ids.begin(), geom1_ids_end), stream);
  geom1_ids.shrink_to_fit(stream);

  printf("<multipoly,point> pairs %u, unique multipoly %lu\n", ids_size,
         geom1_ids.size());

  // number of polygons in each multipolygon
  rmm::device_uvector<uint32_t> num_parts(ids_size, stream);
  rmm::device_uvector<uint32_t> part_begins(num_parts.size() + 1, stream);

  thrust::transform(rmm::exec_policy_nosync(stream), ids.data(), ids.data() + ids_size,
                    num_parts.begin(),
                    [=] __device__(const thrust::pair<uint32_t, uint32_t>& pair) {
                      auto multi_polygon_idx = pair.first;
                      return geom_array1[multi_polygon_idx].num_polygons();
                    });

  part_begins.set_element_to_zero_async(0, stream);
  thrust::inclusive_scan(rmm::exec_policy_nosync(stream), num_parts.begin(),
                         num_parts.end(), part_begins.begin() + 1);
  num_parts.resize(0, stream);
  num_parts.shrink_to_fit(stream);

  auto n_parts = part_begins.back_element(stream);

  rmm::device_uvector<PointLocation> part_locations(n_parts, stream);

  // init with outside. if no hit, point is outside by default
  // aabb id -> vertex begin[polygon] + ith point in this polygon

  rmm::device_uvector<INDEX_T> seg_begins(0, stream);
  rmm::device_buffer bvh_buffer(0, stream);
  rmm::device_uvector<INDEX_T> aabb_multi_poly_ids(0, stream), aabb_part_ids(0, stream),
      aabb_ring_ids(0, stream);
  auto handle = BuildBVH(stream, geom_array1, ArrayView<INDEX_T>(geom1_ids), seg_begins,
                         bvh_buffer, aabb_multi_poly_ids, aabb_part_ids, aabb_ring_ids);

  using params_t = detail::LaunchParamsMultiPolygonPointQuery<POINT_T, INDEX_T>;

  rmm::device_uvector<uint32_t> hit_counters(ids_size, stream);

  thrust::fill(rmm::exec_policy_nosync(stream), hit_counters.begin(), hit_counters.end(),
               0);
  stream.synchronize();
  sw.stop();

  printf("prepare time %lf ms\n", sw.ms());

  params_t params;

  params.multi_polygons = geom_array1;
  params.points = geom_array2;
  params.multi_polygon_ids = ArrayView<INDEX_T>(geom1_ids);
  params.ids = ArrayView<thrust::pair<uint32_t, uint32_t>>(ids.data(), ids_size);
  params.seg_begins = ArrayView<INDEX_T>(seg_begins);
  params.part_begins = ArrayView<INDEX_T>(part_begins);
  params.locations = ArrayView<PointLocation>(part_locations);
  params.handle = handle;
  params.aabb_multi_poly_ids = ArrayView<INDEX_T>(aabb_multi_poly_ids);
  params.aabb_part_ids = ArrayView<INDEX_T>(aabb_part_ids);
  params.aabb_ring_ids = ArrayView<INDEX_T>(aabb_ring_ids);
  params.hit_counters = ArrayView<uint32_t>(hit_counters);

  rmm::device_buffer params_buffer(sizeof(params_t), stream);

  CUDA_CHECK(cudaMemcpyAsync(params_buffer.data(), &params, sizeof(params_t),
                             cudaMemcpyHostToDevice, stream.value()));

  stream.synchronize();

  sw.start();
  rt_engine_->Render(stream, GetMultiPolygonPointQueryShaderId<POINT_T>(),
                     dim3{ids_size, 1, 1},
                     ArrayView<char>((char*)params_buffer.data(), params_buffer.size()));
  stream.synchronize();
  sw.stop();

  auto total_hits = thrust::reduce(rmm::exec_policy_nosync(stream), hit_counters.begin(),
                                   hit_counters.end(), 0ul, thrust::plus<uint32_t>());

  printf("trace time %f ms, total hits %lu, avg hits %lu\n", sw.ms(), total_hits,
         total_hits / ids_size);
  auto* p_part_locations = part_locations.data();
  auto* p_part_begins = part_begins.data();
  auto* p_ids = ids.data();

  auto invalid_pair = thrust::make_pair(std::numeric_limits<uint32_t>::max(),
                                        std::numeric_limits<uint32_t>::max());

  thrust::transform(
      rmm::exec_policy_nosync(stream), thrust::make_counting_iterator<uint32_t>(0),
      thrust::make_counting_iterator<uint32_t>(ids_size), ids.data(),
      [=] __device__(uint32_t i) {
        const auto& pair = p_ids[i];
        auto geom1_id = pair.first;
        auto geom2_id = pair.second;
        const auto& geom1 = geom_array1[geom1_id];
        const auto& geom2 = geom_array2[geom2_id];
        auto begin = p_part_begins[i];

        auto IM = relate(
            geom1, geom2,
            ArrayView<PointLocation>(&p_part_locations[begin], geom1.num_polygons()));
        if (detail::EvaluatePredicate(predicate, IM)) {
          return pair;
        } else {
          return invalid_pair;
        }
      });

  auto end = thrust::remove_if(
      rmm::exec_policy_nosync(stream), ids.data(), ids.data() + ids_size,
      [=] __device__(const thrust::pair<uint32_t, uint32_t>& pair) {
        return pair == invalid_pair;
      });
  ids.set_size(stream, end - ids.data());
  printf("Result size %u\n", end - ids.data());
}

template <typename POINT_T, typename INDEX_T>
void RelateEngine<POINT_T, INDEX_T>::Evaluate(
    const rmm::cuda_stream_view& stream,
    const PointArrayView<POINT_T, INDEX_T>& geom_array1,
    const PolygonArrayView<POINT_T, INDEX_T>& geom_array2, Predicate predicate,
    Queue<thrust::pair<uint32_t, uint32_t>>& ids) {
  thrust::transform(rmm::exec_policy_nosync(stream), ids.data(),
                    ids.data() + ids.size(stream), ids.data(),
                    [] __device__(const thrust::pair<uint32_t, uint32_t>& pair)
                        -> thrust::pair<uint32_t, uint32_t> {
                      return thrust::make_pair(pair.second, pair.first);
                    });
  Evaluate(stream, geom_array2, geom_array1, predicate, ids);
}

template <typename POINT_T, typename INDEX_T>
void RelateEngine<POINT_T, INDEX_T>::Evaluate(
    const rmm::cuda_stream_view& stream,
    const PointArrayView<POINT_T, INDEX_T>& geom_array1,
    const MultiPolygonArrayView<POINT_T, INDEX_T>& geom_array2, Predicate predicate,
    Queue<thrust::pair<uint32_t, uint32_t>>& ids) {
  thrust::transform(rmm::exec_policy_nosync(stream), ids.data(),
                    ids.data() + ids.size(stream), ids.data(),
                    [] __device__(const thrust::pair<uint32_t, uint32_t>& pair)
                        -> thrust::pair<uint32_t, uint32_t> {
                      return thrust::make_pair(pair.second, pair.first);
                    });
  Evaluate(stream, geom_array2, geom_array1, predicate, ids);
}

template <typename POINT_T, typename INDEX_T>
OptixTraversableHandle RelateEngine<POINT_T, INDEX_T>::BuildBVH(
    const rmm::cuda_stream_view& stream,
    const PolygonArrayView<POINT_T, INDEX_T>& polygons, ArrayView<uint32_t> polygon_ids,
    rmm::device_uvector<INDEX_T>& seg_begins, rmm::device_buffer& buffer,
    rmm::device_uvector<INDEX_T>& aabb_poly_ids,
    rmm::device_uvector<INDEX_T>& aabb_ring_ids) {
  auto n_polygons = polygon_ids.size();
  rmm::device_uvector<uint32_t> n_segs(n_polygons, stream);

  // TODO: warp reduce
  thrust::transform(rmm::exec_policy_nosync(stream), polygon_ids.begin(),
                    polygon_ids.end(), n_segs.begin(),
                    [=] __device__(const uint32_t& id) -> uint32_t {
                      const auto& polygon = polygons[id];
                      uint32_t total_segs = 0;

                      for (int ring = 0; ring < polygon.num_rings(); ring++) {
                        total_segs += polygon.get_ring(ring).num_points();
                      }
                      return total_segs;
                    });

  seg_begins = std::move(rmm::device_uvector<INDEX_T>(n_polygons + 1, stream));
  auto* p_seg_begins = seg_begins.data();
  seg_begins.set_element_to_zero_async(0, stream);

  thrust::inclusive_scan(rmm::exec_policy_nosync(stream), n_segs.begin(), n_segs.end(),
                         seg_begins.begin() + 1);

  uint32_t num_aabbs = seg_begins.back_element(stream);

  aabb_poly_ids = std::move(rmm::device_uvector<INDEX_T>(num_aabbs, stream));
  aabb_ring_ids = std::move(rmm::device_uvector<INDEX_T>(num_aabbs, stream));

  auto* p_poly_ids = aabb_poly_ids.data();
  auto* p_ring_ids = aabb_ring_ids.data();

  rmm::device_uvector<OptixAabb> aabbs(num_aabbs, stream);
  auto* p_aabbs = aabbs.data();

  LaunchKernel(stream.value(), [=] __device__() {
    auto lane = threadIdx.x % 32;
    auto global_warp_id = TID_1D / 32;
    auto n_warps = TOTAL_THREADS_1D / 32;

    // each warp takes a polygon
    // i is the renumbered polygon id starting from 0
    for (auto i = global_warp_id; i < n_polygons; i += n_warps) {
      auto poly_id = polygon_ids[i];
      const auto& polygon = polygons[poly_id];
      auto tail = p_seg_begins[i];

      // entire warp sequentially visit each ring
      for (uint32_t ring_idx = 0; ring_idx < polygon.num_rings(); ring_idx++) {
        auto ring = polygon.get_ring(ring_idx);
        // this is like a hash function, its okay to overflow
        OptixAabb aabb;
        aabb.minZ = aabb.maxZ = i;

        // each lane takes a seg
        for (auto seg_idx = lane; seg_idx < ring.num_segments(); seg_idx += 32) {
          const auto& seg = ring.get_line_segment(seg_idx);
          const auto& p1 = seg.get_p1();
          const auto& p2 = seg.get_p2();

          aabb.minX = std::min(p1.x(), p2.x());
          aabb.maxX = std::max(p1.x(), p2.x());
          aabb.minY = std::min(p1.y(), p2.y());
          aabb.maxY = std::max(p1.y(), p2.y());

          if (std::is_same_v<scalar_t, double>) {
            aabb.minX = next_float_from_double(aabb.minX, -1, 2);
            aabb.maxX = next_float_from_double(aabb.maxX, 1, 2);
            aabb.minY = next_float_from_double(aabb.minY, -1, 2);
            aabb.maxY = next_float_from_double(aabb.maxY, 1, 2);
          }
          p_aabbs[tail + seg_idx] = aabb;
          p_poly_ids[tail + seg_idx] = poly_id;
          p_ring_ids[tail + seg_idx] = ring_idx;
        }
        tail += ring.num_segments();
        // fill a dummy AABB, so we have aabb-vertex one-to-one relationship
        if (lane == 0) {
          p_aabbs[tail] = OptixAabb{0, 0, 0, 0, 0, 0};
        }
        tail++;
      }
      assert(p_seg_begins[i + 1] == tail);
    }
  });
  assert(rt_engine_ != nullptr);
  return rt_engine_->BuildAccelCustom(stream.value(), ArrayView<OptixAabb>(aabbs), buffer,
                                      false /*fast build*/, true /*compact*/);
}

template <typename POINT_T, typename INDEX_T>
OptixTraversableHandle RelateEngine<POINT_T, INDEX_T>::BuildBVH(
    const rmm::cuda_stream_view& stream,
    const MultiPolygonArrayView<POINT_T, INDEX_T>& multi_polys,
    ArrayView<uint32_t> multi_poly_ids, rmm::device_uvector<INDEX_T>& seg_begins,
    rmm::device_buffer& buffer, rmm::device_uvector<INDEX_T>& aabb_multi_poly_ids,
    rmm::device_uvector<INDEX_T>& aabb_part_ids,
    rmm::device_uvector<INDEX_T>& aabb_ring_ids) {
  auto n_mult_polygons = multi_poly_ids.size();
  rmm::device_uvector<uint32_t> n_segs(n_mult_polygons, stream);
  auto* p_nsegs = n_segs.data();
  Stopwatch sw;
  double count_time, fill_time, build_time;

  sw.start();
  LaunchKernel(stream, [=] __device__() {
    using WarpReduce = cub::WarpReduce<uint32_t>;
    __shared__ WarpReduce::TempStorage temp_storage[MAX_BLOCK_SIZE / 32];
    auto lane = threadIdx.x % 32;
    auto warp_id = threadIdx.x / 32;
    auto global_warp_id = TID_1D / 32;
    auto n_warps = TOTAL_THREADS_1D / 32;

    for (auto i = global_warp_id; i < n_mult_polygons; i += n_warps) {
      auto id = multi_poly_ids[i];
      const auto& multi_polygon = multi_polys[id];
      uint32_t total_segs = 0;

      for (int part_idx = 0; part_idx < multi_polygon.num_polygons(); part_idx++) {
        auto polygon = multi_polygon.get_polygon(part_idx);
        for (auto ring = lane; ring < polygon.num_rings(); ring += 32) {
          total_segs += polygon.get_ring(ring).num_points();
        }
      }
      total_segs = WarpReduce(temp_storage[warp_id]).Sum(total_segs);
      if (lane == 0) {
        p_nsegs[i] = total_segs;
      }
    }
  });
  stream.synchronize();
  sw.stop();
  count_time = sw.ms();

  seg_begins = std::move(rmm::device_uvector<INDEX_T>(n_mult_polygons + 1, stream));
  auto* p_seg_begins = seg_begins.data();
  seg_begins.set_element_to_zero_async(0, stream);

  thrust::inclusive_scan(rmm::exec_policy_nosync(stream), n_segs.begin(), n_segs.end(),
                         seg_begins.begin() + 1);

  // each line seg is corresponding to an AABB and each ring includes an empty AABB
  uint32_t num_aabbs = seg_begins.back_element(stream);

  printf("num aabbs %u\n", num_aabbs);

  aabb_multi_poly_ids = std::move(rmm::device_uvector<INDEX_T>(num_aabbs, stream));
  aabb_part_ids = std::move(rmm::device_uvector<uint32_t>(num_aabbs, stream));
  aabb_ring_ids = std::move(rmm::device_uvector<uint32_t>(num_aabbs, stream));

  auto* p_multi_poly_ids = aabb_multi_poly_ids.data();
  auto* p_part_ids = aabb_part_ids.data();
  auto* p_ring_ids = aabb_ring_ids.data();

  rmm::device_uvector<OptixAabb> aabbs(num_aabbs, stream);
  auto* p_aabbs = aabbs.data();

  sw.start();
  LaunchKernel(stream.value(), [=] __device__() {
    auto lane = threadIdx.x % 32;
    auto global_warp_id = TID_1D / 32;
    auto n_warps = TOTAL_THREADS_1D / 32;

    // each warp takes a multi polygon
    // i is the renumbered polygon id starting from 0
    for (auto i = global_warp_id; i < n_mult_polygons; i += n_warps) {
      auto multi_poly_id = multi_poly_ids[i];
      const auto& multi_polygon = multi_polys[multi_poly_id];
      auto tail = p_seg_begins[i];

      // entire warp sequentially visit each part
      for (uint32_t part_idx = 0; part_idx < multi_polygon.num_polygons(); part_idx++) {
        auto polygon = multi_polygon.get_polygon(part_idx);

        // entire warp sequentially visit each ring
        for (uint32_t ring_idx = 0; ring_idx < polygon.num_rings(); ring_idx++) {
          auto ring = polygon.get_ring(ring_idx);
          // this is like a hash function, its okay to overflow
          OptixAabb aabb;
          aabb.minZ = aabb.maxZ = i;

          // each lane takes a seg
          for (auto seg_idx = lane; seg_idx < ring.num_segments(); seg_idx += 32) {
            const auto& seg = ring.get_line_segment(seg_idx);
            const auto& p1 = seg.get_p1();
            const auto& p2 = seg.get_p2();

            aabb.minX = std::min(p1.x(), p2.x());
            aabb.maxX = std::max(p1.x(), p2.x());
            aabb.minY = std::min(p1.y(), p2.y());
            aabb.maxY = std::max(p1.y(), p2.y());

            if (std::is_same_v<scalar_t, double>) {
              aabb.minX = next_float_from_double(aabb.minX, -1, 2);
              aabb.maxX = next_float_from_double(aabb.maxX, 1, 2);
              aabb.minY = next_float_from_double(aabb.minY, -1, 2);
              aabb.maxY = next_float_from_double(aabb.maxY, 1, 2);
            }
            p_aabbs[tail + seg_idx] = aabb;
            p_multi_poly_ids[tail + seg_idx] = multi_poly_id;
            p_part_ids[tail + seg_idx] = part_idx;
            p_ring_ids[tail + seg_idx] = ring_idx;
          }
          tail += ring.num_segments();
          // fill a dummy AABB, so we have aabb-vertex one-to-one relationship
          if (lane == 0) {
            p_aabbs[tail] = OptixAabb{0, 0, 0, 0, 0, 0};
          }
          tail++;
        }
      }
      assert(p_seg_begins[i + 1] == tail);
    }
  });
  stream.synchronize();
  sw.stop();
  fill_time = sw.ms();

  assert(rt_engine_ != nullptr);
  sw.start();
  auto handle =
      rt_engine_->BuildAccelCustom(stream.value(), ArrayView<OptixAabb>(aabbs), buffer,
                                   false /*fast build*/, true /*compact*/);
  stream.synchronize();
  sw.stop();
  build_time = sw.ms();
  printf("count line segs %lf ms, fill AABBs %lf ms, build BVH %lf ms\n", count_time,
         fill_time, build_time);
  return handle;
}
// Explicitly instantiate the template for specific types
template class RelateEngine<Point<double, 2>, uint32_t>;
}  // namespace gpuspatial
