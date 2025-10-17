#include "gpuspatial/index/geometry_grouper.hpp"
#include "gpuspatial/index/relate_engine.cuh"
#include "gpuspatial/loader/device_geometries.cuh"
#include "gpuspatial/relate/predicate.cuh"
#include "gpuspatial/relate/relate.cuh"
#include "gpuspatial/relate/relate_block.cuh"
#include "gpuspatial/relate/relate_warp.cuh"
#include "gpuspatial/utils/array_view.h"
#include "gpuspatial/utils/launcher.h"
#include "gpuspatial/utils/queue.h"

#include <thrust/partition.h>
#include <thrust/remove.h>
#include <rmm/cuda_stream_view.hpp>
#include <rmm/exec_policy.hpp>

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
      // Evaluate(stream, geoms2.template GetGeometryArrayView<geom2_array_view_t>(),
      // predicate, ids);
      break;
    }
    case GeometryType::kLineString: {
      using geom2_array_view_t = LineStringArrayView<POINT_T, INDEX_T>;
      // Evaluate(stream, geoms2.template GetGeometryArrayView<geom2_array_view_t>(),
      //          predicate, ids);
      break;
    }
    case GeometryType::kMultiLineString: {
      using geom2_array_view_t = MultiLineStringArrayView<POINT_T, INDEX_T>;
      // Evaluate(stream, geoms2.template GetGeometryArrayView<geom2_array_view_t>(),
      //          predicate, ids);
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
      // Evaluate(stream, geoms2.template GetGeometryArrayView<geom2_array_view_t>(),
      // predicate, ids);
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
      // Evaluate(stream, geoms1_->template GetGeometryArrayView<geom1_array_view_t>(),
      //          geom_array2, predicate, ids);
      break;
    }
    case GeometryType::kLineString: {
      using geom1_array_view_t = LineStringArrayView<POINT_T, INDEX_T>;
      // Evaluate(stream, geoms1_->template GetGeometryArrayView<geom1_array_view_t>(),
      //          geom_array2, predicate, ids);
      break;
    }
    case GeometryType::kMultiLineString: {
      using geom1_array_view_t = MultiLineStringArrayView<POINT_T, INDEX_T>;
      // Evaluate(stream, geoms1_->template GetGeometryArrayView<geom1_array_view_t>(),
      //          geom_array2, predicate, ids);
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
#if 0
    std::vector<thrust::pair<uint32_t, uint32_t>> h_ids(ids_size);

    CUDA_CHECK(cudaMemcpyAsync(h_ids.data(), ids.data(),
                               ids_size * sizeof(thrust::pair<uint32_t, uint32_t>),
                               cudaMemcpyDeviceToHost, stream));
    stream.synchronize();
    auto h_prefix_sum_polygons1 = ToVector(stream, geom_array1.prefix_sum_polygons_);
    auto h_prefix_sum_rings1 = ToVector(stream, geom_array1.prefix_sum_rings_);
    auto h_vertices1 = ToVector(stream, geom_array1.vertices_);
    auto h_mbrs1 = ToVector(stream, geom_array1.mbrs_);

    auto view1 = GEOM1_ARRAY_VIEW_T(
        ArrayView<INDEX_T>(h_prefix_sum_polygons1.data(), h_prefix_sum_polygons1.size()),
        ArrayView<INDEX_T>(h_prefix_sum_rings1.data(), h_prefix_sum_rings1.size()),
        ArrayView<POINT_T>(h_vertices1.data(), h_vertices1.size()),
        ArrayView<Box<POINT_T>>(h_mbrs1.data(), h_mbrs1.size()));

    auto h_prefix_sum_polygons2 = ToVector(stream, geom_array2.prefix_sum_polygons_);
    auto h_prefix_sum_rings2 = ToVector(stream, geom_array2.prefix_sum_rings_);
    auto h_vertices2 = ToVector(stream, geom_array2.vertices_);
    auto h_mbrs2 = ToVector(stream, geom_array2.mbrs_);

    auto view2 = GEOM1_ARRAY_VIEW_T(
        ArrayView<INDEX_T>(h_prefix_sum_polygons2.data(), h_prefix_sum_polygons2.size()),
        ArrayView<INDEX_T>(h_prefix_sum_rings2.data(), h_prefix_sum_rings2.size()),
        ArrayView<POINT_T>(h_vertices2.data(), h_vertices2.size()),
        ArrayView<Box<POINT_T>>(h_mbrs2.data(), h_mbrs2.size()));

    // for (auto& pair : h_ids) {
    //   auto geom1_id = pair.first;
    //   auto geom2_id = pair.second;
    //   const auto& geom1 = view1[geom1_id];
    //   const auto& geom2 = view2[geom2_id];
    //
    //   auto IM = relate(geom1, geom2);
    //   printf("geom1 id: %u, geom2 id: %u, IM: %d\n", geom1_id, geom2_id, IM);
    // }
    // const auto& geom1 = view1[geom1_id];
    // const auto& geom2 = view2[geom2_id];
    // auto IM = relate(geom1, geom2);
    // printf("geom1 id: %u, geom2 id: %u, IM: %d\n", geom1_id, geom2_id, IM);
#endif
}

template <typename POINT_T, typename INDEX_T>
void RelateEngine<POINT_T, INDEX_T>::Evaluate(
    const rmm::cuda_stream_view& stream,
    const PolygonArrayView<POINT_T, INDEX_T>& geom_array1,
    const PointArrayView<POINT_T, INDEX_T>& geom_array2, Predicate predicate,
    Queue<thrust::pair<uint32_t, uint32_t>>& ids) {
  EvaluatePolygonPointLB(stream, geom_array1, geom_array2, predicate, ids);
}

template <typename POINT_T, typename INDEX_T>
void RelateEngine<POINT_T, INDEX_T>::Evaluate(
    const rmm::cuda_stream_view& stream,
    const MultiPolygonArrayView<POINT_T, INDEX_T>& geom_array1,
    const PointArrayView<POINT_T, INDEX_T>& geom_array2, Predicate predicate,
    Queue<thrust::pair<uint32_t, uint32_t>>& ids) {
  EvaluatePolygonPointLB(stream, geom_array1, geom_array2, predicate, ids);
}

template <typename POINT_T, typename INDEX_T>
void RelateEngine<POINT_T, INDEX_T>::Evaluate(
    const rmm::cuda_stream_view& stream,
    const PointArrayView<POINT_T, INDEX_T>& geom_array1,
    const PolygonArrayView<POINT_T, INDEX_T>& geom_array2, Predicate predicate,
    Queue<thrust::pair<uint32_t, uint32_t>>& ids) {
  EvaluatePointPolygonLB(stream, geom_array1, geom_array2, predicate, ids);
}

template <typename POINT_T, typename INDEX_T>
void RelateEngine<POINT_T, INDEX_T>::Evaluate(
    const rmm::cuda_stream_view& stream,
    const PointArrayView<POINT_T, INDEX_T>& geom_array1,
    const MultiPolygonArrayView<POINT_T, INDEX_T>& geom_array2, Predicate predicate,
    Queue<thrust::pair<uint32_t, uint32_t>>& ids) {
  EvaluatePointPolygonLB(stream, geom_array1, geom_array2, predicate, ids);
}

template <typename POINT_T, typename INDEX_T>
template <typename GEOM1_ARRAY_VIEW_T, typename GEOM2_ARRAY_VIEW_T>
void RelateEngine<POINT_T, INDEX_T>::EvaluatePointPolygonLB(
    const rmm::cuda_stream_view& stream, const GEOM1_ARRAY_VIEW_T& geom_array1,
    const GEOM2_ARRAY_VIEW_T& geom_array2, Predicate predicate,
    Queue<thrust::pair<uint32_t, uint32_t>>& ids) {
  auto ids_size = ids.size(stream);
  auto invalid_pair = thrust::make_pair(std::numeric_limits<uint32_t>::max(),
                                        std::numeric_limits<uint32_t>::max());

  // serial [serial end], use warp [warp end], use block
  auto serial_end = thrust::partition(
      rmm::exec_policy_nosync(stream), ids.data(), ids.data() + ids_size,
      [=] __device__(const thrust::pair<uint32_t, uint32_t>& pair) {
        auto geom2_id = pair.second;
        auto nv = geom_array2[geom2_id].num_vertices();
        return nv < WARP_SIZE;
      });

  auto warp_end = thrust::partition(
      rmm::exec_policy_nosync(stream), serial_end, ids.data() + ids_size,
      [=] __device__(const thrust::pair<uint32_t, uint32_t>& pair) {
        auto geom2_id = pair.second;
        auto nv = geom_array2[geom2_id].num_vertices();
        return nv < MAX_BLOCK_SIZE;
      });

  auto n_serial = thrust::distance(ids.data(), serial_end);
  auto n_use_warp = thrust::distance(serial_end, warp_end);
  auto n_use_block = thrust::distance(warp_end, ids.data() + ids_size);

  LaunchKernel(stream, [=] __device__() mutable {
    using BlockReduce = cub::BlockReduce<int, MAX_BLOCK_SIZE>;
    __shared__ BlockReduce::TempStorage temp_storage;

    // each warp takes an AABB
    for (auto i = blockIdx.x; i < n_use_block; i += gridDim.x) {
      auto& pair = warp_end[i];
      auto geom1_id = pair.first;
      auto geom2_id = pair.second;
      const auto& geom1 = geom_array1[geom1_id];
      const auto& geom2 = geom_array2[geom2_id];

      auto IM = relate(geom1, geom2, &temp_storage);

      // overwrite the entry that does not match the predicate
      if (threadIdx.x == 0) {
        if (!detail::EvaluatePredicate(predicate, IM)) pair = invalid_pair;
      }
    }
  });

  LaunchKernel(stream, [=] __device__() mutable {
    auto lane_id = threadIdx.x % WARP_SIZE;
    auto warp_id = threadIdx.x / WARP_SIZE;
    auto global_warp_id = TID_1D / WARP_SIZE;
    auto global_n_warps = TOTAL_THREADS_1D / WARP_SIZE;
    using WarpReduce = cub::WarpReduce<int>;
    __shared__ WarpReduce::TempStorage temp_storage[MAX_BLOCK_SIZE / WARP_SIZE];

    // each warp takes an AABB
    for (auto i = global_warp_id; i < n_use_warp; i += global_n_warps) {
      auto& pair = serial_end[i];
      auto geom1_id = pair.first;
      auto geom2_id = pair.second;
      const auto& geom1 = geom_array1[geom1_id];
      const auto& geom2 = geom_array2[geom2_id];

      auto IM = relate(geom1, geom2, &temp_storage[warp_id]);

      // overwrite the entry that does not match the predicate
      if (lane_id == 0) {
        if (!detail::EvaluatePredicate(predicate, IM)) pair = invalid_pair;
      }
    }
  });

  thrust::transform(rmm::exec_policy_nosync(stream), ids.data(), ids.data() + n_serial,
                    ids.data(),
                    [=] __device__(const thrust::pair<uint32_t, uint32_t>& pair) {
                      auto geom1_id = pair.first;
                      auto geom2_id = pair.second;
                      const auto& geom1 = geom_array1[geom1_id];
                      const auto& geom2 = geom_array2[geom2_id];

                      auto IM = relate(geom1, geom2);
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
template <typename GEOM1_ARRAY_VIEW_T, typename GEOM2_ARRAY_VIEW_T>
void RelateEngine<POINT_T, INDEX_T>::EvaluatePolygonPointLB(
    const rmm::cuda_stream_view& stream, const GEOM1_ARRAY_VIEW_T& geom_array1,
    const GEOM2_ARRAY_VIEW_T& geom_array2, Predicate predicate,
    Queue<thrust::pair<uint32_t, uint32_t>>& ids) {
  auto ids_size = ids.size(stream);
  auto invalid_pair = thrust::make_pair(std::numeric_limits<uint32_t>::max(),
                                        std::numeric_limits<uint32_t>::max());

  // serial [serial end], use warp [warp end], use block
  auto serial_end = thrust::partition(
      rmm::exec_policy_nosync(stream), ids.data(), ids.data() + ids_size,
      [=] __device__(const thrust::pair<uint32_t, uint32_t>& pair) {
        auto geom1_id = pair.first;
        auto nv = geom_array1[geom1_id].num_vertices();
        return nv < WARP_SIZE;
      });

  auto warp_end = thrust::partition(
      rmm::exec_policy_nosync(stream), serial_end, ids.data() + ids_size,
      [=] __device__(const thrust::pair<uint32_t, uint32_t>& pair) {
        auto geom1_id = pair.first;
        auto nv = geom_array1[geom1_id].num_vertices();
        return nv < MAX_BLOCK_SIZE;
      });

  auto n_serial = thrust::distance(ids.data(), serial_end);
  auto n_use_warp = thrust::distance(serial_end, warp_end);
  auto n_use_block = thrust::distance(warp_end, ids.data() + ids_size);

  LaunchKernel(stream, [=] __device__() mutable {
    using BlockReduce = cub::BlockReduce<int, MAX_BLOCK_SIZE>;
    __shared__ BlockReduce::TempStorage temp_storage;

    // each warp takes an AABB
    for (auto i = blockIdx.x; i < n_use_block; i += gridDim.x) {
      auto& pair = warp_end[i];
      auto geom1_id = pair.first;
      auto geom2_id = pair.second;
      const auto& geom1 = geom_array1[geom1_id];
      const auto& geom2 = geom_array2[geom2_id];

      auto IM = relate(geom1, geom2, &temp_storage);

      // overwrite the entry that does not match the predicate
      if (threadIdx.x == 0) {
        if (!detail::EvaluatePredicate(predicate, IM)) pair = invalid_pair;
      }
    }
  });

  LaunchKernel(stream, [=] __device__() mutable {
    auto lane_id = threadIdx.x % WARP_SIZE;
    auto warp_id = threadIdx.x / WARP_SIZE;
    auto global_warp_id = TID_1D / WARP_SIZE;
    auto global_n_warps = TOTAL_THREADS_1D / WARP_SIZE;
    using WarpReduce = cub::WarpReduce<int>;
    __shared__ WarpReduce::TempStorage temp_storage[MAX_BLOCK_SIZE / WARP_SIZE];

    // each warp takes an AABB
    for (auto i = global_warp_id; i < n_use_warp; i += global_n_warps) {
      auto& pair = serial_end[i];
      auto geom1_id = pair.first;
      auto geom2_id = pair.second;
      const auto& geom1 = geom_array1[geom1_id];
      const auto& geom2 = geom_array2[geom2_id];

      auto IM = relate(geom1, geom2, &temp_storage[warp_id]);

      // overwrite the entry that does not match the predicate
      if (lane_id == 0) {
        if (!detail::EvaluatePredicate(predicate, IM)) pair = invalid_pair;
      }
    }
  });

  thrust::transform(rmm::exec_policy_nosync(stream), ids.data(), ids.data() + n_serial,
                    ids.data(),
                    [=] __device__(const thrust::pair<uint32_t, uint32_t>& pair) {
                      auto geom1_id = pair.first;
                      auto geom2_id = pair.second;
                      const auto& geom1 = geom_array1[geom1_id];
                      const auto& geom2 = geom_array2[geom2_id];

                      auto IM = relate(geom1, geom2);
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

// Explicitly instantiate the template for specific types
template class RelateEngine<Point<double, 2>, uint32_t>;
}  // namespace gpuspatial
