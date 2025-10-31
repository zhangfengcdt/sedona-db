#ifndef GPUSPATIAL_RELATE_RELATE_ENGINE_CUH
#define GPUSPATIAL_RELATE_RELATE_ENGINE_CUH
#include <rmm/cuda_stream_view.hpp>
#include "gpuspatial/index/detail/rt_engine.hpp"
#include "gpuspatial/loader/device_geometries.cuh"
#include "gpuspatial/relate/predicate.cuh"
#include "gpuspatial/utils/queue.h"

namespace gpuspatial {

template <typename POINT_T, typename INDEX_T>
class RelateEngine {
  using scalar_t = typename POINT_T::scalar_t;

 public:
  RelateEngine() = default;

  RelateEngine(const DeviceGeometries<POINT_T, INDEX_T>* geoms1);

  RelateEngine(const DeviceGeometries<POINT_T, INDEX_T>* geoms1,
               const details::RTEngine* rt_engine);

  void Evaluate(const rmm::cuda_stream_view& stream,
                const DeviceGeometries<POINT_T, INDEX_T>& geoms2, Predicate predicate,
                Queue<thrust::pair<uint32_t, uint32_t>>& ids);

  template <typename GEOM2_ARRAY_VIEW_T>
  void Evaluate(const rmm::cuda_stream_view& stream,
                const GEOM2_ARRAY_VIEW_T& geom_array2, Predicate predicate,
                Queue<thrust::pair<uint32_t, uint32_t>>& ids);

  template <typename GEOM1_ARRAY_VIEW_T, typename GEOM2_ARRAY_VIEW_T>
  void Evaluate(const rmm::cuda_stream_view& stream,
                const GEOM1_ARRAY_VIEW_T& geom_array1,
                const GEOM2_ARRAY_VIEW_T& geom_array2, Predicate predicate,
                Queue<thrust::pair<uint32_t, uint32_t>>& ids);

  void Evaluate(const rmm::cuda_stream_view& stream,
                const PolygonArrayView<POINT_T, INDEX_T>& geom_array1,
                const PointArrayView<POINT_T, INDEX_T>& geom_array2, Predicate predicate,
                Queue<thrust::pair<uint32_t, uint32_t>>& ids);

  void Evaluate(const rmm::cuda_stream_view& stream,
                const MultiPolygonArrayView<POINT_T, INDEX_T>& geom_array1,
                const PointArrayView<POINT_T, INDEX_T>& geom_array2, Predicate predicate,
                Queue<thrust::pair<uint32_t, uint32_t>>& ids);

  void Evaluate(const rmm::cuda_stream_view& stream,
                const PointArrayView<POINT_T, INDEX_T>& geom_array1,
                const PolygonArrayView<POINT_T, INDEX_T>& geom_array2,
                Predicate predicate, Queue<thrust::pair<uint32_t, uint32_t>>& ids);

  void Evaluate(const rmm::cuda_stream_view& stream,
                const PointArrayView<POINT_T, INDEX_T>& geom_array1,
                const MultiPolygonArrayView<POINT_T, INDEX_T>& geom_array2,
                Predicate predicate, Queue<thrust::pair<uint32_t, uint32_t>>& ids);

  template <typename GEOM1_ARRAY_VIEW_T, typename GEOM2_ARRAY_VIEW_T>
  void EvaluatePointPolygonLB(const rmm::cuda_stream_view& stream,
                              const GEOM1_ARRAY_VIEW_T& geom_array1,
                              const GEOM2_ARRAY_VIEW_T& geom_array2, Predicate predicate,
                              Queue<thrust::pair<uint32_t, uint32_t>>& ids);

  template <typename GEOM1_ARRAY_VIEW_T, typename GEOM2_ARRAY_VIEW_T>
  void EvaluatePolygonPointLB(const rmm::cuda_stream_view& stream,
                              const GEOM1_ARRAY_VIEW_T& geom_array1,
                              const GEOM2_ARRAY_VIEW_T& geom_array2, Predicate predicate,
                              Queue<thrust::pair<uint32_t, uint32_t>>& ids);

  /**
   * Build BVH for a subset of polygons
   * @param stream
   * @param polygons
   * @param polygon_ids
   * @param buffer
   */
  OptixTraversableHandle BuildBVH(const rmm::cuda_stream_view& stream,
                                  const PolygonArrayView<POINT_T, INDEX_T>& polygons,
                                  ArrayView<uint32_t> polygon_ids,
                                  rmm::device_uvector<INDEX_T>& seg_begins,
                                  rmm::device_uvector<INDEX_T>& seg_polygon_ids,
                                  rmm::device_buffer& buffer);

  OptixTraversableHandle BuildBVH(
      const rmm::cuda_stream_view& stream,
      const MultiPolygonArrayView<POINT_T, INDEX_T>& multi_polygons,
      ArrayView<uint32_t> multi_polygon_ids, rmm::device_uvector<INDEX_T>& seg_begins,
      rmm::device_buffer& buffer, rmm::device_uvector<INDEX_T>& geom_ids,
      rmm::device_uvector<INDEX_T>& part_ids, rmm::device_uvector<INDEX_T>& ring_ids);

 private:
  const DeviceGeometries<POINT_T, INDEX_T>* geoms1_;
  const details::RTEngine* rt_engine_;
};
}  // namespace gpuspatial
#endif  // GPUSPATIAL_RELATE_RELATE_ENGINE_CUH
