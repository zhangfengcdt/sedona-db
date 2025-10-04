#ifndef GPUSPATIAL_RELATE_RELATE_ENGINE_CUH
#define GPUSPATIAL_RELATE_RELATE_ENGINE_CUH
#include "gpuspatial/loader/device_geometries.cuh"
#include "gpuspatial/relate/predicate.cuh"
#include "gpuspatial/utils/queue.h"
#include "rmm/cuda_stream_view.hpp"

namespace gpuspatial {

template <typename POINT_T, typename INDEX_T>
class RelateEngine {
 public:
  RelateEngine() = default;

  RelateEngine(const DeviceGeometries<POINT_T, INDEX_T>* geoms1);

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

 private:
  const DeviceGeometries<POINT_T, INDEX_T>* geoms1_;
};
}  // namespace gpuspatial
#endif  // GPUSPATIAL_RELATE_RELATE_ENGINE_CUH
