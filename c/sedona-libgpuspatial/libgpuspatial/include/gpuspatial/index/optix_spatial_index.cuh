#ifndef GPUSPATIAL_INDEX_OPTIX_SPATIAL_INDEX_CUH
#define GPUSPATIAL_INDEX_OPTIX_SPATIAL_INDEX_CUH
#include <thread>
#include "gpuspatial/geom/box.cuh"
#include "gpuspatial/geom/point.cuh"
#include "gpuspatial/index/detail/rt_engine.hpp"
#include "gpuspatial/index/geometry_grouper.hpp"
#include "gpuspatial/index/object_pool.hpp"
#include "gpuspatial/index/spatial_index.hpp"
#include "gpuspatial/loader/device_geometries.cuh"
#include "gpuspatial/loader/geometry_segment.h"
#include "gpuspatial/utils/gpu_timer.hpp"
#include "gpuspatial/utils/queue.h"

#include "rmm/cuda_stream_pool.hpp"
#include "rmm/cuda_stream_view.hpp"
#include "rmm/device_uvector.hpp"
// #define GPUSPATIAL_PROFILING
namespace gpuspatial {

template <typename POINT_T, typename INDEX_T>
class OptixSpatialIndex : public SpatialIndex<POINT_T, INDEX_T> {
  using scalar_t = typename POINT_T::scalar_t;
  static constexpr int n_dim = POINT_T::n_dim;
  using index_t = INDEX_T;
  using point_t = Point<scalar_t, n_dim>;
  using dev_geometries_t = DeviceGeometries<point_t, index_t>;
  using box_t = Box<point_t>;
  static_assert(n_dim == 2, "OptixSpatialIndex only supports 2D geometries for now");

 public:
  struct SpatialIndexConfig : SpatialIndex<POINT_T, INDEX_T>::Config {
    const char* ptx_root;
    bool prefer_fast_build = false;
    bool compact = true;
    uint32_t concurrency = 1;
    uint32_t n_geoms_per_aabb = 1;
    float result_buffer_memory_reserve_ratio =
        0.9;  // reserve a ratio of available memory for result sets
    size_t stack_size_bytes = 3 * 1024;  // this value determines RELATE_MAX_DEPTH
    SpatialIndexConfig() : ptx_root(nullptr), prefer_fast_build(false), compact(false) {
      concurrency = std::thread::hardware_concurrency();
    }
  };

  struct SpatialIndexContext : SpatialIndex<POINT_T, INDEX_T>::Context {
    rmm::cuda_stream_view cuda_stream;
    std::string shader_id;

    std::shared_ptr<rmm::device_uvector<point_t>> stream_points;
    std::shared_ptr<rmm::device_uvector<box_t>> stream_boxes;

    std::unique_ptr<rmm::device_uvector<char>> bvh_buffer;
    OptixTraversableHandle handle;
    std::vector<char> h_launch_params_buffer;
    std::unique_ptr<rmm::device_buffer> launch_params_buffer;
    // output
    Queue<thrust::pair<uint32_t, uint32_t>> results;
#ifdef GPUSPATIAL_PROFILING
    GPUTimer timer;
    // counters
    double parse_ms = 0.0;
    double alloc_ms = 0.0;
    double filter_ms = 0.0;
    double refine_ms = 0.0;
    double copy_res_ms = 0.0;
#endif
  };

  OptixSpatialIndex() = default;

  ~OptixSpatialIndex() = default;

  void Init(const typename SpatialIndex<POINT_T, INDEX_T>::Config* config) override;

  void Clear() override;

  void PushBuild(const box_t* boxes, uint32_t n_boxes) override;

  void FinishBuilding() override;

  std::shared_ptr<typename SpatialIndex<POINT_T, INDEX_T>::Context> CreateContext()
      override {
    return ctx_pool_->take();
  }

  void PushStream(typename SpatialIndex<POINT_T, INDEX_T>::Context* context,
                  const point_t* points, uint32_t n_points,
                  std::vector<uint32_t>* build_indices,
                  std::vector<uint32_t>* stream_indices) override;

  void PushStream(typename SpatialIndex<POINT_T, INDEX_T>::Context* context,
                  const box_t* boxes, uint32_t n_boxes,
                  std::vector<uint32_t>* build_indices,
                  std::vector<uint32_t>* stream_indices) override;

  // Internal method but has to be public for the CUDA kernel to access

  void filter(SpatialIndexContext* ctx, uint32_t dim_x,
              std::vector<uint32_t>* build_indices,
              std::vector<uint32_t>* stream_indices);

 private:
  SpatialIndexConfig config_;
  std::unique_ptr<rmm::cuda_stream_pool> stream_pool_;
  details::RTEngine rt_engine_;
  std::unique_ptr<rmm::device_uvector<char>> bvh_buffer_;

  std::shared_ptr<BoxSegment<point_t, index_t>> box_seg_;
  std::shared_ptr<DeviceGeometries<point_t, index_t>> build_geometries_;
  GeometryGrouper<point_t, index_t> geometry_grouper;
  OptixTraversableHandle handle_;

  std::shared_ptr<ObjectPool<SpatialIndexContext>> ctx_pool_;

  OptixTraversableHandle buildBVH(const rmm::cuda_stream_view& stream,
                                  const ArrayView<OptixAabb>& aabbs,
                                  std::unique_ptr<rmm::device_uvector<char>>& buffer);

  void allocateResultBuffer(SpatialIndexContext* ctx);

  void prepareLaunchParamsPointQuery(SpatialIndexContext* ctx);

  void prepareLaunchParamsBoxQuery(SpatialIndexContext* ctx, bool forward);
};

}  // namespace gpuspatial
#endif  // GPUSPATIAL_INDEX_OPTIX_SPATIAL_INDEX_CUH
