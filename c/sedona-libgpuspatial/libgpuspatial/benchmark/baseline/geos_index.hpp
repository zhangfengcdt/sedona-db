#ifndef GPUSPATIAL_BENCHMARK_BASELINE_GEOSINDEX_HPP
#define GPUSPATIAL_BENCHMARK_BASELINE_GEOSINDEX_HPP

#include <thread>
#include "../../include/gpuspatial/index/streaming_joiner.hpp"
#include "geoarrow/geoarrow.h"
#include "geoarrow_geos/geoarrow_geos.hpp"
#include "geos_c.h"
#include "nanoarrow/nanoarrow.h"

namespace gpuspatial {
class GEOSIndex : public StreamingJoiner {
 public:
  struct GEOSContext : Context {
    GEOSContextHandle_t handle;
    std::vector<const GEOSPreparedGeometry*> prepared_geometries;
    std::unique_ptr<geoarrow::geos::GeometryVector> geom_vec;
    std::unique_ptr<geoarrow::geos::ArrayReader> reader;
    GEOSContext() { handle = GEOS_init_r(); }
    ~GEOSContext() {
      for (auto& prepared_geom : prepared_geometries) {
        if (prepared_geom != nullptr) {
          GEOSPreparedGeom_destroy_r(handle, prepared_geom);
        }
      }
      GEOS_finish_r(handle);
    }
  };

  GEOSIndex();

  ~GEOSIndex();

  void Init(const Config* config) override;

  void Clear() override;

  void PushBuild(const ArrowSchema* schema, const ArrowArray* array, int64_t offset,
                 int64_t length) override;

  void FinishBuilding() override;

  std::shared_ptr<Context> CreateContext() override {
    return std::make_shared<GEOSContext>();
  }

  void PushStream(Context* base_ctx, const ArrowSchema* schema, const ArrowArray* array,
                  int64_t offset, int64_t length, Predicate predicate,
                  std::vector<uint32_t>* build_indices,
                  std::vector<uint32_t>* array_indices,
                  int32_t array_index_offset) override;

 private:
  struct BuildData {
    GEOSGeometry* geom;
    size_t build_index;
  };

  struct Payload {
    GEOSContextHandle_t handle;
    const GEOSGeometry* geom;
    const BuildData* p_build_data;
    GEOSContext* ctx;
    int32_t stream_index;
    std::vector<uint32_t>* build_indices;
    std::vector<uint32_t>* stream_indices;
  };

  GEOSContextHandle_t handle_;
  ArrowSchema* build_schema_ ;
  std::unique_ptr<geoarrow::geos::ArrayReader> build_reader_;
  std::vector<BuildData> build_data_;
  std::unique_ptr<geoarrow::geos::GeometryVector> geoms_vec_;
  GEOSSTRtree* tree_ = nullptr;
};
}  // namespace gpuspatial
#endif  //  GPUSPATIAL_BENCHMARK_BASELINE_GEOSINDEX_HPP