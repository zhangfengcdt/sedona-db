#include "geos_index.hpp"

namespace gpuspatial {

GEOSIndex::GEOSIndex() {
  handle_ = GEOS_init_r();
  geoms_vec_ = std::make_unique<geoarrow::geos::GeometryVector>(handle_);
  tree_ = nullptr;
}

GEOSIndex::~GEOSIndex() {
  GEOSIndex::Clear();
  if (tree_ != nullptr) {
    GEOSSTRtree_destroy_r(handle_, tree_);
    tree_ = nullptr;
  }
  GEOS_finish_r(handle_);
}

void GEOSIndex::Init(const Config* config) {
  tree_ = GEOSSTRtree_create_r(handle_, 10);
  build_reader_ = std::make_unique<geoarrow::geos::ArrayReader>();
}

void GEOSIndex::Clear() {
  if (tree_ != nullptr) {
    GEOSSTRtree_destroy_r(handle_, tree_);
    tree_ = GEOSSTRtree_create_r(handle_, 10);
  }

  if (!build_data_.empty()) {
    for (auto& build_data : build_data_) {
      GEOSGeom_destroy_r(handle_, build_data.geom);
    }
    build_data_.clear();
  }
  build_reader_ = nullptr;
}

void GEOSIndex::PushBuild(const ArrowSchema* schema, const ArrowArray* array,
                          int64_t offset, int64_t length) {
  size_t n_out;

  geoms_vec_->resize(length);
  build_reader_->InitFromSchema(handle_, const_cast<ArrowSchema*>(schema));
  auto err_code = build_reader_->Read(const_cast<ArrowArray*>(array), offset, length,
                                      geoms_vec_->mutable_data(), &n_out);

  if (err_code != GEOARROW_GEOS_OK) {
    throw std::runtime_error("Failed to read WKB from build, errno " +
                             std::to_string(err_code) + ": " +
                             std::string(build_reader_->GetLastError()));
  }

  auto prev_size = build_data_.size();
  build_data_.resize(prev_size + n_out);

  for (size_t i = 0; i < n_out; i++) {
    // Next batch of read will destroy the geometry, so we need to take the ownership
    auto* geom_build = geoms_vec_->take_ownership_of(i);

    if (geom_build != nullptr) {
#ifndef NDEBUG
      auto valid_val = GEOSisValid_r(handle_, geom_build);
      if (valid_val != 1) {
        throw std::runtime_error("Invalid data, id: " + std::to_string(i));
      }
#endif
      auto& build_data = build_data_[i + prev_size];
      build_data.geom = geom_build;
      build_data.build_index = prev_size + i;
      // store index, not pointer to avoid invalid pointers by resize
      GEOSSTRtree_insert_r(handle_, tree_, geom_build, (void*)build_data.build_index);
    }
  }
}

void GEOSIndex::FinishBuilding() {
  if (GEOSSTRtree_build_r(handle_, tree_) != 1) {
    throw std::runtime_error("Failed to build GEOSSTRtree");
  }
}

void GEOSIndex::PushStream(Context* base_ctx, const ArrowSchema* schema,
                           const struct ArrowArray* array, int64_t offset, int64_t length,
                           Predicate predicate, std::vector<uint32_t>* build_indices,
                           std::vector<uint32_t>* array_indices,
                           int32_t array_index_offset) {
  auto* ctx = (GEOSContext*)base_ctx;
  auto handle = ctx->handle;
  size_t n_out;

  if (ctx->geom_vec == nullptr) {
    ctx->geom_vec = std::make_unique<geoarrow::geos::GeometryVector>(handle);
  }

  if (ctx->reader == nullptr) {
    ctx->reader = std::make_unique<geoarrow::geos::ArrayReader>();
    ctx->reader->InitFromSchema(handle, const_cast<ArrowSchema*>(schema));
  }

  if (ctx->prepared_geometries.empty()) {
    ctx->prepared_geometries.resize(build_data_.size(), nullptr);
  }

  ctx->geom_vec->resize(length);
  if (ctx->reader->Read((ArrowArray*)array, offset, length, ctx->geom_vec->mutable_data(),
                        &n_out) != GEOARROW_GEOS_OK) {
    throw std::runtime_error("Failed to read WKB from stream");
  }

  Payload payload;

  payload.handle = handle;
  payload.ctx = ctx;
  payload.p_build_data = build_data_.data();
  payload.build_indices = build_indices;
  payload.stream_indices = array_indices;

  for (size_t i = 0; i < n_out; i++) {
    auto* geom_point = ctx->geom_vec->borrow(i);

    if (geom_point != nullptr) {
      double x, y;

      GEOSGeomGetX_r(handle, geom_point, &x);
      GEOSGeomGetY_r(handle, geom_point, &y);

      payload.stream_index = array_index_offset + offset + i;
      payload.geom = geom_point;
      GEOSSTRtree_query_r(
          handle, tree_, geom_point,
          [](void* item, void* data) {
            auto build_data_index = (size_t)item;
            auto* payload = (Payload*)data;
            const auto& build_data = payload->p_build_data[build_data_index];

            // use GEOSPreparedContains_r for the single thread setting
            if (payload->ctx->prepared_geometries[build_data_index] == nullptr) {
              payload->ctx->prepared_geometries[build_data_index] =
                  GEOSPrepare_r(payload->handle, build_data.geom);
            }

            if (GEOSPreparedContains_r(
                    payload->handle, payload->ctx->prepared_geometries[build_data_index],
                    payload->geom) == 1) {
              payload->build_indices->push_back(build_data.build_index);
              payload->stream_indices->push_back(payload->stream_index);
            }
          },
          &payload);
    }
  }
}

}  // namespace gpuspatial