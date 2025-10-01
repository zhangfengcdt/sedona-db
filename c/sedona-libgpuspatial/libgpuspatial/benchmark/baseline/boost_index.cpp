#include "boost_index.hpp"

namespace gpuspatial {

void BoostIndex::Init(const Config* config) { Clear(); }

void BoostIndex::Clear() {
  index_inputs_.clear();
  builds_.clear();
  rtree.clear();
}

void BoostIndex::PushBuild(const ArrowSchema* schema, const ArrowArray* array,
                           int64_t offset, int64_t length) {
  ArrowArrayView array_view;
  ArrowError arrow_error;
  ArrowArrayViewInitFromType(&array_view, NANOARROW_TYPE_BINARY);
  if (ArrowArrayViewSetArray(&array_view, array, &arrow_error) != NANOARROW_OK) {
    throw std::runtime_error("ArrowArrayViewSetArray error " +
                             std::string(arrow_error.message));
  }
  auto prev_size = index_inputs_.size();

  index_inputs_.reserve(index_inputs_.size() + length);
  builds_.reserve(builds_.size() + length);

  for (int64_t i = offset; i < length; i++) {
    if (!ArrowArrayViewIsNull(&array_view, i)) {
      auto item = ArrowArrayViewGetBytesUnsafe(&array_view, i);
      boost::span<const uint8_t> data(item.data.as_uint8, item.size_bytes);
      auto geometry = ngen::geopackage::wkb::read(data);
      box_t box = boost::apply_visitor(envelope_visitor(), geometry);
      auto build_idx = prev_size + i;

      if (!boost::geometry::is_valid(box)) {
        box.min_corner().set<0>(0);
        box.min_corner().set<1>(0);
        box.max_corner().set<0>(0);
        box.max_corner().set<1>(0);
      }

      index_inputs_.emplace_back(box, build_idx);
      builds_.push_back(geometry);
    }
  }
}

void BoostIndex::FinishBuilding() {
  // bulk loading
  rtree = rtree_t(index_inputs_.begin(), index_inputs_.end());
}

void BoostIndex::PushStream(Context* base_ctx, const ArrowSchema* schema,
                            const ArrowArray* array, int64_t offset, int64_t length,
                            Predicate predicate, std::vector<uint32_t>* build_indices,
                            std::vector<uint32_t>* array_indices,
                            int32_t array_index_offset) {
  namespace bg = boost::geometry;
  ArrowArrayView array_view;
  ArrowError arrow_error;
  ArrowArrayViewInitFromType(&array_view, NANOARROW_TYPE_BINARY);
  if (ArrowArrayViewSetArray(&array_view, array, &arrow_error) != NANOARROW_OK) {
    throw std::runtime_error("ArrowArrayViewSetArray error " +
                             std::string(arrow_error.message));
  }
  for (int64_t i = 0; i < length; i++) {
    if (!ArrowArrayViewIsNull(&array_view, i + offset)) {
      auto item = ArrowArrayViewGetBytesUnsafe(&array_view, i + offset);
      boost::span<const uint8_t> data(item.data.as_uint8, item.size_bytes);
      auto geometry = ngen::geopackage::wkb::read(data);

      auto p = boost::apply_visitor(point_visitor(), geometry);

      std::for_each(
          rtree.qbegin(boost::geometry::index::contains(p)), rtree.qend(),
          [&](auto const& val) {
            auto build_index = val.second;

            if (boost::apply_visitor(within_geometry_visitor(p), builds_[build_index])) {
              auto stream_index = array_index_offset + offset + i;
              build_indices->push_back(build_index);
              array_indices->push_back(stream_index);
            }
          });
    }
  }
}

}  // namespace gpuspatial