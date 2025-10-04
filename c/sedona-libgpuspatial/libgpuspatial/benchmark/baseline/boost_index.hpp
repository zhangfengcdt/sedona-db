#ifndef GPUSPATIAL_BENCHMARK_BOOST_INDEX_HPP
#define GPUSPATIAL_BENCHMARK_BOOST_INDEX_HPP
#include "../../include/gpuspatial/index/streaming_joiner.hpp"
#include "boost/geometry.hpp"
#include "boost/variant.hpp"
#include "geoarrow/geoarrow.hpp"
#include "wkb.hpp"

namespace gpuspatial {

class BoostIndex : public StreamingJoiner {
  using point_t = ngen::geopackage::wkb::point_t;
  using linestring_t = ngen::geopackage::wkb::linestring_t;
  using polygon_t = ngen::geopackage::wkb::polygon_t;
  using multipoint_t = ngen::geopackage::wkb::multipoint_t;
  using multilinestring_t = ngen::geopackage::wkb::multilinestring_t;
  using multipolygon_t = ngen::geopackage::wkb::multipolygon_t;
  using box_t = boost::geometry::model::box<point_t>;
  using item_t = std::pair<box_t, size_t>;
  using rtree_t =
      boost::geometry::index::rtree<item_t, boost::geometry::index::rstar<16>>;

  struct envelope_visitor : public boost::static_visitor<box_t> {
    template <typename Geometry>
    box_t operator()(const Geometry& geom) const {
      return bg::return_envelope<box_t>(geom);
    }
  };

  // The visitor to extract a point_t
  struct point_visitor : public boost::static_visitor<point_t> {
    // Handle the point_t case
    point_t operator()(const point_t& point) const { return point; }

    // Handle the linestring_t case
    point_t operator()(const linestring_t& line) const {
      throw std::runtime_error("Should not be invoked");
    }

    // Handle the polygon_t case
    point_t operator()(const polygon_t& poly) const {
      throw std::runtime_error("Should not be invoked");
    }

    // Handle the multipoint_t case
    point_t operator()(const multipoint_t& mpoint) const {
      throw std::runtime_error("Should not be invoked");
    }

    // Handle the multilinestring_t case
    point_t operator()(const multilinestring_t& mline) const {
      throw std::runtime_error("Should not be invoked");
    }

    // Handle the multipolygon_t case
    point_t operator()(const multipolygon_t& mpoly) const {
      throw std::runtime_error("Should not be invoked");
    }
  };

  // Define a visitor for the point-in-polygon test
  template <typename GEOM1_T>
  struct within_geometry_visitor : public boost::static_visitor<bool> {
   private:
    const GEOM1_T& geom1_;

   public:
    // Store the point we are testing
    explicit within_geometry_visitor(const GEOM1_T& geom1) : geom1_(geom1) {}

    template <typename OtherGeometry>
    bool operator()(const OtherGeometry& geom) const {
      return bg::within(geom1_, geom);
    }
  };

 public:
  class BoostContext : public Context {};

  BoostIndex() = default;

  void Clear() override;

  void Init(const Config* config) override;

  void PushBuild(const ArrowSchema* schema, const ArrowArray* array, int64_t offset,
                 int64_t length) override;

  void FinishBuilding() override;

  std::shared_ptr<Context> CreateContext() override {
    return std::make_shared<BoostContext>();
  }

  void PushStream(Context* base_ctx, const ArrowSchema* schema, const ArrowArray* array,
                  int64_t offset, int64_t length, Predicate predicate,
                  std::vector<uint32_t>* build_indices,
                  std::vector<uint32_t>* array_indices,
                  int32_t array_index_offset) override;

 private:
  rtree_t rtree;
  std::vector<item_t> index_inputs_;
  std::vector<ngen::geopackage::wkb::geometry> builds_;
};

}  // namespace gpuspatial
#endif  // GPUSPATIAL_BENCHMARK_BOOST_INDEX_HPP