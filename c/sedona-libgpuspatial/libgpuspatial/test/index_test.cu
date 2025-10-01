#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <random>
#include <vector>
#include "../include/gpuspatial/index/optix_spatial_index.cuh"
#include "../include/gpuspatial/index/spatial_index.hpp"
#include "test_common.hpp"

namespace TestUtils {
std::string GetTestDataPath(const std::string& relative_path_to_file);
}

namespace gpuspatial {

template <typename POINT_T>
std::vector<Box<POINT_T>> generateUniformBoundingBoxes2D(
    size_t count, typename POINT_T::scalar_t x_min, typename POINT_T::scalar_t x_max,
    typename POINT_T::scalar_t y_min, typename POINT_T::scalar_t y_max,
    typename POINT_T::scalar_t width_min, typename POINT_T::scalar_t width_max,
    typename POINT_T::scalar_t height_min, typename POINT_T::scalar_t height_max) {
  using scalar_t = typename POINT_T::scalar_t;
  std::vector<Box<POINT_T>> boxes;
  boxes.reserve(count);

  // Use a high-quality random number generator engine
  std::random_device rd;
  std::mt19937 gen(rd());

  // Create uniform distributions for each parameter
  std::uniform_real_distribution<scalar_t> x_dist(x_min, x_max);
  std::uniform_real_distribution<scalar_t> y_dist(y_min, y_max);
  std::uniform_real_distribution<scalar_t> width_dist(width_min, width_max);
  std::uniform_real_distribution<scalar_t> height_dist(height_min, height_max);

  for (size_t i = 0; i < count; ++i) {
    auto x = x_dist(gen);
    auto y = y_dist(gen);
    auto width = width_dist(gen);
    auto height = height_dist(gen);

    POINT_T min_point(x, y);
    POINT_T max_point(x + width, y + height);
    Box<POINT_T> box(min_point, max_point);
    boxes.push_back(box);
  }

  return boxes;
}

template <typename POINT_T>
std::vector<POINT_T> generateUniformPoints2D(size_t count,
                                             typename POINT_T::scalar_t x_min,
                                             typename POINT_T::scalar_t x_max,
                                             typename POINT_T::scalar_t y_min,
                                             typename POINT_T::scalar_t y_max) {
  using scalar_t = typename POINT_T::scalar_t;
  std::vector<POINT_T> points;
  points.reserve(count);

  // Use a high-quality random number generator engine
  std::random_device rd;
  std::mt19937 gen(rd());

  // Create uniform distributions for each parameter
  std::uniform_real_distribution<scalar_t> x_dist(x_min, x_max);
  std::uniform_real_distribution<scalar_t> y_dist(y_min, y_max);

  for (size_t i = 0; i < count; ++i) {
    auto x = x_dist(gen);
    auto y = y_dist(gen);

    POINT_T point(x, y);
    points.push_back(point);
  }

  return points;
}

template <typename T>
class SpatialIndexTest : public ::testing::Test {};
TYPED_TEST_SUITE(SpatialIndexTest, TestUtils::PointTypes);

TYPED_TEST(SpatialIndexTest, PointBox) {
  using point_t = TypeParam;
  using index_t = uint32_t;
  using scalar_t = typename point_t::scalar_t;
  using spatial_index_t = OptixSpatialIndex<point_t, index_t>;

  typename spatial_index_t::SpatialIndexConfig config;
  std::string ptx_root = TestUtils::GetTestDataPath("shaders_ptx");

  config.ptx_root = ptx_root.c_str();
  spatial_index_t spatial_index;

  std::vector<uint32_t> build_indices, stream_indices;

  spatial_index.Init(&config);
  int n_batches = 10;
  int min_boxes_per_batch = 1;
  int max_boxes_per_batch = 10000;
  scalar_t x_min = 0.0, x_max = 100.0;
  scalar_t y_min = 0.0, y_max = 100.0;
  scalar_t width_min = 0.1, width_max = 5.0;
  scalar_t height_min = 0.1, height_max = 5.0;

  for (int batch = 0; batch < n_batches; batch++) {
    spatial_index.Clear();
    build_indices.clear();
    stream_indices.clear();

    std::random_device rd;
    std::mt19937 gen(rd());

    // 2. Define the uniform distribution.
    // std::uniform_int_distribution maps the engine's output to a
    // uniform integer range, inclusive of both min and max.
    std::uniform_int_distribution<> distrib(min_boxes_per_batch, max_boxes_per_batch);

    // 3. Generate and print the random number.
    int build_size = distrib(gen);
    auto build_boxes = generateUniformBoundingBoxes2D<point_t>(
        build_size, x_min, x_max, y_min, y_max, width_min, width_max, height_min,
        height_max);
    spatial_index.PushBuild(build_boxes.data(), build_boxes.size());
    spatial_index.FinishBuilding();
    auto stream_size = distrib(gen);

    auto stream_points =
        generateUniformPoints2D<point_t>(stream_size, x_min, x_max, y_min, y_max);

    auto context = spatial_index.CreateContext();

    spatial_index.PushStream(context.get(), stream_points.data(), stream_size,
                             &build_indices, &stream_indices);

    int n_results = 0;
    for (auto box : build_boxes) {
      for (auto point : stream_points) {
        if (box.covers(point)) {
          n_results++;
        }
      }
    }
    EXPECT_EQ(n_results, build_indices.size());
  }
}

TYPED_TEST(SpatialIndexTest, BoxBox) {
  using point_t = TypeParam;
  using index_t = uint32_t;
  using scalar_t = typename point_t::scalar_t;
  using spatial_index_t = OptixSpatialIndex<point_t, index_t>;

  typename spatial_index_t::SpatialIndexConfig config;
  std::string ptx_root = TestUtils::GetTestDataPath("shaders_ptx");

  config.ptx_root = ptx_root.c_str();
  spatial_index_t spatial_index;

  std::vector<uint32_t> build_indices, stream_indices;

  spatial_index.Init(&config);
  int n_batches = 10;
  int min_boxes_per_batch = 1;
  int max_boxes_per_batch = 10000;
  scalar_t x_min = 0.0, x_max = 100.0;
  scalar_t y_min = 0.0, y_max = 100.0;
  scalar_t width_min = 0.1, width_max = 5.0;
  scalar_t height_min = 0.1, height_max = 5.0;

  for (int batch = 0; batch < n_batches; batch++) {
    spatial_index.Clear();
    build_indices.clear();
    stream_indices.clear();

    std::random_device rd;
    std::mt19937 gen(rd());

    // 2. Define the uniform distribution.
    // std::uniform_int_distribution maps the engine's output to a
    // uniform integer range, inclusive of both min and max.
    std::uniform_int_distribution<> distrib(min_boxes_per_batch, max_boxes_per_batch);

    // 3. Generate and print the random number.
    int build_size = distrib(gen);
    auto build_boxes = generateUniformBoundingBoxes2D<point_t>(
        build_size, x_min, x_max, y_min, y_max, width_min, width_max, height_min,
        height_max);
    spatial_index.PushBuild(build_boxes.data(), build_boxes.size());
    spatial_index.FinishBuilding();
    auto stream_size = distrib(gen);

    auto stream_boxes = generateUniformBoundingBoxes2D<point_t>(
        stream_size, x_min, x_max, y_min, y_max, width_min, width_max, height_min,
        height_max);

    auto context = spatial_index.CreateContext();

    spatial_index.PushStream(context.get(), stream_boxes.data(), stream_size,
                             &build_indices, &stream_indices);

    int n_results = 0;
    for (auto box1 : build_boxes) {
      for (auto box2 : stream_boxes) {
        if (box1.intersects(box2)) {
          n_results++;
        }
      }
    }
    EXPECT_EQ(n_results, build_indices.size());
  }
}

}  // namespace gpuspatial