#ifndef GPUSPATIAL_INDEX_SPATIAL_INDEX_HPP
#define GPUSPATIAL_INDEX_SPATIAL_INDEX_HPP
#include <memory>
#include <stdexcept>
#include <vector>
#include "gpuspatial/geom/box.cuh"

namespace gpuspatial {

template <typename POINT_T, typename INDEX_T>
class SpatialIndex {
 public:
  using point_t = POINT_T;
  using box_t = Box<point_t>;

  struct Context {
    virtual ~Context() = default;
  };

  struct Config {
    virtual ~Config() = default;
  };

  virtual ~SpatialIndex() = default;

  /**
   * Initialize the index with the given configuration. This method should be called only
   * once before using the index.
   * @param config
   */
  virtual void Init(const Config* config) = 0;

  /**
   * Provide an array of geometries to build the index.
   * @param boxes An array of boxes to build the index.
   * @param n_boxes number of boxes
   */
  virtual void PushBuild(const box_t* boxes, uint32_t n_boxes) = 0;

  /**
   * Waiting the index to be built.
   * This method should be called after all geometries have been pushed.
   */
  virtual void FinishBuilding() = 0;

  /**
   * Remove all geometries from the index, so the index can reused.
   */
  virtual void Clear() = 0;
  /**
   * Query the index with an array of boxes and return the indices of
   * the boxes that intersect any boxes in the index. This method is
   * thread-safe.
   * @param context A context object that can be used to store intermediate results.
   * @param points A buffer containing points to query the index.
   * @param n_points length of points
   * @param build_indices A vector to store the indices of the geometries in the index
   * that have a spatial overlap with the geometries in the stream.
   * @param stream_indices A vector to store the indices of the geometries in the stream
   * that have a spatial overlap with the geometries in the index.
   */
  virtual void PushStream(Context* context, const point_t* points, uint32_t n_points,
                          std::vector<uint32_t>* build_indices,
                          std::vector<uint32_t>* stream_indices) = 0;
  /**
   * Query the index with an array of boxes and return the indices of
   * the boxes that intersect any boxes in the index. This method is
   * thread-safe.
   * @param context A context object that can be used to store intermediate results.
   * @param boxes A buffer containing boxes to query the index.
   * @param n_boxes length of boxes
   * @param build_indices A vector to store the indices of the geometries in the index
   * that have a spatial overlap with the geometries in the stream.
   * @param stream_indices A vector to store the indices of the geometries in the stream
   * that have a spatial overlap with the geometries in the index.
   */
  virtual void PushStream(Context* context, const box_t* boxes, uint32_t n_boxes,
                          std::vector<uint32_t>* build_indices,
                          std::vector<uint32_t>* stream_indices) = 0;

  /**
   * Create a context object for issuing queries against the index.
   * @return A context object that is used to store intermediate results.
   */
  virtual std::shared_ptr<Context> CreateContext() = 0;
};

}  // namespace gpuspatial
#endif  // GPUSPATIAL_INDEX_SPATIAL_INDEX_HPP
