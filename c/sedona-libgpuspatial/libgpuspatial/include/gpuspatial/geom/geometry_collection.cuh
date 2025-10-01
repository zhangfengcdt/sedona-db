#ifndef GPUSPATIAL_GEOM_GEOMETRY_COLLECTION_CUH
#define GPUSPATIAL_GEOM_GEOMETRY_COLLECTION_CUH
#include "gpuspatial/geom/box.cuh"
#include "gpuspatial/geom/geometry_type.cuh"
#include "gpuspatial/utils/array_view.h"

namespace gpuspatial {

namespace detail {
/**
 * @struct Feature
 * @brief Defines a top-level feature, which can be a simple shape,
 * a multi-part geometry, or a collection. It groups GeometryParts
 * and describes the original hierarchy.
 */
template <typename INDEX_T>
struct Feature {
  GeometryType type;
  // The starting index in the Geometry Parts Buffer.
  INDEX_T part_offset;

  // The number of consecutive parts that make up this feature.
  // For a simple Point, part_count is 1.
  // For a MultiPolygon of 5 polygons, part_count is 5.
  INDEX_T part_count;

  // The index of the parent Feature in this same buffer.
  // A value of -1 indicates a root feature with no parent.
  // This field allows us to represent nested GeometryCollections.
  int parent_id;

  Feature() = default;

  Feature(GeometryType a_type, INDEX_T a_part_offset, INDEX_T a_part_count)
      : type(a_type),
        part_offset(a_part_offset),
        part_count(a_part_count),
        parent_id(-1) {}

  Feature(GeometryType a_type, INDEX_T a_part_offset, INDEX_T a_part_count,
          int a_parent_id)
      : type(a_type),
        part_offset(a_part_offset),
        part_count(a_part_count),
        parent_id(a_parent_id) {}
};

/**
 * @struct GeometryPart
 * @brief Defines a single, atomic geometric part (Point, LineString, or Polygon).
 * This is the fundamental building block for all renderable shapes.
 */
template <typename INDEX_T>
struct GeometryPart {
  // The type of this atomic part.
  GeometryType type;

  // The starting index in the main Vertex Buffer for this part's vertices.
  INDEX_T vertex_offset;

  // The total number of vertices this part uses from the Vertex Buffer.
  INDEX_T vertex_count;

  // For Polygons: The starting index in the Ring Starts Buffer.
  // For other types, this can be -1 or 0.
  INDEX_T ring_starts_offset;

  // For Polygons: The number of rings (1 outer + N holes).
  // For LineStrings and Points, this will be 0.
  INDEX_T ring_count;

  // Default constructor
  GeometryPart() = default;

  // Constructor with three parameters
  GeometryPart(GeometryType a_type, INDEX_T a_vertex_offset, INDEX_T a_vertex_count)
      : type(a_type),
        vertex_offset(a_vertex_offset),
        vertex_count(a_vertex_count),
        ring_starts_offset(0),
        ring_count(0) {}

  // Constructor with all parameters
  GeometryPart(GeometryType a_type, INDEX_T a_vertex_offset, INDEX_T a_vertex_count,
               INDEX_T a_ring_starts_offset, INDEX_T a_ring_count)
      : type(a_type),
        vertex_offset(a_vertex_offset),
        vertex_count(a_vertex_count),
        ring_starts_offset(a_ring_starts_offset),
        ring_count(a_ring_count) {}
};

}  // namespace detail

template <typename POINT_T, typename INDEX_T>
class GeometryCollection {
 public:
  using point_t = POINT_T;
  using index_t = INDEX_T;
  using box_t = Box<POINT_T>;
  using scalar_t = typename point_t::scalar_t;
  static constexpr GeometryType geometry_type = GeometryType::kGeometryCollection;

 private:
  ArrayView<detail::Feature<index_t>> features_;
  ArrayView<detail::GeometryPart<index_t>> parts_;
  ArrayView<index_t> ring_starts_;
  ArrayView<point_t> vertices_;
  ArrayView<box_t> mbrs_;
};

}  // namespace gpuspatial
#endif  // GPUSPATIAL_GEOM_GEOMETRY_COLLECTION_CUH
