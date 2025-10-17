#ifndef GPUSPATIAL_RELATE_RELATE_BLOCK_CUH
#define GPUSPATIAL_RELATE_RELATE_BLOCK_CUH
#include "gpuspatial/relate/relate.cuh"
namespace gpuspatial {

template <typename POINT_T, typename INDEX_T>
DEV_INLINE int32_t
relate(const MultiPoint<POINT_T>& geom1, const MultiPolygon<POINT_T, INDEX_T>& geom2,
       cub::BlockReduce<int, MAX_BLOCK_SIZE>::TempStorage* temp_storage) {
  uint32_t nloops1;
  uint32_t nloops2;
  int32_t retval = IM__EXTER_EXTER_2D;

  if (geom1.empty()) {
    if (geom2.empty()) return IM__EXTER_EXTER_2D;
    return IM__EXTER_INTER_2D | IM__EXTER_BOUND_1D | IM__EXTER_EXTER_2D;
  } else if (geom2.empty())
    return IM__INTER_EXTER_0D | IM__EXTER_EXTER_2D;

  nloops1 = geom1.num_points();
  nloops2 = geom2.num_polygons();

  retval = IM__EXTER_EXTER_2D;
  for (int i = 0; i < nloops1; i++) {
    const auto& pt = geom1.get_point(i);
    bool matched = false;

    for (int j = 0; j < nloops2; j++) {
      const auto& poly = geom2.get_polygon(j);
      /* skip empty polygon */
      if (poly.empty()) continue;
      retval |= IM__EXTER_INTER_2D | IM__EXTER_BOUND_1D;
      auto& mbr = poly.get_mbr();
      // if (!mbr.covers(pt)) {
      //   continue;
      // }

      /* dive into the polygon */
      switch (poly.locate_point(pt, temp_storage)) {
        case PointLocation::kInside: {
          matched = true;
          retval |= IM__INTER_INTER_0D;
          break;
        }
        case PointLocation::kBoundary: {
          matched = true;
          retval |= IM__INTER_BOUND_0D;
          break;
        }
        case PointLocation::kOutside: {
          break;
        }
        default: {
          printf("Error!!\n");
          assert(false);
          return -1; /* error */
        }
      }
    }
    if (!matched) retval |= IM__INTER_EXTER_0D;
  }
  return retval;
}

template <typename POINT_T, typename INDEX_T>
DEV_INLINE int32_t
relate(const MultiPolygon<POINT_T, INDEX_T>& geom1, const MultiPoint<POINT_T>& geom2,
       cub::BlockReduce<int, MAX_BLOCK_SIZE>::TempStorage* temp_storage) {
  return IM__TWIST(relate(geom2, geom1, temp_storage));
}

template <typename POINT_T, typename INDEX_T>
DEV_INLINE int32_t
relate(const POINT_T& geom1, const MultiPolygon<POINT_T, INDEX_T>& geom2,
       cub::BlockReduce<int, MAX_BLOCK_SIZE>::TempStorage* temp_storage) {
  MultiPoint<POINT_T> p1;
  if (!geom1.empty()) {
    p1 = {ArrayView<POINT_T>(const_cast<POINT_T*>(&geom1), 1), geom1.get_mbr()};
  }
  return relate(p1, geom2, temp_storage);
}

template <typename POINT_T, typename INDEX_T>
DEV_INLINE int32_t
relate(const MultiPolygon<POINT_T, INDEX_T>& geom1, const POINT_T& geom2,
       cub::BlockReduce<int, MAX_BLOCK_SIZE>::TempStorage* temp_storage) {
  return IM__TWIST(relate(geom2, geom1, temp_storage));
}

template <typename POINT_T, typename INDEX_T>
DEV_INLINE int32_t
relate(const POINT_T& geom1, const Polygon<POINT_T, INDEX_T>& geom2,
       cub::BlockReduce<int, MAX_BLOCK_SIZE>::TempStorage* temp_storage) {
  MultiPoint<POINT_T> m1;
  if (!geom1.empty()) {
    m1 = {ArrayView<POINT_T>(const_cast<POINT_T*>(&geom1), 1), geom1.get_mbr()};
  }

  auto prefix_sum_rings = geom2.get_prefix_sum_rings();
  auto vertices = geom2.get_vertices();

  INDEX_T prefix_sum_parts[2] = {0, (INDEX_T)geom2.num_rings()};

  MultiPolygon<POINT_T, INDEX_T> m2(ArrayView<INDEX_T>(prefix_sum_parts, 2),
                                    prefix_sum_rings, vertices, geom2.get_mbr());

  return relate(m1, m2, temp_storage);
}

template <typename POINT_T, typename INDEX_T>
DEV_INLINE int32_t
relate(const Polygon<POINT_T, INDEX_T>& geom1, const POINT_T& geom2,
       cub::BlockReduce<int, MAX_BLOCK_SIZE>::TempStorage* temp_storage) {
  return IM__TWIST(relate(geom2, geom1, temp_storage));
}

}  // namespace gpuspatial
#endif  // GPUSPATIAL_RELATE_RELATE_BLOCK_CUH