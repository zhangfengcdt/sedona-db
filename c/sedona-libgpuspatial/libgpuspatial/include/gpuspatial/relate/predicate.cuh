#ifndef GPUSPATIAL_RELATE_PREDICATE_CUH
#define GPUSPATIAL_RELATE_PREDICATE_CUH
#include <cstdint>
#include <cassert>
#include "gpuspatial/relate/im.cuh"
#include "gpuspatial/utils/cuda_utils.h"

namespace gpuspatial {

enum class Predicate {
  kEquals,
  kDisjoint,
  kTouches,
  kContains,
  kCovers,
  kIntersects,
  kWithin,
  kCoveredBy
};

}  // namespace gpuspatial
#endif  // GPUSPATIAL_RELATE_PREDICATE_CUH
