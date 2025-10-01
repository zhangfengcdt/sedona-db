#ifndef GPUSPATIAL_INDEX_OPTIX_SPATIAL_INDEX_HPP
#define GPUSPATIAL_INDEX_OPTIX_SPATIAL_INDEX_HPP
#include <memory>
#include "gpuspatial/index/spatial_index.hpp"

namespace gpuspatial {

template <typename SCALA_T, int N_DIM>
std::unique_ptr<SpatialIndex<SCALA_T, N_DIM>> CreateOptixSpatialIndex();

template <typename SCALA_T, int N_DIM>
void InitOptixSpatialIndex(SpatialIndex<SCALA_T, N_DIM>* index, const char* ptx_root,
                           uint32_t concurrency);

}  // namespace gpuspatial

#endif  // GPUSPATIAL_INDEX_OPTIX_SPATIAL_INDEX_HPP
