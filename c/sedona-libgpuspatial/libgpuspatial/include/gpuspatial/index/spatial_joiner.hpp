#ifndef GPUSPATIAL_INDEX_SPATIAL_INDEX_HPP
#define GPUSPATIAL_INDEX_SPATIAL_INDEX_HPP
#include <memory>
#include "gpuspatial/index/streaming_joiner.hpp"

namespace gpuspatial {
std::unique_ptr<StreamingJoiner> CreateSpatialJoiner();

void InitSpatialJoiner(StreamingJoiner* index, const char* ptx_root, uint32_t concurrency);
}  // namespace gpuspatial

#endif  // GPUSPATIAL_INDEX_SPATIAL_INDEX_HPP
