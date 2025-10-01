#ifndef GPUSPATIAL_UTILS_MEM_UTILS_HPP
#define GPUSPATIAL_UTILS_MEM_UTILS_HPP
#include <cuda_runtime.h>
#include "gpuspatial/utils/exception.h"
#include "rmm/cuda_stream_view.hpp"

namespace gpuspatial {
namespace detail {
template <typename T>
void async_copy_h2d(const rmm::cuda_stream_view& stream, const T* src, T* dst,
                    size_t count) {
  // Calculate the total size in bytes from the element count
  size_t size_in_bytes = count * sizeof(T);

  // Issue the asynchronous copy command to the specified stream
  CUDA_CHECK(cudaMemcpyAsync(dst, src, size_in_bytes, cudaMemcpyHostToDevice, stream));
}
}
}  // namespace gpuspatial
#endif  // GPUSPATIAL_UTILS_MEM_UTILS_HPP
