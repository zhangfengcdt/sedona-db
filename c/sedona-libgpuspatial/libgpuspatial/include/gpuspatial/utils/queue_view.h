#ifndef GPUSPATIAL_UTILS_QUEUE_VIEW_H
#define GPUSPATIAL_UTILS_QUEUE_VIEW_H
#include <cooperative_groups.h>

#include "gpuspatial/utils/array_view.h"
#include "gpuspatial/utils/cuda_utils.h"

namespace gpuspatial {
template <typename T, typename SIZE_T = uint32_t>
class QueueView {
 public:
  using value_type = T;

  QueueView() = default;

  DEV_HOST explicit QueueView(const ArrayView<T>& data, SIZE_T* last_pos)
      : data_(data), last_pos_(last_pos) {}

  DEV_INLINE SIZE_T Append(const T& item) {
    auto allocation = atomicAdd(last_pos_, 1);
#if defined(__CUDA_ARCH__)
    if (allocation >= data_.size()) {
      printf("Queue overflow, TID %u, allocation %u, capacity %lu\n", TID_1D, allocation,
             data_.size());
      __trap();
    }
#endif
    assert(allocation < data_.size());
    data_[allocation] = item;
    return allocation;
  }

  DEV_INLINE SIZE_T AppendWarp(const T& item) {
    auto g = cooperative_groups::coalesced_threads();
    SIZE_T warp_res;

    if (g.thread_rank() == 0) {
      warp_res = atomicAdd(last_pos_, g.size());
    }
    auto begin = g.shfl(warp_res, 0) + g.thread_rank();
    assert(begin < data_.size());
    data_[begin] = item;
    return begin;
  }

  DEV_INLINE void Clear() const { *last_pos_ = 0; }

  DEV_INLINE T& operator[](SIZE_T i) { return data_[i]; }

  DEV_INLINE const T& operator[](SIZE_T i) const { return data_[i]; }

  DEV_INLINE SIZE_T size() const { return *last_pos_; }

  DEV_INLINE void Swap(QueueView& rhs) {
    data_.Swap(rhs.data_);
    thrust::swap(last_pos_, rhs.last_pos_);
  }

  DEV_INLINE T* data() { return data_.data(); }

  DEV_INLINE const T* data() const { return data_.data(); }

 private:
  ArrayView<T> data_;
  SIZE_T* last_pos_{};
};
}  // namespace gpuspatial
#endif  // GPUSPATIAL_UTILS_QUEUE_VIEW_H
