#ifndef GPUSPATIAL_UTILS_MORTON_CODE_H
#define GPUSPATIAL_UTILS_MORTON_CODE_H
// adopt from https://github.com/ToruNiina/lbvh
#include <vector_types.h>
#include <cuda/std/cmath>
#include "gpuspatial/utils/cuda_utils.h"

namespace gpuspatial {
namespace detail {
/**
 * @ brief Spreads the lower 10 bits of v to every third bit for 3D interleaving.
 */
DEV_HOST_INLINE
std::uint32_t expand_bits_3d(std::uint32_t v) noexcept {
  v = (v * 0x00010001u) & 0xFF0000FFu;
  v = (v * 0x00000101u) & 0x0F00F00Fu;
  v = (v * 0x00000011u) & 0xC30C30C3u;
  v = (v * 0x00000005u) & 0x49249249u;
  return v;
}

/**
 * @brief Spreads the lower 16 bits of v to every second bit for 2D interleaving.
 */
DEV_HOST_INLINE
std::uint32_t expand_bits_2d(std::uint32_t v) noexcept {
  v = (v | (v << 8)) & 0x00FF00FFu;
  v = (v | (v << 4)) & 0x0F0F0F0Fu;
  v = (v | (v << 2)) & 0x33333333u;
  v = (v | (v << 1)) & 0x55555555u;
  return v;
}

// --- 3D Morton Code Functions ---

DEV_HOST_INLINE
std::uint32_t morton_code(float3 xyz, float resolution = 1024.0f) noexcept {
  xyz.x = ::fminf(::fmaxf(xyz.x * resolution, 0.0f), resolution - 1.0f);
  xyz.y = ::fminf(::fmaxf(xyz.y * resolution, 0.0f), resolution - 1.0f);
  xyz.z = ::fminf(::fmaxf(xyz.z * resolution, 0.0f), resolution - 1.0f);
  const std::uint32_t xx = expand_bits_3d(static_cast<std::uint32_t>(xyz.x));
  const std::uint32_t yy = expand_bits_3d(static_cast<std::uint32_t>(xyz.y));
  const std::uint32_t zz = expand_bits_3d(static_cast<std::uint32_t>(xyz.z));
  return (xx << 2) | (yy << 1) | zz;
}

DEV_HOST_INLINE
std::uint32_t morton_code(double3 xyz, double resolution = 1024.0) noexcept {
  xyz.x = ::fmin(::fmax(xyz.x * resolution, 0.0), resolution - 1.0);
  xyz.y = ::fmin(::fmax(xyz.y * resolution, 0.0), resolution - 1.0);
  xyz.z = ::fmin(::fmax(xyz.z * resolution, 0.0), resolution - 1.0);
  const std::uint32_t xx = expand_bits_3d(static_cast<std::uint32_t>(xyz.x));
  const std::uint32_t yy = expand_bits_3d(static_cast<std::uint32_t>(xyz.y));
  const std::uint32_t zz = expand_bits_3d(static_cast<std::uint32_t>(xyz.z));
  return (xx << 2) | (yy << 1) | zz;
}

// --- 2D Morton Code Functions ---

DEV_HOST_INLINE
std::uint32_t morton_code(float2 xy, float resolution = 1024.0f) noexcept {
  xy.x = ::fminf(::fmaxf(xy.x * resolution, 0.0f), resolution - 1.0f);
  xy.y = ::fminf(::fmaxf(xy.y * resolution, 0.0f), resolution - 1.0f);
  const std::uint32_t xx = expand_bits_2d(static_cast<std::uint32_t>(xy.x));
  const std::uint32_t yy = expand_bits_2d(static_cast<std::uint32_t>(xy.y));
  return (yy << 1) | xx;
}

DEV_HOST_INLINE
std::uint32_t morton_code(double2 xy, double resolution = 1024.0) noexcept {
  xy.x = ::fmin(::fmax(xy.x * resolution, 0.0), resolution - 1.0);
  xy.y = ::fmin(::fmax(xy.y * resolution, 0.0), resolution - 1.0);
  const std::uint32_t xx = expand_bits_2d(static_cast<std::uint32_t>(xy.x));
  const std::uint32_t yy = expand_bits_2d(static_cast<std::uint32_t>(xy.y));
  return (yy << 1) | xx;
}
}  // namespace detail
}  // namespace gpuspatial
#endif  // GPUSPATIAL_UTILS_MORTON_CODE_H
