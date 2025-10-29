#ifndef GPUSPATIAL_GEOM_LINE_SEGMENT_CUH
#define GPUSPATIAL_GEOM_LINE_SEGMENT_CUH
#include "gpuspatial/geom/box.cuh"
#include "gpuspatial/geom/point.cuh"
#include "gpuspatial/utils/cuda_utils.h"
#include "gpuspatial/utils/floating_point.h"

namespace gpuspatial {
enum class PointLineSegmentLocation {
  kLeft,
  kBoundary,
  kRight,
};

enum class RayCastResult { kIn, kOut, kOn };

template <typename POINT_T>
struct EdgeEquation {
  typename POINT_T::scalar_t a, b, c;  // ax + by + c=0; b >= 0

  EdgeEquation() = default;

  DEV_HOST EdgeEquation(const POINT_T& p1, const POINT_T& p2) {
    a = p1.y() - p2.y();
    b = p2.x() - p1.x();
    c = -p1.x() * a - p1.y() * b;

    assert(a != 0 || b != 0);

    if (b < 0) {
      a = -a;
      b = -b;
      c = -c;
    }
  }
};

template <typename POINT_T>
class LineSegment {
  using point_t = POINT_T;
  using scalar_t = typename point_t::scalar_t;
  static constexpr int n_dim = point_t::n_dim;
  using box_t = Box<point_t>;

 public:
  LineSegment() = default;
  DEV_HOST LineSegment(const point_t& p1, const point_t& p2) : p1_(p1), p2_(p2) {}

  DEV_HOST_INLINE const point_t& get_p1() const { return p1_; }

  DEV_HOST_INLINE const point_t& get_p2() const { return p2_; }

  DEV_HOST_INLINE point_t centroid() const {
    point_t c;
    for (int i = 0; i < n_dim; i++) {
      c.set_coordinate(i, (p1_.get_coordinate(i) + p2_.get_coordinate(i)) / 2.0);
    }
    return c;
  }

  DEV_HOST_INLINE int orientation(const point_t& q) const {
    auto d_x = (q.x() - p1_.x());
    auto d_y = (q.y() - p1_.y());
    typename point_t::scalar_t constexpr zero = 0.0;

    if (float_equal(d_x, zero) && float_equal(d_y, zero)) {
      return 0;
    }
    auto v1 = d_x * (p2_.y() - p1_.y());
    auto v2 = (p2_.x() - p1_.x()) * d_y;

    if (float_equal(v1, v2)) {
      return 0;
    }
    auto side = v1 - v2;
    return side < 0 ? -1 : 1;
  }

  DEV_HOST_INLINE box_t get_mbr() const {
    point_t min_p, max_p;
    for (int dim = 0; dim < n_dim; dim++) {
      min_p.set_coordinate(dim, std::numeric_limits<scalar_t>::max());
      max_p.set_coordinate(dim, std::numeric_limits<scalar_t>::lowest());
    }

    for (int dim = 0; dim < n_dim; dim++) {
      auto v1 = p1_.get_coordinate(dim);
      auto v2 = p2_.get_coordinate(dim);
      auto min_v = std::min(v1, v2);
      auto max_v = std::max(v1, v2);
      min_p.set_coordinate(dim, std::min(min_p.get_coordinate(dim), min_v));
      max_p.set_coordinate(dim, std::max(max_p.get_coordinate(dim), max_v));
    }
    return box_t(min_p, max_p);
  }

  template <typename point_type = POINT_T,
            typename std::enable_if<point_type::n_dim == 2, bool>::type = true>
  DEV_HOST_INLINE bool covers(const point_type& q) const {
    auto side = ((q.x() - p1_.x()) * (p2_.y() - p1_.y()) -
                 (p2_.x() - p1_.x()) * (q.y() - p1_.y()));

    if (side == 0) {
      return (p1_.x() <= q.x() && q.x() <= p2_.x()) ||
             (p1_.x() >= q.x() && q.x() >= p2_.x()) ||
             (p1_.y() <= q.y() && q.y() <= p2_.y()) ||
             (p1_.y() >= q.y() && q.y() >= p2_.y());
    }
    return false;
  }

  template <typename point_type = POINT_T,
            typename std::enable_if<point_type::n_dim == 2, bool>::type = true>
  DEV_HOST_INLINE PointLocation locate_point(const point_t& q) const {
    if (orientation(q) == 0) {
      if (((p1_.x() <= q.x() && q.x() <= p2_.x()) ||
           (p2_.x() <= q.x() && q.x() <= p1_.x())) &&
          ((p1_.y() <= q.y() && q.y() <= p2_.y()) ||
           (p2_.y() <= q.y() && q.y() <= p1_.y()))) {
        if ((p1_.x() == q.x() && p1_.y() == q.y()) ||
            (p2_.x() == q.x() && p2_.y() == q.y()))
          return PointLocation::kBoundary;
        return PointLocation::kInside;
      }
    }

    return PointLocation::kOutside;
  }

  // Ref:https://github.com/tidwall/tg/blob/b26f589e18027cbbdff70268f9eb6d1fad6dbee1/tg.c#L1172
  template <typename point_type = POINT_T,
            typename std::enable_if<point_type::n_dim == 2, bool>::type = true>
  DEV_HOST_INLINE RayCastResult raycast(point_t p) const {
    auto r = get_mbr();
    if (p.y() < r.get_min().y() || p.y() > r.get_max().y()) {
      return RayCastResult::kOut;
    }
    if (p.x() < r.get_min().x()) {
      if (p.y() != r.get_min().y() && p.y() != r.get_max().y()) {
        return RayCastResult::kIn;
      }
    } else if (p.x() > r.get_max().x()) {
      if (r.get_min().y() != r.get_max().y() && r.get_min().x() != r.get_max().x()) {
        return RayCastResult::kOut;
      }
    }
    auto a = p1_;
    auto b = p2_;
    if (b.y() < a.y()) {
      auto t = a;
      a = b;
      b = t;
    }
    if (pteq(p, a) || pteq(p, b)) {
      return RayCastResult::kOn;
    }
    if (a.y() == b.y()) {
      if (a.x() == b.x()) {
        return RayCastResult::kOut;
      }
      if (p.y() == b.y()) {
        if (!(p.x() < r.get_min().x() || p.x() > r.get_max().x())) {
          return RayCastResult::kOn;
        }
      }
    }
    if (a.x() == b.x() && p.x() == b.x()) {
      if (p.y() >= a.y() && p.y() <= b.y()) {
        return RayCastResult::kOn;
      }
    }
    if (collinear(a.x(), a.y(), b.x(), b.y(), p.x(), p.y())) {
      if (p.x() < r.get_min().x()) {
        if (r.get_min().y() == r.get_max().y()) {
          return RayCastResult::kOut;
        }
      } else if (p.x() > r.get_max().x()) {
        return RayCastResult::kOut;
      }
      return RayCastResult::kOn;
    }
    if (p.y() == a.y() || p.y() == b.y()) {
      p.y() = nexttoward(p.y(), INFINITY);
    }
    if (p.y() < a.y() || p.y() > b.y()) {
      return RayCastResult::kOut;
    }
    if (a.x() > b.x()) {
      if (p.x() >= a.x()) {
        return RayCastResult::kOut;
      }
      if (p.x() <= b.x()) {
        return RayCastResult::kIn;
      }
    } else {
      if (p.x() >= b.x()) {
        return RayCastResult::kOut;
      }
      if (p.x() <= a.x()) {
        return RayCastResult::kIn;
      }
    }
    if ((p.y() - a.y()) / (p.x() - a.x()) >= (b.y() - a.y()) / (b.x() - a.x())) {
      return RayCastResult::kIn;
    }
    return RayCastResult::kOut;
  }

 private:
  point_t p1_, p2_;

  DEV_HOST_INLINE static bool collinear(double x1, double y1,  // point 1
                                        double x2, double y2,  // point 2
                                        double x3, double y3   // point 3
  ) {
    bool x1x2 = feq(x1, x2);
    bool x1x3 = feq(x1, x3);
    bool x2x3 = feq(x2, x3);
    bool y1y2 = feq(y1, y2);
    bool y1y3 = feq(y1, y3);
    bool y2y3 = feq(y2, y3);
    if (x1x2) {
      return x1x3;
    }
    if (y1y2) {
      return y1y3;
    }
    if ((x1x2 & y1y2) | (x1x3 & y1y3) | (x2x3 & y2y3)) {
      return true;
    }
    double cx1 = x3 - x1;
    double cy1 = y3 - y1;
    double cx2 = x2 - x1;
    double cy2 = y2 - y1;
    double s1 = cx1 * cy2;
    double s2 = cy1 * cx2;
    // Check if precision was lost.
    double s3 = (s1 / cy2) - cx1;
    double s4 = (s2 / cx2) - cy1;
    if (s3 < 0) {
      s1 = nexttoward(s1, -INFINITY);
    } else if (s3 > 0) {
      s1 = nexttoward(s1, +INFINITY);
    }
    if (s4 < 0) {
      s2 = nexttoward(s2, -INFINITY);
    } else if (s4 > 0) {
      s2 = nexttoward(s2, +INFINITY);
    }
    return eq_zero(s1 - s2);
  }
  DEV_HOST_INLINE static bool feq(double x, double y) { return !((x < y) | (x > y)); }

  DEV_HOST_INLINE static bool eq_zero(double x) { return feq(x, 0); }

  DEV_HOST_INLINE static bool pteq(const point_t& a, const point_t& b) {
    return feq(a.x(), b.x()) && feq(a.y(), b.y());
  }
};

}  // namespace gpuspatial
#endif  //  GPUSPATIAL_GEOM_LINE_SEGMENT_CUH