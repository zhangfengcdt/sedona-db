
#include <gtest/gtest.h>

#include "gpuspatial_testing.hpp"

TEST(Testing, TestArrayUtils) {
  nanoarrow::UniqueArray array;

  std::vector<std::string> wkts{"POINT (0 1)", "", "POINT (2 3)"};
  gpuspatial::testing::MakeWKBArrayFromWKT(wkts, array.get());
  ASSERT_EQ(gpuspatial::testing::ReadWKBArray(array.get()), wkts);
}
