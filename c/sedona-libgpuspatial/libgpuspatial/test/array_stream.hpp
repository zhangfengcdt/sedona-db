
#include <string>
#include <vector>

#include "geoarrow/geoarrow.hpp"
#include "gpuspatial_testing.hpp"
#include "nanoarrow/nanoarrow.hpp"

namespace gpuspatial {

void ArrayStreamFromWKT(const std::vector<std::vector<std::string>>& batches,
                        enum GeoArrowType type, struct ArrowArrayStream* out);

void ArrayStreamFromIpc(const std::string& filename, std::string geometry_column,
                        struct ArrowArrayStream* out);

}  // namespace gpuspatial
