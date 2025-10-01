#ifndef GPUSPATIAL_BENCHMARK_CONFIG_HPP
#define GPUSPATIAL_BENCHMARK_CONFIG_HPP
#include <string>

namespace gpuspatial {
enum class FileType { kArrowIPC, kParquet };

enum class Execution {
  kBoost,
  kGEOS,
  kRT,
  kRT_Filter,
};

struct BenchmarkConfig {
  std::string build_file;
  std::string stream_file;
  std::string output_file;
  std::string json_file;
  std::string ptx_root;
  int64_t batch_size;
  double load_factor;
  FileType file_type;
  Execution execution;
  int limit;
  int parallelism;
};
}  // namespace gpuspatial

#endif  // GPUSPATIAL_BENCHMARK_CONFIG_HPP
