#include <algorithm>
#include <cctype>
#include <filesystem>  // Requires C++17
#include <iostream>
#include <thread>

#include "config.hpp"
#include "flags.hpp"

#include "run_queries.cuh"
namespace TestUtils {
// Global variable to store the executable's directory.
// Alternatively, use a singleton or pass it through test fixtures.
std::filesystem::path g_executable_dir;

// Helper function to get the full path to a test data file
std::string GetTestDataPath(const std::string& relative_path_to_file) {
  if (g_executable_dir.empty()) {
    // Fallback or error if g_executable_dir was not initialized.
    // This indicates an issue with main() or test setup.
    throw std::runtime_error(
        "Executable directory not set. Ensure TestUtils::Initialize is called from main().");
  }
  std::filesystem::path full_path = g_executable_dir / relative_path_to_file;
  return full_path.string();
}

// Call this from main()
void Initialize(const char* argv0) {
  if (argv0 == nullptr) {
    // This should ideally not happen if called from main
    g_executable_dir = std::filesystem::current_path();  // Fallback, less reliable
    std::cerr
        << "Warning: argv[0] was null. Using current_path() as executable directory."
        << std::endl;
    return;
  }
  // Get the absolute path to the executable.
  // std::filesystem::absolute can correctly interpret argv[0] whether it's
  // a full path, relative path, or just the executable name (if in PATH).
  std::filesystem::path exe_path =
      std::filesystem::absolute(std::filesystem::path(argv0));
  g_executable_dir = exe_path.parent_path();
  std::cout << "Test executable directory initialized to: " << g_executable_dir
            << std::endl;
}

}  // namespace TestUtils

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("Usage: ");
  if (argc == 1) {
    gflags::ShowUsageWithFlags(argv[0]);
    exit(1);
  }
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  TestUtils::Initialize(argv[0]);  // Initialize our utility

  gpuspatial::BenchmarkConfig benchmark_config;
  auto to_lower = [](const std::string& str) {
    std::string lower_data;
    lower_data.resize(str.size());  // Allocate memory for the new string

    std::transform(str.begin(), str.end(), lower_data.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return lower_data;
  };

  benchmark_config.build_file = FLAGS_build_file;
  benchmark_config.stream_file = FLAGS_stream_file;
  benchmark_config.output_file = FLAGS_output_file;
  benchmark_config.json_file = FLAGS_json_file;
  benchmark_config.load_factor = FLAGS_load_factor;

  std::string method = to_lower(FLAGS_execution);

  if (method == "boost") {
    benchmark_config.execution = gpuspatial::Execution::kBoost;
  } else if (method == "rt") {
    benchmark_config.execution = gpuspatial::Execution::kRT;
  } else if (method == "rt_filter") {
    benchmark_config.execution = gpuspatial::Execution::kRT_Filter;
  } else if (method == "geos") {
    benchmark_config.execution = gpuspatial::Execution::kGEOS;
  } else {
    throw std::runtime_error("Unknown execution method.");
  }
  benchmark_config.ptx_root = TestUtils::GetTestDataPath("shaders_ptx");
  benchmark_config.batch_size = FLAGS_batch_size;
  benchmark_config.limit = FLAGS_limit;
  if (benchmark_config.limit == -1) {
    benchmark_config.limit = std::numeric_limits<int>::max();
  }
  benchmark_config.parallelism = FLAGS_parallelism;
  if (benchmark_config.parallelism == -1) {
    benchmark_config.parallelism = std::thread::hardware_concurrency();
  }

  if (!benchmark_config.output_file.empty()) {
    remove(benchmark_config.output_file.c_str());
  }

  RunQueries(benchmark_config);

  gflags::ShutDownCommandLineFlags();
}
