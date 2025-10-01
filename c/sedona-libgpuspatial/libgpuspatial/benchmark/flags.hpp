#ifndef GPUSPATIAL_BENCHMARK_FLAGS_HPP
#define GPUSPATIAL_BENCHMARK_FLAGS_HPP
#include <gflags/gflags.h>

DECLARE_string(build_file);
DECLARE_string(stream_file);
DECLARE_string(output_file);
DECLARE_string(execution);
DECLARE_int32(batch_size);
DECLARE_int32(limit);
DECLARE_int32(parallelism);
DECLARE_string(json_file);
DECLARE_double(load_factor);
#endif // GPUSPATIAL_BENCHMARK_FLAGS_HPP
