#ifndef GPUSPATIAL_BENCHMARK_RUN_BENCHMARK_CUH
#define GPUSPATIAL_BENCHMARK_RUN_BENCHMARK_CUH
#include "config.hpp"

namespace gpuspatial {
void RunQueries(BenchmarkConfig& config);
void RunQueriesMultithread(BenchmarkConfig& config);
}

#endif  // GPUSPATIAL_BENCHMARK_RUN_BENCHMARK_CUH
