#include "flags.hpp"

DEFINE_string(build_file, "", "file path of build side (S3 URL or Local)");
DEFINE_string(stream_file, "", "file path of stream side (S3 URL or Local)");
DEFINE_string(output_file, "", "file path of output side (local path, optional)");
DEFINE_string(execution, "rt/geos", "Using which method to run spatial joins");
DEFINE_int32(batch_size, 262144, "Number of records to read per batch with Arrow");
DEFINE_int32(limit, -1, "Limit reading how many stream files. -1 means no limit");
DEFINE_double(load_factor, 1e-4, "");
DEFINE_int32(
    parallelism, 1,
    "Number of threads calling PushStream. -1 indicates using all available cores");
DEFINE_string(json_file, "", "Write running statistics to a json file");
