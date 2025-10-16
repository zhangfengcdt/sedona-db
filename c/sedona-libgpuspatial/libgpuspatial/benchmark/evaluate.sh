#!/usr/bin/env bash

# Function to resolve the script path
get_script_dir() {
  local source="${BASH_SOURCE[0]}"
  while [ -h "$source" ]; do
    local dir
    dir=$(dirname "$source")
    source=$(readlink "$source")
    [[ $source != /* ]] && source="$dir/$source"
  done
  echo "$(cd -P "$(dirname "$source")" >/dev/null 2>&1 && pwd)"
}

script_dir=$(get_script_dir)
source "${script_dir}/common.sh"
log_dir="${script_dir}/logs"


function default() {
  exec=$1

  for S3_POLYGON in "${S3_POLYGON_DATASETS[@]}"; do
    for S3_POINT in "${S3_POINT_DATASETS[@]}"; do
      polygon_name=$(basename "$S3_POLYGON")
      point_name=$(basename "$S3_POINT")

      parallelism=$(nproc)
      batch_size=$DEFAULT_BATCH_SIZE
      log="${log_dir}/default/${polygon_name}_${point_name}_${exec}.json"

      if [[ -f "$log" ]]; then
        echo "Skip $log"
      else
        echo "${log}" | xargs dirname | xargs mkdir -p
        $BENCHMARK_ROOT/benchmark \
            -build_file $S3_POLYGON \
            -stream_file $S3_POINT \
            -execution $exec \
            -limit $POINT_FILES_LIMIT \
            -parallelism $parallelism \
            -batch_size $batch_size \
            -json_file "$log"
      fi
    done
  done
}

function vary_parallelism() {
  exec=$1
  batch_size=$2

  for S3_POLYGON in "${S3_POLYGON_DATASETS[@]}"; do
    polygon_name=$(basename "$S3_POLYGON")

    parallelism=1
    MAX_PROCESSORS=$(nproc)
    while (( parallelism <= MAX_PROCESSORS )); do
        log="${log_dir}/vary_parallelism/${polygon_name}_${exec}_batch_size_${batch_size}_parallelism_${parallelism}.json"

        if [[ -f "$log" ]]; then
          echo "Skip $log"
        else
          echo "${log}" | xargs dirname | xargs mkdir -p
          $BENCHMARK_ROOT/benchmark \
              -build_file $S3_POLYGON \
              -stream_file $S3_POINT \
              -execution $exec \
              -limit $POINT_FILES_LIMIT \
              -parallelism $parallelism \
              -batch_size $batch_size \
              -json_file $log
        fi
        parallelism=$(( parallelism * 2 ))
    done
    done
}

function vary_batch_size() {
  exec=$1
  parallelism=$2

  for S3_POLYGON in "${S3_POLYGON_DATASETS[@]}"; do
      polygon_name=$(basename "$S3_POLYGON")

      for batch_size in 128 256 512 1024 2048 4096 16384 32768 65536 131072 262144 524288; do
        log="${log_dir}/vary_batch_size/${polygon_name}_${exec}_batch_size_${batch_size}_parallelism_${parallelism}.json"

        if [[ -f "$log" ]]; then
          echo "Skip $log"
        else
          echo "${log}" | xargs dirname | xargs mkdir -p
          $BENCHMARK_ROOT/benchmark \
              -build_file $S3_POLYGON \
              -stream_file $S3_POINT \
              -execution $exec \
              -limit $POINT_FILES_LIMIT \
              -parallelism $parallelism \
              -batch_size $batch_size \
              -json_file "$log"
        fi
      done
  done
}

if nvidia-smi &> /dev/null; then
#    vary_parallelism "rt" $DEFAULT_BATCH_SIZE
#    vary_batch_size "rt" 1 # number of threads
#    vary_batch_size "rt" $(nproc)
#    vary_parallelism "rt_filter" $DEFAULT_BATCH_SIZE
#    vary_batch_size "rt_filter" 1 # number of threads
#    vary_batch_size "rt_filter" $(nproc)
    default "rt"
else
#    vary_parallelism "geos" $DEFAULT_BATCH_SIZE
#    vary_batch_size "geos" 1 # number of threads
#    vary_batch_size "geos" $(nproc)

#    vary_parallelism "boost" $DEFAULT_BATCH_SIZE
#    vary_batch_size "boost" 1 # number of threads
#    vary_batch_size "boost" $(nproc)
    default "geos"
    default "boost"
fi
