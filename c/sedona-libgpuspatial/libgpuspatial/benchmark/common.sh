#!/usr/bin/env bash

export BENCHMARK_ROOT="/home/ubuntu/.cache/clion/gpuspatial-main/build"
export S3_POLYGON_DATASETS=("wherobots-benchmark-prod/data/3rdparty-bench/postal-codes-sorted" "wherobots-benchmark-prod/data/sedona-db/california/BE/overture-buildings")
export S3_POINT_DATASETS=("wherobots-benchmark-prod/data/sedona-db/california/BE/nodes" "wherobots-benchmark-prod/data/3rdparty-bench/osm-nodes-large-sorted-corrected")
export POINT_FILES_LIMIT=10
export DEFAULT_BATCH_SIZE=262144