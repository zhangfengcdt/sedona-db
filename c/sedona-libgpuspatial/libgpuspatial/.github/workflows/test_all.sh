#!/usr/bin/env bash
set -e

nvidia-smi

./loader_test
./index_test
./joiner_test
./c_wrapper_test
./relate_test

#max_files=5
#build_file="wherobots-benchmark-prod/data/3rdparty-bench/postal-codes-sorted"
#stream_file="wherobots-benchmark-prod/data/3rdparty-bench/osm-nodes-large-sorted-corrected"
#
#output_file_answer="answer_limit_${max_files}"
#output_file_result="result_limit_${max_files}"
#
#function check_result() {
#  conf=$1
#  cat "$output_file_result" | sort -n > "${output_file_result}.sorted"
#  mv "${output_file_result}.sorted" "$output_file_result"
#
#    if diff -q "$output_file_answer" "$output_file_result" &>/dev/null; then
#      echo "Result is correct, config: $conf"
#    else
#      echo "Wrong result, config: $conf"
#      exit 1
#    fi
#}
#
## Generate reference
#./benchmark -build_file "$build_file" \
#      -stream_file "$stream_file" \
#      -execution geos \
#      -limit $max_files \
#      -output_file "$output_file_answer"
#cat "$output_file_answer" | sort -n > "${output_file_answer}.sorted"
#mv "${output_file_answer}.sorted" "${output_file_answer}"
#
#
#for execution in rt geos boost; do
#  for parallelism in $(seq 1 "$(nproc)"); do
#    ./benchmark -build_file "$build_file" \
#      -stream_file "$stream_file" \
#      -execution $execution \
#      -limit $max_files \
#      -parallelism $parallelism \
#      -output_file "$output_file_result"
#    check_result "execution: ${execution}, parallelism: ${parallelism}"
#  done
#done
