#!/bin/bash
warmup=0
runtime=30
builddir=$1
output_dir=$2
lat_output_dir=${output_dir}_lat
cur_dir=$(pwd)
bgn=0
fgn=28
load_data_dir=../alibaba/load_data
trace_dir=../alibaba/trace_data
cxl_percentage=80
target=art

source ./utils/env_setup.sh

rm -rf $builddir
mkdir -p $builddir
cd $builddir
cmake -DCMAKE_BUILD_TYPE=release ..
make -j 32
cd $cur_dir
mkdir -p $output_dir
mkdir -p $lat_output_dir

# 7
./$builddir/real_world_trace --target ${target} --runtime $runtime --warmup $warmup --fg $fgn --bg $bgn --file $load_data_dir/7.in --second-file $trace_dir/input_7_0.csv --cxl-percentage $cxl_percentage --max-local-memory-usage 306 > $output_dir/7_baseline_${cxl_percentage}.txt

./$builddir/real_world_trace_lat --target ${target} --runtime $runtime --warmup $warmup --fg $fgn --bg $bgn --file $load_data_dir/7.in --second-file $trace_dir/input_7_0.csv --cxl-percentage $cxl_percentage --max-local-memory-usage 306 > $lat_output_dir/7_baseline_${cxl_percentage}.txt

# 40
./$builddir/real_world_trace --target ${target} --runtime $runtime --warmup $warmup --fg $fgn --bg $bgn --file $load_data_dir/40.in --second-file $trace_dir/input_40_0.csv --cxl-percentage $cxl_percentage --max-local-memory-usage 359 > $output_dir/40_baseline_${cxl_percentage}.txt

./$builddir/real_world_trace_lat --target ${target} --runtime $runtime --warmup $warmup --fg $fgn --bg $bgn --file $load_data_dir/40.in --second-file $trace_dir/input_40_0.csv --cxl-percentage $cxl_percentage --max-local-memory-usage 359 > $lat_output_dir/40_baseline_${cxl_percentage}.txt