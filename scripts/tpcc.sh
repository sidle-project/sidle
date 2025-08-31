#!/bin/bash
warmup=0
runtime=30
builddir=$1
output_dir=$2
cur_dir=$(pwd)
bgn=0
fgn=28

source ./utils/env_setup.sh

rm -rf $builddir
mkdir -p $builddir
cd $builddir
cmake -DCMAKE_BUILD_TYPE=release ..
make -j 32
cd $cur_dir
mkdir -p $output_dir


for target in masstree art
do 
  wn=$((${fgn}*8))
  ./$builddir/tpcc --warehouse-num $wn \
            --warmup $warmup \
            --runtime $runtime \
            -n 1 \
            --fg $fgn \
            --bg $bgn -o 80 > $output_dir/tpcc_${target}.txt
done