#!/bin/bash
warmup=0
runtime=30
builddir=$1
output_dir=$2
cur_dir=$(pwd)
bgn=0
fgn=28
size=50000000

source ./utils/env_setup.sh

rm -rf $builddir
mkdir -p $builddir
cd $builddir
cmake -DCMAKE_BUILD_TYPE=release ..
make -j 32
cd $cur_dir
mkdir -p $output_dir

local_memory_usage_list=(86 172 215 215 215 215)

id=0
for target in masstree
do
    for key_size in 8 16 20 24 32 40
    do
        for cxl_percentage in 80
        do
            local_memory_usage=${local_memory_usage_list[$id]}
            id=$((id+1))
            if [ ! -f $output_dir/${key_size}.txt ];then
                echo "------[Overall] key_length=$key_size, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
                echo "local_memory_usage=$local_memory_usage"
                ./$builddir/rw_mix_str${key_size} \
                    -a 0.9 \
                    -b 0.1 \
                    --target $target \
                    --runtime $runtime \
                    --warmup $warmup \
                    --fg $fgn \
                    --bg $bgn \
                    --table-size $size --cxl-percentage $cxl_percentage \
                    --max-local-memory-usage $local_memory_usage > $output_dir/${key_size}.txt
            fi
        done
    done
done

