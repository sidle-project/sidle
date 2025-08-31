#!/bin/bash
warmup=0
runtime=30
builddir=$1
output_dir=$2
cur_dir=$(pwd)
bgn=0
fgn=28
size=50000000
with_insert_size=50000000
zipfian_theta=0.99
hot_data_ratio=5
hot_query_ratio=90
hot_data_start=5

source ./utils/env_setup.sh

rm -rf $builddir
mkdir -p $builddir
cd $builddir
cmake -DCMAKE_BUILD_TYPE=release ..
make -j 32
cd $cur_dir
mkdir -p $output_dir

masstree_basic_memory_usage=66
masstree_with_insert_memory_usage=120

for target in masstree
do
    for cxl_percentage in 90
    do
        local_memory_usage=$masstree_basic_memory_usage
        if [ ! -f $output_dir/${target}-skewed-update-heavy-${fgn}-${size}-${cxl_percentage}.txt ];then
            echo "------[Overall] skewed_partition update-heavy origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
            echo "$builddir/skewed_partition -a 0.5 -b 0.5 --target $target --runtime $runtime --warmup $warmup --fg $fgn --bg $bgn --table-size $size"
            ./$builddir/skewed_partition \
                -a 0.5 \
                -b 0.5 \
                --target $target \
                --runtime $runtime \
                --warmup $warmup \
                --fg $fgn \
                --bg $bgn \
                --table-size $size --cxl-percentage $cxl_percentage \
                --hot-data-ratio $hot_data_ratio --hot-query-ratio $hot_query_ratio \
                --hot-data-start $hot_data_start --max-local-memory-usage $local_memory_usage > $output_dir/${target}-skewed-update-heavy-${fgn}-${size}-${cxl_percentage}.txt
        fi

        if [ ! -f $output_dir/${target}-skewed-read-mostly-${fgn}-${size}-${cxl_percentage}.txt ];then
            echo "------[Overall] skewed_partition read-mostly origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
            ./$builddir/skewed_partition \
                -a 0.9 \
                -b 0.1 \
                --target $target \
                --runtime $runtime \
                --warmup $warmup \
                --fg $fgn \
                --bg $bgn \
                --table-size $size --cxl-percentage $cxl_percentage \
                --hot-data-ratio $hot_data_ratio --hot-query-ratio $hot_query_ratio \
                --hot-data-start $hot_data_start --max-local-memory-usage $local_memory_usage > $output_dir/${target}-skewed-read-mostly-${fgn}-${size}-${cxl_percentage}.txt
        fi

        if [ ! -f $output_dir/${target}-skewed-read-only-${fgn}-${size}-${cxl_percentage}.txt ];then
            echo "------[Overall] skewed_partition read-only origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
            ./$builddir/skewed_partition \
                -a 1 \
                --target $target \
                --runtime $runtime \
                --warmup $warmup \
                --fg $fgn \
                --bg $bgn \
                --table-size $size --cxl-percentage $cxl_percentage \
                --hot-data-ratio $hot_data_ratio --hot-query-ratio $hot_query_ratio \
                --hot-data-start $hot_data_start --max-local-memory-usage $local_memory_usage > $output_dir/${target}-skewed-read-only-${fgn}-${size}-${cxl_percentage}.txt
        fi 

        local_memory_usage=$masstree_with_insert_memory_usage
        if [ ! -f $output_dir/${target}-skewed-with-insert-${fgn}-${size}-${cxl_percentage}.txt ];then
            echo "------[Overall] skewed_partition with insert origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
            ./$builddir/skewed_partition_dynamic \
                -a 0.95 \
                -b 0.05 \
                --target $target \
                --runtime 30 \
                --warmup $warmup \
                --fg $fgn \
                --bg $bgn \
                --table-size $with_insert_size --cxl-percentage $cxl_percentage \
                --hot-data-ratio $hot_data_ratio --hot-query-ratio $hot_query_ratio \
                --hot-data-start $hot_data_start --max-local-memory-usage $local_memory_usage > $output_dir/${target}-skewed-with-insert-${fgn}-${size}-${cxl_percentage}.txt
        fi
    done
done

