#!/bin/bash
warmup=0
runtime=180
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
hot_data_start=0
hot_range_count=4

source ./utils/env_setup.sh

rm -rf $builddir
mkdir -p $builddir
cd $builddir
cmake -DCMAKE_BUILD_TYPE=release ..
make -j 32
cd $cur_dir
mkdir -p $output_dir

for target in masstree
do
    for cxl_percentage in 90
    do
        if [ ! -f $output_dir/${target}-dynamic-hot-range-update-heavy-${fgn}-${size}-${cxl_percentage}.txt ];then
            echo "------[Overall] dynamic-hot-range update-heavy origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
            echo "$builddir/dynamic_hot_range -a 0.5 -b 0.5 --target $target --runtime $runtime --warmup $warmup --fg $fgn --bg $bgn --table-size $size"
            ./$builddir/dynamic_hot_range \
                -a 0.5 \
                -b 0.5 \
                --target $target \
                --runtime $runtime \
                --warmup $warmup \
                --fg $fgn \
                --bg $bgn \
                --table-size $size --cxl-percentage $cxl_percentage \
                --hot-data-ratio $hot_data_ratio --hot-query-ratio $hot_query_ratio \
                --hot-data-start $hot_data_start --hot-range-count $hot_range_count > $output_dir/${target}-dynamic-hot-range-update-heavy-${fgn}-${size}-${cxl_percentage}.txt
        fi

        if [ ! -f $output_dir/${target}-dynamic-hot-range-read-mostly-${fgn}-${size}-${cxl_percentage}.txt ];then
            echo "------[Overall] dynamic-hot-range read-mostly origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
            ./$builddir/dynamic_hot_range \
                -a 0.9 \
                -b 0.1 \
                --target $target \
                --runtime $runtime \
                --warmup $warmup \
                --fg $fgn \
                --bg $bgn \
                --table-size $size --cxl-percentage $cxl_percentage \
                --hot-data-ratio $hot_data_ratio --hot-query-ratio $hot_query_ratio \
                --hot-data-start $hot_data_start --hot-range-count $hot_range_count > $output_dir/${target}-dynamic-hot-range-read-mostly-${fgn}-${size}-${cxl_percentage}.txt
        fi

        if [ ! -f $output_dir/${target}-dynamic-hot-range-read-only-${fgn}-${size}-${cxl_percentage}.txt ];then
            echo "------[Overall] dynamic-hot-range read-only origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
            ./$builddir/dynamic_hot_range \
                -a 1 \
                --target $target \
                --runtime $runtime \
                --warmup $warmup \
                --fg $fgn \
                --bg $bgn \
                --table-size $size --cxl-percentage $cxl_percentage \
                --hot-data-ratio $hot_data_ratio --hot-query-ratio $hot_query_ratio \
                --hot-data-start $hot_data_start --hot-range-count $hot_range_count > $output_dir/${target}-dynamic-hot-range-read-only-${fgn}-${size}-${cxl_percentage}.txt
        fi 
    done
done


