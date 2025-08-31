#!/bin/bash
warmup=0
runtime=60
builddir=$1
output_dir=$2
cur_dir=$(pwd)
bgn=0
fgn=28
size=50000000
zipfian_theta=0.99

source ./utils/env_setup.sh

rm -rf $builddir
mkdir -p $builddir
cd $builddir
cmake -DCMAKE_BUILD_TYPE=release ..
make -j 32
cd $cur_dir
mkdir -p $output_dir

basic_local_memory_usage=43
short_range_local_memory_usage=65
read_latest_local_memory_usage=125
hot_data_percentages=(5 5 10 30 60)

id=0

for target in masstree
do
    for cxl_percentage in 90 80 60 40 20 
    do
        multiplier=$(echo "scale=0; (100 - $cxl_percentage) / 10" | bc)
        echo "multiplier=$multiplier"
        echo "id=$id"
        if [ ! -f $output_dir/${target}-update-heavy-${fgn}-${size}-${cxl_percentage}.txt ];then
            echo "------[Overall] ycsb update-heavy origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
            echo "$builddir/rw_ycsb_origin -a 0.5 -b 0.5 --target $target --runtime $runtime --warmup $warmup --fg $fgn --bg $bgn --table-size $size"
            local_memory_usage=$(echo "scale=0; $basic_local_memory_usage * $multiplier" | bc)
            hot_data_percentage=${hot_data_percentages[$id]}
            ./$builddir/rw_ycsb_origin \
                -a 0.5 \
                -b 0.5 \
                --target $target \
                --runtime $runtime \
                --warmup $warmup \
                --fg $fgn \
                --bg $bgn \
                --table-size $size --cxl-percentage $cxl_percentage \
                --max-local-memory-usage $local_memory_usage --hot-percentage-lower-bound $hot_data_percentage > $output_dir/${target}-update-heavy-${fgn}-${size}-${cxl_percentage}.txt
        fi

        if [ ! -f $output_dir/${target}-read-mostly-${fgn}-${size}-${cxl_percentage}.txt ];then
            echo "------[Overall] ycsb read-mostly origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
            local_memory_usage=$(echo "scale=0; $basic_local_memory_usage * $multiplier" | bc)
            hot_data_percentage=${hot_data_percentages[$id]}
            ./$builddir/rw_ycsb_origin \
                -a 0.9 \
                -b 0.1 \
                --target $target \
                --runtime $runtime \
                --warmup $warmup \
                --fg $fgn \
                --bg $bgn \
                --table-size $size --cxl-percentage $cxl_percentage \
                --max-local-memory-usage $local_memory_usage --hot-percentage-lower-bound $hot_data_percentage > $output_dir/${target}-read-mostly-${fgn}-${size}-${cxl_percentage}.txt
        fi

        if [ ! -f $output_dir/${target}-read-only-${fgn}-${size}-${cxl_percentage}.txt ];then
            echo "------[Overall] ycsb read-only origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
            local_memory_usage=$(echo "scale=0; $basic_local_memory_usage * $multiplier" | bc)
            hot_data_percentage=${hot_data_percentages[$id]}
            ./$builddir/rw_ycsb_origin \
                -a 1 \
                --target $target \
                --runtime $runtime \
                --warmup $warmup \
                --fg $fgn \
                --bg $bgn \
                --table-size $size --cxl-percentage $cxl_percentage \
                --max-local-memory-usage $local_memory_usage --hot-percentage-lower-bound $hot_data_percentage > $output_dir/${target}-read-only-${fgn}-${size}-${cxl_percentage}.txt
        fi

        if [ ! -f $output_dir/${target}-read-latest-${fgn}-${size}-${cxl_percentage}.txt ];then
            echo "------[Overall] ycsb read-latest origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
            local_memory_usage=$(echo "scale=0; $read_latest_local_memory_usage * $multiplier" | bc)
            hot_data_percentage=${hot_data_percentages[$id]}
            ./$builddir/read_latest_origin \
                --target $target \
                --runtime 30 \
                --warmup $warmup \
                --fg $fgn \
                --bg $bgn \
                --table-size $size --cxl-percentage $cxl_percentage \
                --max-local-memory-usage $local_memory_usage --hot-percentage-lower-bound $hot_data_percentage > $output_dir/${target}-read-latest-${fgn}-${size}-${cxl_percentage}.txt
        fi

        if [ ! -f $output_dir/${target}-short-ranges-${fgn}-${size}-${cxl_percentage}.txt ];then
            echo "------[Overall] ycsb short-ranges origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
            local_memory_usage=$(echo "scale=0; $short_range_local_memory_usage * $multiplier" | bc)
            hot_data_percentage=${hot_data_percentages[$id]}
            ./$builddir/short_range_origin \
                --target $target \
                --runtime 30 \
                --warmup $warmup \
                --fg $fgn \
                --bg $bgn \
                --table-size $size --cxl-percentage $cxl_percentage \
                --max-local-memory-usage $local_memory_usage --hot-percentage-lower-bound $hot_data_percentage > $output_dir/${target}-short-ranges-${fgn}-${size}-${cxl_percentage}.txt
        fi

        if [ ! -f $output_dir/${target}-read-modify-write-${fgn}-${size}-${cxl_percentage}.txt ];then
            echo "------[Overall] ycsb read-modify-write origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
            local_memory_usage=$(echo "scale=0; $basic_local_memory_usage * $multiplier" | bc)
            hot_data_percentage=${hot_data_percentages[$id]}
            ./$builddir/read_modify_write_origin \
                --target $target \
                --runtime $runtime \
                --warmup $warmup \
                --fg $fgn \
                --bg $bgn \
                --table-size $size --cxl-percentage $cxl_percentage \
                --max-local-memory-usage $local_memory_usage --hot-percentage-lower-bound $hot_data_percentage > $output_dir/${target}-read-modify-write-${fgn}-${size}-${cxl_percentage}.txt
        fi
        id=$((id + 1))
    done
done


