#!/bin/bash
warmup=0
runtime=60
builddir=$1
output_dir=$2
is_latency=${3:-0}
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

art_basic_local_memory_usage=110
art_read_latest_local_memory_usage=263

for target in art
do
    for zipfian_theta in 0 0.5 0.75 0.9 0.99 1.25 1.5
    do
        for cxl_percentage in 80
        do
            multiplier=$(echo "scale=0; (100 - $cxl_percentage) / 20" | bc)
            echo "multiplier=$multiplier"
            if [ ! -f $output_dir/${target}-update-heavy-${fgn}-${size}-${cxl_percentage}.txt ];then
                echo "------[Overall] ycsb update-heavy origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
                echo "$builddir/rw_ycsb_origin -a 0.5 -b 0.5 --target $target --runtime $runtime --warmup $warmup --fg $fgn --bg $bgn --table-size $size"
                if [ "$target" == "art" ]; then
                    local_memory_usage=$art_basic_local_memory_usage
                else
                    local_memory_usage=$basic_local_memory_usage
                fi
                if [ "$is_latency" -eq 1 ]; then
                    proc="rw_ycsb_lat"
                else
                    proc="rw_ycsb_origin"
                fi
                echo "local_memory_usage=$local_memory_usage"
                ./$builddir/${proc} \
                    -a 0.5 \
                    -b 0.5 \
                    --target $target \
                    --runtime $runtime \
                    --warmup $warmup \
                    --fg $fgn \
                    --bg $bgn \
                    --table-size $size --cxl-percentage $cxl_percentage \
                    --max-local-memory-usage $local_memory_usage \
                    -u $zipfian_theta > $output_dir/${target}-update-heavy-${fgn}-${size}-${cxl_percentage}-${zipfian_theta}.txt
            fi

            if [ ! -f $output_dir/${target}-read-mostly-${fgn}-${size}-${cxl_percentage}.txt ];then
                echo "------[Overall] ycsb read-mostly origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
                if [ "$target" == "art" ]; then
                    local_memory_usage=$art_basic_local_memory_usage
                else
                    local_memory_usage=$basic_local_memory_usage
                fi
                if [ "$is_latency" -eq 1 ]; then
                    proc="rw_ycsb_lat"
                else
                    proc="rw_ycsb_origin"
                fi
                ./$builddir/${proc} \
                    -a 0.9 \
                    -b 0.1 \
                    --target $target \
                    --runtime $runtime \
                    --warmup $warmup \
                    --fg $fgn \
                    --bg $bgn \
                    --table-size $size --cxl-percentage $cxl_percentage \
                    --max-local-memory-usage $local_memory_usage \
                    -u $zipfian_theta > $output_dir/${target}-read-mostly-${fgn}-${size}-${cxl_percentage}-${zipfian_theta}.txt
            fi

            if [ ! -f $output_dir/${target}-read-only-${fgn}-${size}-${cxl_percentage}.txt ];then
                echo "------[Overall] ycsb read-only origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
                if [ "$target" == "art" ]; then
                    local_memory_usage=$art_basic_local_memory_usage
                else
                    local_memory_usage=$basic_local_memory_usage
                fi
                if [ "$is_latency" -eq 1 ]; then
                    proc="rw_ycsb_lat"
                else
                    proc="rw_ycsb_origin"
                fi
                ./$builddir/${proc} \
                    -a 1 \
                    --target $target \
                    --runtime $runtime \
                    --warmup $warmup \
                    --fg $fgn \
                    --bg $bgn \
                    --table-size $size --cxl-percentage $cxl_percentage \
                    --max-local-memory-usage $local_memory_usage \
                    -u $zipfian_theta > $output_dir/${target}-read-only-${fgn}-${size}-${cxl_percentage}-${zipfian_theta}.txt
            fi

            if [ "$target" != "art" ]; then
                if [ ! -f $output_dir/${target}-short-ranges-${fgn}-${size}-${cxl_percentage}.txt ];then
                    echo "------[Overall] ycsb short-ranges origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
                    local_memory_usage=$short_range_local_memory_usage
                    if [ "$is_latency" -eq 1 ]; then 
                        proc="short_range_lat"
                    else
                        proc="short_range_origin"
                    fi
                    ./$builddir/${proc} \
                        --target $target \
                        --runtime 30 \
                        --warmup $warmup \
                        --fg $fgn \
                        --bg $bgn \
                        --table-size $size --cxl-percentage $cxl_percentage \
                        --max-local-memory-usage $local_memory_usage \
                        -u $zipfian_theta > $output_dir/${target}-short-ranges-${fgn}-${size}-${cxl_percentage}-${zipfian_theta}.txt
                fi
            fi

            if [ ! -f $output_dir/${target}-read-modify-write-${fgn}-${size}-${cxl_percentage}.txt ];then
                echo "------[Overall] ycsb read-modify-write origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
                if [ "$target" == "art" ]; then
                    local_memory_usage=$art_basic_local_memory_usage
                else
                    local_memory_usage=$basic_local_memory_usage
                fi
                if [ "$is_latency" -eq 1 ]; then 
                    proc="read_modify_write_lat"
                else 
                    proc="read_modify_write_origin"
                fi
                ./$builddir/${proc} \
                    --target $target \
                    --runtime $runtime \
                    --warmup $warmup \
                    --fg $fgn \
                    --bg $bgn \
                    --table-size $size --cxl-percentage $cxl_percentage \
                    --max-local-memory-usage $local_memory_usage \
                    -u $zipfian_theta > $output_dir/${target}-read-modify-write-${fgn}-${size}-${cxl_percentage}-${zipfian_theta}.txt
            fi
        done
    done
done


