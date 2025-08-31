#!/bin/bash
warmup=0
runtime=60
builddir=$1
output_dir=$2
cur_dir=$(pwd)
bgn=0
fgn=28
zipfian_theta=0.99

size_list=('10000000' '20000000' '50000000' '100000000' '200000000' '400000000')
basic_memory_usage_list=('89' '100' '390' '538' '651' '800')
read_latest_list=('124' '263' '390' '538' '819' '1000')
length=${#size_list[@]}

source ./utils/env_setup.sh

rm -rf $builddir
mkdir -p $builddir
cd $builddir
cmake -DCMAKE_BUILD_TYPE=release ..
make -j 32
cd $cur_dir
mkdir -p $output_dir

for target in art
do 
    for cxl_percentage in 80
    do
        for ((i=0; i<$length; i++)); do
            if [ ! -f $output_dir/${target}-update-heavy-${fgn}-${size_list[$i]}-${cxl_percentage}.txt ];then
                echo "------[Overall] ycsb update-heavy origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
                echo "$builddir/rw_ycsb_origin -a 0.5 -b 0.5 --target $target --runtime $runtime --warmup $warmup --fg $fgn --bg $bgn --table-size ${size_list[$i]}"
                ./$builddir/rw_ycsb_origin \
                    -a 0.5 \
                    -b 0.5 \
                    --target $target \
                    --runtime $runtime \
                    --warmup $warmup \
                    --fg $fgn \
                    --bg $bgn \
                    --table-size ${size_list[$i]} --cxl-percentage $cxl_percentage \
                    --max-local-memory-usage ${basic_memory_usage_list[$i]} > $output_dir/${target}-update-heavy-${fgn}-${size_list[$i]}-${cxl_percentage}.txt
            fi

            if [ ! -f $output_dir/${target}-read-mostly-${fgn}-${size_list[$i]}-${cxl_percentage}.txt ];then
                echo "------[Overall] ycsb read-mostly origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
                ./$builddir/rw_ycsb_origin \
                    -a 0.9 \
                    -b 0.1 \
                    --target $target \
                    --runtime $runtime \
                    --warmup $warmup \
                    --fg $fgn \
                    --bg $bgn \
                    --table-size ${size_list[$i]} --cxl-percentage $cxl_percentage \
                    --max-local-memory-usage ${basic_memory_usage_list[$i]} > $output_dir/${target}-read-mostly-${fgn}-${size_list[$i]}-${cxl_percentage}.txt
            fi

            if [ ! -f $output_dir/${target}-read-only-${fgn}-${size_list[$i]}-${cxl_percentage}.txt ];then
                echo "------[Overall] ycsb read-only origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
                ./$builddir/rw_ycsb_origin \
                    -a 1 \
                    --target $target \
                    --runtime $runtime \
                    --warmup $warmup \
                    --fg $fgn \
                    --bg $bgn \
                    --table-size ${size_list[$i]} --cxl-percentage $cxl_percentage \
                    --max-local-memory-usage ${basic_memory_usage_list[$i]} > $output_dir/${target}-read-only-${fgn}-${size_list[$i]}-${cxl_percentage}.txt
            fi

            if [ ! -f $output_dir/${target}-read-latest-${fgn}-${size_list[$i]}-${cxl_percentage}.txt ];then
                echo "------[Overall] ycsb read-latest origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
                ./$builddir/read_latest_origin \
                    --target $target \
                    --runtime 30 \
                    --warmup $warmup \
                    --fg $fgn \
                    --bg $bgn \
                    --table-size ${size_list[$i]} --cxl-percentage $cxl_percentage \
                    --max-local-memory-usage ${read_latest_list[$i]} > $output_dir/${target}-read-latest-${fgn}-${size_list[$i]}-${cxl_percentage}.txt
            fi

            if [ "$target" != "art" ]; then
                if [ ! -f $output_dir/${target}-short-ranges-${fgn}-${size_list[$i]}-${cxl_percentage}.txt ];then
                    echo "------[Overall] ycsb short-ranges origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
                    ./$builddir/short_range_origin \
                        --target $target \
                        --runtime 30 \
                        --warmup 0 \
                        --fg $fgn \
                        --bg $bgn \
                        --table-size ${size_list[$i]} --cxl-percentage $cxl_percentage \
                        --max-local-memory-usage ${short_range_list[$i]} > $output_dir/${target}-short-ranges-${fgn}-${size_list[$i]}-${cxl_percentage}.txt
                fi
            fi

            if [ ! -f $output_dir/${target}-read-modify-write-${fgn}-${size_list[$i]}-${cxl_percentage}.txt ];then
                echo "------[Overall] ycsb read-modify-write origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
                ./$builddir/read_modify_write_origin \
                    --target $target \
                    --runtime $runtime \
                    --warmup $warmup \
                    --fg $fgn \
                    --bg $bgn \
                    --table-size ${size_list[$i]} --cxl-percentage $cxl_percentage \
                    --max-local-memory-usage ${basic_memory_usage_list[$i]} > $output_dir/${target}-read-modify-write-${fgn}-${size_list[$i]}-${cxl_percentage}.txt
            fi
        done
    done
done


