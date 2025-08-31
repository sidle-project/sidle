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

for target in masstree
do
    for cxl_percentage in 80
    do  
        wakeup_interval=('100 1000' '200 1000' '500 2000' '1000 3000' '1500 4500')
        for t in "${wakeup_interval[@]}"
        do
            interval=($t)
            basic_worker_wakeup_interval=${interval[0]}
            cooler_wakeup_interval=${interval[1]}
            if [ ! -f $output_dir/${target}-update-heavy-${fgn}-${size}-${cxl_percentage}-${basic_worker_wakeup_interval}-${cooler_wakeup_interval}.txt ];then
                echo "------[Overall] ycsb update-heavy origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
                echo "$builddir/rw_ycsb_origin -a 0.5 -b 0.5 --target $target --runtime $runtime --warmup $warmup --fg $fgn --bg $bgn --table-size $size"
                ./$builddir/rw_ycsb_origin \
                    -a 0.5 \
                    -b 0.5 \
                    --target $target \
                    --runtime $runtime \
                    --warmup $warmup \
                    --fg $fgn \
                    --bg $bgn \
                    --table-size $size --cxl-percentage $cxl_percentage --basic-worker-wakeup-interval $basic_worker_wakeup_interval \
                --cooler-wakeup-interval $cooler_wakeup_interval > $output_dir/${target}-update-heavy-${fgn}-${size}-${cxl_percentage}-${basic_worker_wakeup_interval}-${cooler_wakeup_interval}.txt
            fi

            if [ ! -f $output_dir/${target}-read-mostly-${fgn}-${size}-${cxl_percentage}-${basic_worker_wakeup_interval}-${cooler_wakeup_interval}.txt ];then
                echo "------[Overall] ycsb read-mostly origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
                ./$builddir/rw_ycsb_origin \
                    -a 0.9 \
                    -b 0.1 \
                    --target $target \
                    --runtime $runtime \
                    --warmup $warmup \
                    --fg $fgn \
                    --bg $bgn \
                    --table-size $size --cxl-percentage $cxl_percentage --basic-worker-wakeup-interval $basic_worker_wakeup_interval \
                --cooler-wakeup-interval $cooler_wakeup_interval > $output_dir/${target}-read-mostly-${fgn}-${size}-${cxl_percentage}-${basic_worker_wakeup_interval}-${cooler_wakeup_interval}.txt
            fi

            if [ ! -f $output_dir/${target}-read-only-${fgn}-${size}-${cxl_percentage}-${basic_worker_wakeup_interval}-${cooler_wakeup_interval}.txt ];then
                echo "------[Overall] ycsb read-only origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
                ./$builddir/rw_ycsb_origin \
                    -a 1 \
                    --target $target \
                    --runtime $runtime \
                    --warmup $warmup \
                    --fg $fgn \
                    --bg $bgn \
                    --table-size $size --cxl-percentage $cxl_percentage --basic-worker-wakeup-interval $basic_worker_wakeup_interval \
                --cooler-wakeup-interval $cooler_wakeup_interval > $output_dir/${target}-read-only-${fgn}-${size}-${cxl_percentage}-${basic_worker_wakeup_interval}-${cooler_wakeup_interval}.txt
            fi

            if [ ! -f $output_dir/${target}-read-latest-${fgn}-${size}-${cxl_percentage}-${basic_worker_wakeup_interval}-${cooler_wakeup_interval}.txt ];then
                echo "------[Overall] ycsb read-latest origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
                ./$builddir/read_latest_origin \
                    --target $target \
                    --runtime 30 \
                    --warmup $warmup \
                    --fg $fgn \
                    --bg $bgn \
                    --table-size $size --cxl-percentage $cxl_percentage --basic-worker-wakeup-interval $basic_worker_wakeup_interval \
                --cooler-wakeup-interval $cooler_wakeup_interval > $output_dir/${target}-read-latest-${fgn}-${size}-${cxl_percentage}-${basic_worker_wakeup_interval}-${cooler_wakeup_interval}.txt
            fi

            if [ ! -f $output_dir/${target}-short-ranges-${fgn}-${size}-${cxl_percentage}-${basic_worker_wakeup_interval}-${cooler_wakeup_interval}.txt ];then
                echo "------[Overall] ycsb short-ranges origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
                ./$builddir/short_range_origin \
                    --target $target \
                    --runtime 30 \
                    --warmup 0 \
                    --fg $fgn \
                    --bg $bgn \
                    --table-size $size --cxl-percentage $cxl_percentage --basic-worker-wakeup-interval $basic_worker_wakeup_interval \
                --cooler-wakeup-interval $cooler_wakeup_interval > $output_dir/${target}-short-ranges-${fgn}-${size}-${cxl_percentage}-${basic_worker_wakeup_interval}-${cooler_wakeup_interval}.txt
            fi

            if [ ! -f $output_dir/${target}-read-modify-write-${fgn}-${size}-${cxl_percentage}-${basic_worker_wakeup_interval}-${cooler_wakeup_interval}.txt ];then
                echo "------[Overall] ycsb read-modify-write origin, target=$target, fgn=$fgn, bgn=$bgn, cxl_percentage=$cxl_percentage"
                ./$builddir/read_modify_write_origin \
                    --target $target \
                    --runtime $runtime \
                    --warmup $warmup \
                    --fg $fgn \
                    --bg $bgn \
                    --table-size $size --cxl-percentage $cxl_percentage --basic-worker-wakeup-interval $basic_worker_wakeup_interval \
                --cooler-wakeup-interval $cooler_wakeup_interval > $output_dir/${target}-read-modify-write-${fgn}-${size}-${cxl_percentage}-${basic_worker_wakeup_interval}-${cooler_wakeup_interval}.txt
            fi
        done
    done
done


