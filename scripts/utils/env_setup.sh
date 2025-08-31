#!/bin/bash

# disable hugepage
sudo sh -c "echo 0 > /proc/sys/vm/nr_hugepages"

# set cpu frequency to performance mode
sudo cpupower frequency-set --governor performance

# flush cache
sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'
