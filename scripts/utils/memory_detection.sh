#!/bin/bash

MAX_MEMORY=0
process_name=$1
file_name=$2
PID=$(pgrep -f /"$process_name")


if [[ -n $PID ]]; then
    echo "process /$process_name 's pid is $PID"
else
    echo "cannot find /$process_name"
    exit 1
fi

while ps -p $PID > /dev/null; do
    MEMORY=$(ps -o rss= -p $PID)

    if [[ "$MEMORY" -gt "$MAX_MEMORY" ]]; then
        MAX_MEMORY=$MEMORY
    fi
    
    sleep 0.1
done

echo "memory usage: $((MAX_MEMORY / 1024))MB" >> $file_name
