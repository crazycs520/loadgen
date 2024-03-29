#!/bin/bash

CONFIG_FILE="./sysbench.conf"
TABLES=50
TABLE_SIZE=10000000
THREADS=(25 50 100 200)
LOAD_TYPES=("oltp_point_select" "oltp_read_only" "oltp_read_write")



# parameter $1 is load type, like oltp_point_select,oltp_read_only,oltp_write_only, oltp_read_write
# parameter $2 is threads
sysbenchFn(){
    echo ">>>>>>> sysbench $1, threads: $2"
    sysbench --config-file=$CONFIG_FILE $1 --tables=$TABLES --table-size=$TABLE_SIZE --db-ps-mode=auto --rand-type=uniform --threads=$2 run
    echo "<<<<<<< sysbench $1, threads: $2\n\n"
}

#sysbench --config-file=./sysbench.conf oltp_point_select --tables=$TABLES --table-size=$TABLE_SIZE --threads=4  prepare
#sysbench --config-file=./sysbench.conf oltp_point_select --tables=$TABLES --table-size=$TABLE_SIZE --threads=4  prewarm


for load in ${LOAD_TYPES[*]}
do
    for thread in ${THREADS[*]}
    do
        sysbenchFn $load $thread run
    done
done


