#!/bin/bash

source config.sh

TABLES=32
TABLE_SIZE=1000000

function do_sysbench() {
    echo "do sysbench $1, thread=$2"
    sysbench $1 run --mysql-host=$HOST --mysql-port=$PORT --mysql-user=$USER --db-driver=mysql --mysql-password=$PASSWORD --tables=$TABLES --table-size=$TABLE_SIZE --mysql-db=test --threads=$2 --time=100  --report-interval=10 --mysql-ignore-errors=1062,2013,8028,9007
}

do_sysbench oltp_point_select 32
do_sysbench oltp_point_select 64
do_sysbench oltp_point_select 128

