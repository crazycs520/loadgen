#!/bin/bash

HOST=0.0.0.0
PORT=4000
LOADGEN_BIN="../bin/loadgen --host=$HOST --port=$PORT"


set_chunk_size(){
  mysql -u root -h $HOST -P $PORT -e "set @@global.tidb_max_chunk_size=$1"
}

# $1 is chunk size
# $2 is query bound
# $3 is thread
bench(){
  date
  set_chunk_size $1
  $LOADGEN_BIN payload index-lookup --rows=100000 --rows-per-region=1000 --bound=$2 --thread=$3 --time=300
  sleep 30
}


bench 1024 200000 1
bench 20480 20000 1
bench 1024 200000 20
bench 20480 20000 20
