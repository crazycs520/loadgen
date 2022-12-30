#!/bin/bash

HOST=0.0.0.0
PORT=4000
LOADGEN_BIN="../bin/loadgen --host=$HOST --port=$PORT"
THREAD=20
ROWS=1000000

$LOADGEN_BIN payload fk-prepare
$LOADGEN_BIN exec --sql="alter table fk_child add foreign key fk_1 (pid) references fk_parent(id);"

echo '------------- insert with fk check -----------------'
$LOADGEN_BIN payload fk-insert-child --thread=$THREAD --rows=$ROWS --fk-check=true  --batch-size=1 
sleep 5
echo '------------- insert without fk check -----------------'
$LOADGEN_BIN payload fk-insert-child --thread=$THREAD --rows=$ROWS --fk-check=false --batch-size=1 

echo '------------- insert without fk constraint -----------------'
$LOADGEN_BIN exec --sql="alter table fk_child drop foreign key fk_1;"
$LOADGEN_BIN payload fk-insert-child --thread=$THREAD --rows=$ROWS --fk-check=true --batch-size=1 

