#!/bin/bash

LOADGEN_BIN='../bin/loadgen'
THREAD=20
ROWS=1000000

$LOADGEN_BIN payload fk-prepare
$LOADGEN_BIN payload fk-add-fk
$LOADGEN_BIN payload fk-insert-child --thread=$THREAD --rows=$ROWS --fk-check=true  --batch-size=1 
sleep 5
$LOADGEN_BIN payload fk-insert-child --thread=$THREAD --rows=$ROWS --fk-check=false --batch-size=1 

