#!/bin/bash

HOST=0.0.0.0
PORT=4000
LOADGEN_BIN="../bin/loadgen --host=$HOST --port=$PORT"

$LOADGEN_BIN payload fk-prepare
$LOADGEN_BIN payload fk-insert-child --thread=20 --rows=10000000
$LOADGEN_BIN payload fk-add-fk
$LOADGEN_BIN payload fk-insert-child --thread=20 --rows=10000000 --fk-check=true
$LOADGEN_BIN payload fk-insert-child --thread=20 --rows=10000000 --fk-check=false

