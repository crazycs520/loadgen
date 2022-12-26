#!/bin/bash

LOADGEN_BIN='../bin/loadgen'

$LOADGEN_BIN payload fk-prepare
$LOADGEN_BIN payload fk-insert-child --thread=20 --rows=10000000
$LOADGEN_BIN payload fk-add-fk
$LOADGEN_BIN payload fk-insert-child --thread=20 --rows=10000000 --fk-check=true
$LOADGEN_BIN payload fk-insert-child --thread=20 --rows=10000000 --fk-check=false

