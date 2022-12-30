#!/bin/bash

HOST=0.0.0.0
PORT=4000
LOADGEN_BIN="../bin/loadgen --host=$HOST --port=$PORT"
THREAD=20
ROWS=1000000

echo '---------------- delete with fk check --------------------'
$LOADGEN_BIN payload fk-prepare --rows=$ROWS
$LOADGEN_BIN exec --sql="alter table fk_child add foreign key (pid) references fk_parent(id)"
$LOADGEN_BIN payload fk-delete-parent --thread=$THREAD --rows=$ROWS --fk-check=true

echo '---------------- delete without fk check --------------------'

$LOADGEN_BIN payload fk-prepare --rows=$ROWS
$LOADGEN_BIN exec --sql="alter table fk_child add foreign key (pid) references fk_parent(id)"
$LOADGEN_BIN payload fk-delete-parent --thread=$THREAD --rows=$ROWS --fk-check=false


echo '---------------- delete without fk constraint --------------------'
$LOADGEN_BIN payload fk-prepare --rows=$ROWS
$LOADGEN_BIN payload fk-delete-parent --thread=$THREAD --rows=$ROWS --fk-check=false
