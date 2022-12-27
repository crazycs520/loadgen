#!/bin/bash

LOADGEN_BIN='../bin/loadgen'
THREAD=20
ROWS=1000000

echo '---------------- update with fk check --------------------'
$LOADGEN_BIN payload fk-prepare --rows=$ROWS
$LOADGEN_BIN exec --sql="alter table fk_child add foreign key (pid) references fk_parent(id)"
$LOADGEN_BIN payload fk-update-parent --thread=$THREAD --rows=$ROWS --fk-check=true

echo '---------------- update without fk check --------------------'

$LOADGEN_BIN payload fk-prepare --rows=$ROWS
$LOADGEN_BIN exec --sql="alter table fk_child add foreign key (pid) references fk_parent(id)"
$LOADGEN_BIN payload fk-update-parent --thread=$THREAD --rows=$ROWS --fk-check=false


echo '---------------- update without fk constraint --------------------'
$LOADGEN_BIN payload fk-prepare --rows=$ROWS
$LOADGEN_BIN payload fk-update-parent --thread=$THREAD --rows=$ROWS --fk-check=false
