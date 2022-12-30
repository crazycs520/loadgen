#!/bin/bash

HOST=0.0.0.0
PORT=4000
LOADGEN_BIN="../bin/loadgen --host=$HOST --port=$PORT"
THREAD=20
ROWS=1000000

echo '---------------- update with fk check --------------------'
$LOADGEN_BIN payload fk-prepare --rows=$ROWS
$LOADGEN_BIN payload fk-insert-child --thread=20 --rows=$ROWS --parent-row=$ROWS  --batch-size=100
$LOADGEN_BIN exec --sql="alter table fk_child add foreign key (pid) references fk_parent(id) on update cascade"
$LOADGEN_BIN payload fk-update-parent --thread=$THREAD --rows=$ROWS --fk-check=true

echo '---------------- update without fk check --------------------'
$LOADGEN_BIN payload fk-prepare --rows=$ROWS
$LOADGEN_BIN exec --sql="alter table fk_child add foreign key (pid) references fk_parent(id) on update cascade"
$LOADGEN_BIN payload fk-update-parent --thread=$THREAD --rows=$ROWS --fk-check=false

echo '---------------- update without fk constraint --------------------'
$LOADGEN_BIN payload fk-prepare --rows=$ROWS
$LOADGEN_BIN payload fk-update-parent --thread=$THREAD --rows=$ROWS --fk-check=false

echo '---------------- update without fk constraint manual cascade delete child --------------------'
$LOADGEN_BIN payload fk-prepare --thread=$THREAD --rows=$ROWS
# add and drop foreign key for make sure there is an index on pid column
$LOADGEN_BIN exec --sql="alter table fk_child add foreign key fk_1 (pid) references fk_parent(id)"
$LOADGEN_BIN exec --sql="alter table fk_child drop foreign key fk_1"
$LOADGEN_BIN payload fk-insert-child --thread=20 --rows=$ROWS --parent-row=$ROWS  --batch-size=100
$LOADGEN_BIN payload fk-update-parent --thread=$THREAD --rows=$ROWS --manual-cascade=true