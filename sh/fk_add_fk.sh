#!/bin/bash

HOST=0.0.0.0
PORT=4000
LOADGEN_BIN="../bin/loadgen --host=$HOST --port=$PORT"
THREAD=20
ROWS=10000000

# prepare
$LOADGEN_BIN payload fk-prepare
$LOADGEN_BIN payload fk-insert-child --thread=$THREAD --rows=$ROWS

echo '---------------- add fk speed --------------------'
$LOADGEN_BIN exec --sql="alter table fk_child add foreign key (pid) references fk_parent(id)"

echo '---------------- add fk without auto create index speed --------------------'
$LOADGEN_BIN exec --sql="alter table fk_child add foreign key (pid) references fk_parent(id)"


echo '---------------- add index speed  --------------------'
$LOADGEN_BIN exec --sql="alter table fk_child add index idx0(pid)"
