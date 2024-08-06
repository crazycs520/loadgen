#!/bin/bash

HOST=192.168.178.12
PORT=2883
USER=root
PASSWORD='crazycs520.'
TABLES=32
DB=test

function exec_sql() {
    echo "exec sql: $1"
    #mysql -u $USER -h $HOST -P $PORT -p $PASSWORD -e "$1"
}

for i in {1..32}
do
  exec_sql "alter table $DB.sbtest$i drop index k_$i;"
done

