#!/bin/bash

HOST=127.0.0.1
PORT=4001
USER=root
DB=test
CMD=$1

function exec_sql() {
    echo "exec sql: $1"
    mysql -u $USER -h $HOST -P $PORT --password=$PASSWORD -e "$1"
}

for i in {1..32}
do
  exec_sql "alter table $DB.sbtest$i $CMD"
done

