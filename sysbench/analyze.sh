#!/bin/bash

HOST=10.202.15.235
PORT=2883
USER=root@test
PASSWORD=''
DB=test


function exec_sql() {
    echo "exec sql: $1"
    mysql -u $USER -h $HOST -P $PORT --password=$PASSWORD -e "$1"
}

for i in {1..32}
do
  echo $i;
  exec_sql "analyze table $DB.sbtest$i;"
done

