#!/bin/bash

HOST=10.202.15.221
PORT=4000
USER=root
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

