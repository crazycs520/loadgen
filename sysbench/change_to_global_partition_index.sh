#!/bin/bash

HOST=10.202.15.235
PORT=2883
USER=root@test
DB=test

function exec_sql() {
    echo "exec sql: $1"
    mysql -u $USER -h $HOST -P $PORT --password=$PASSWORD -e "$1"
}

for i in {1..32}
do
  echo $i;
  exec_sql "alter table $DB.sbtest$i drop index k_$i;"
  exec_sql "alter table $DB.sbtest$i add index k_$i(k) global PARTITION BY RANGE(k)
     (PARTITION p1 VALUES LESS THAN(1000000),
       PARTITION p2 VALUES LESS THAN(2000000),
       PARTITION p3 VALUES LESS THAN(3000000),
       PARTITION p4 VALUES LESS THAN(4000000),
       PARTITION p5 VALUES LESS THAN(5000000),
       PARTITION p6 VALUES LESS THAN(6000000),
       PARTITION p7 VALUES LESS THAN(7000000),
       PARTITION p8 VALUES LESS THAN(8000000),
       PARTITION p9 VALUES LESS THAN(9000000),
       PARTITION p10 VALUES LESS THAN MAXVALUE
     );"

done
