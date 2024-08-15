#!/bin/bash

HOST=10.202.15.229
PORT=2883
USER=root@test
DB=test

function exec_sql() {
    echo "exec sql: $1"
    mysql -u $USER -h $HOST -P $PORT --password=$PASSWORD -e "$1"
}

for i in {1..32}
do
      exec_sql "alter table $DB.sbtest$i add column k1 bigint;"
      exec_sql "alter table $DB.sbtest$i add column k2 bigint;"
      exec_sql "alter table $DB.sbtest$i add column k3 bigint;"
      exec_sql "alter table $DB.sbtest$i add column k4 bigint;"
      exec_sql "alter table $DB.sbtest$i add column k5 bigint;"
      exec_sql "alter table $DB.sbtest$i add column k6 bigint;"
      exec_sql "alter table $DB.sbtest$i add column k7 bigint;"
      exec_sql "alter table $DB.sbtest$i add column k8 bigint;"
      exec_sql "update $DB.sbtest$i set k1=k, k2=k, k3=k, k4=k, k5=k,k6=k,k7=k,k8=k;"
  for j in {1..8}
  do
    echo $i;
    exec_sql "alter table $DB.sbtest$i add index kk_$j(k$j) global PARTITION BY RANGE(k$j)
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
done
