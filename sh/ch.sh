#!/bin/bash

tiup bench tpcc -H advanced-tidb-tidb -P 4000 -D tpcc --warehouses 1000 --threads 4 prepare
sleep 120
tiup bench tpcc -H advanced-tidb-tidb -P 4000 -D tpcc --warehouses 1000 --threads 25 --time 10m run
tiup bench tpcc -H advanced-tidb-tidb -P 4000 -D tpcc --warehouses 1000 --threads 50 --time 10m run
tiup bench tpcc -H advanced-tidb-tidb -P 4000 -D tpcc --warehouses 1000 --threads 100 --time 10m run
tiup bench tpcc -H advanced-tidb-tidb -P 4000 -D tpcc --warehouses 1000 --threads 200 --time 10m run

