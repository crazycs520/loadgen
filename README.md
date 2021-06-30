# loadgen

## build

```shell
make
```

## example:

```shell
# bench const SQL qps
bin/loadgen bench --sql "select * from t" --host 127.0.0.1 --port 4000 --db test --thread 5

# bench rand SQL qps, use `#rand-val` to generate rand value, the rand value is between `valmin` and `valmax`
bin/loadgen bench --sql "select * from t where id=#rand-val" --host 127.0.0.1 --port 4000 --db test --thread 5 --valmin=0 --valmax=100

# use bench command to create 10000 tables. the `#seq-val` is a sequence value, from `valmin` to `valmax`
bin/loadgen bench --sql " create table if not exists t#seq-val (a int key, b int)" --valmin 0 --valmax 10000 --thread 1

# generate payload: full table scan with aggregation
# the SQL is select sum(a+b+e), sum(a*b), sum(a*e), sum(b*e), sum(a*b*e) from test.t_full_table_scan; 
# the total table rows is 1000
bin/loadgen payload full-table-scan --rows=1000

# generate payload: full table scan without aggregation
# the SQL is select * from test.t_full_table_scan;
# the total table rows is 100
bin/loadgen payload full-table-scan --rows=100 --agg=false


# generate payload: full index lookup with aggregation
# the SQL is select sum(a+e), max(c) from test.t_full_index_lookup use index (idx0);
# the total table rows is 100
bin/loadgen payload full-index-lookup --rows 100

# generate payload: full index scan with aggregation
# the SQL is select sum(b), avg(b), min(b), max(b) from test.t_full_index_scan use index (idx0);
# the total table rows is 10000
bin/loadgen payload full-index-scan --rows 10000
```

## payload generator usage

`loadgen` already contain many payload generator, you can use `run the specified payload` to see all supported payload:

```shell
▶ bin/loadgen payload -h
run the specified payload

Usage:
  loadgen payload [flags]
  loadgen payload [command]

Available Commands:
  fix-point-get           payload of fix-point-get
  full-index-lookup       payload of full-index-lookup
  full-index-scan         payload of full-index-scan
  full-table-scan         payload of full-table-scan
  gen-stmt                payload of generate many kind of statements
  index-lookup-for-update payload of index-lookup-for-update
  normal-oltp             payload of normal OLTP test, such as select, point-get, insert, update
  point-get-for-update    payload of point-get-for-update
  rand-batch-point-get    payload of rand-batch-point-get
  rand-point-get          payload of rand-point-get
  write-auto-inc          payload of write-auto-inc
  write-conflict          payload of write conflict
  write-hot               payload of write hot, such as insert with auto_increment, timestamp index
```
