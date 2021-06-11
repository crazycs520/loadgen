# testutil

## build

```shell
make
```

## usage

#### bench

```shell
bin/testutil bench --sql "select * from t where a=1"
```
# case test introduction

## write conflict

### command: 

```shell
testutil case write-conflict --concurrency 100 --interval 5 --probability 100
```

- concurrency: 指定并发连接数。
- interval: 指定打印相关执行信息的间隔时间，默认值是 1 秒。
- probability: 指定冲突的概率，默认值是 100, 表示冲突的概率是 1/100。

### introduction

表 t 的定义如下：

```sql
CREATE TABLE `t` (
  `id` int(11) NOT NULL,
  `name` varchar(10) DEFAULT NULL,
  `count` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
);
```

多个连接并行执行以下 SQL：

```sql
insert into t values (@a,'aaa', 1) on duplicate key update count=count+1;  
```
SQL 中的 @a 是随机值，取值范围是 [0, probability)

定期打印以上查询打印慢日志信息以及冲突的错误数量。


## write conflict in pessimistic transaction

### command: 

```shell
begin;
testutil case write-conflict-pessimistic --concurrency 100 --interval 5 --probability 100;
commit;
```

和 write conflict 类似，区别是在悲观事务中执行以上 SQL。

## read-write conflict

### command: 

```shell
testutil case read-write-conflict --concurrency 100 --interval 5 --probability 100
```

### introduction

表 t 的定义如下：

```sql
CREATE TABLE `t` (
  `id` int(11) DEFAULT NULL,
  `name` varchar(10) DEFAULT NULL,
  `count` bigint(20) DEFAULT NULL,
  UNIQUE KEY `id` (`id`)
);
```

- 多个连接并行执行以下 SQL：

```sql
insert into t values (@a,'aaa', 1) on duplicate key update count=count+1;  
select * from t where id = @a;
```

SQL 中的 @a 是随机值，取值范围是 [0, probability), @a 所在的 column 上有 unique index。

## 压测 Coprocessor

### command: 

```shell
testutil case stress-cop --concurrency 100 --interval 10 --rows 1000000
```

### introduction

表 t 的定义如下：

```sql
CREATE TABLE `t_cop` (
  `id` int(11) NOT NULL,
  `name` varchar(10) DEFAULT NULL,
  `count` bigint(20) DEFAULT NULL,
  `age` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
);
```

1. 导入 `$rows` 行数据到表t.
2. 多个连接并行执行以下 SQL 来压测 Coprocessor

```sql
select sum(id*count*age) from stress_test.t_cop;
```
