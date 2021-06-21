package data

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/crazycs520/load/config"
	"github.com/crazycs520/load/util"
)

type LoadDataSuit struct {
	cfg         *config.Config
	insertCount int64
	batchSize   int
}

func NewLoadDataSuit(cfg *config.Config) *LoadDataSuit {
	return &LoadDataSuit{
		cfg:       cfg,
		batchSize: 100,
	}
}

func (c *LoadDataSuit) SetBatchSize(n int) {
	c.batchSize = n
}

func (c *LoadDataSuit) Prepare(t *TableInfo, rows, regionRowNum int) error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	exist := c.checkTableExist(db, t)
	if exist && rows == 0 {
		return nil
	}

	match := c.checkTableRowMatch(db, t, rows)
	if match {
		t.AddInsertedRowSize(int64(rows))
		return nil
	}

	err := c.createTable(db, t)
	if err != nil {
		return err
	}
	if rows == 0 {
		return nil
	}

	c.splitTableRegion(db, t, rows, regionRowNum)

	return c.LoadData(t, rows)
}

func (c *LoadDataSuit) CreateTable(t *TableInfo, dropIfExist bool) error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	if !dropIfExist {
		exist := c.checkTableExist(db, t)
		if exist {
			return nil
		}
	}
	return c.createTable(db, t)
}

func (c *LoadDataSuit) createTable(db *sql.DB, t *TableInfo) error {
	fmt.Printf("create table %v\n", t.DBTableName())
	sqls := []string{
		"create database if not exists " + t.DBName,
		"use " + t.DBName,
		"drop table if exists " + t.DBTableName(),
		t.createSQL(),
	}
	for _, s := range sqls {
		_, err := db.Exec(s)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *LoadDataSuit) splitTableRegion(db *sql.DB, t *TableInfo, rows, regionRowNum int) {
	if rows <= regionRowNum || regionRowNum <= 0 {
		return
	}
	regionNum := rows / regionRowNum
	fmt.Printf("split %v regions for table %v\n", regionNum, t.DBTableName())

	split := fmt.Sprintf("split table %v between (0) and (%v) regions %v;", t.DBTableName(), rows, regionNum)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err := db.ExecContext(ctx, split)
	if err != nil {
		fmt.Printf("split region error: %v\n", err)
	}
	cancel()
}

func (c *LoadDataSuit) LoadData(t *TableInfo, rows int) error {
	fmt.Printf("start insert %v rows into table %v\n", rows, t.DBTableName())
	// prepare data.
	step := (rows / c.cfg.Concurrency) + 1
	if step < 10 {
		return c.insertData(t, 0, rows)
	}
	var wg sync.WaitGroup
	errCh := make(chan error, c.cfg.Concurrency)
	for i := 0; i < c.cfg.Concurrency; i++ {
		wg.Add(1)
		start := i * step
		end := (i + 1) * step
		if end > rows {
			end = rows
		}
		go func() {
			defer wg.Done()
			err := c.insertData(t, start, end)
			if err != nil {
				fmt.Printf("insert data error: %v\n", err)
				errCh <- err
			}
		}()
	}
	var err error
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case err = <-errCh:
				cancel()
			case <-ticker.C:
				fmt.Printf("inserted rows: %v \n", atomic.LoadInt64(&c.insertCount))
			}
		}
	}()
	wg.Wait()
	if err == nil {
		cancel()
	}
	fmt.Println("finish load data")
	return nil
}

func (c *LoadDataSuit) insertData(t *TableInfo, start, end int) error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	var err error
	stmt, err := db.Prepare(t.GenPrepareInsertSQL())
	if err != nil {
		return err
	}

	txn, err := db.Begin()
	if err != nil {
		return err
	}
	for i := start; i < end; i++ {
		args := t.GenPrepareInsertStmtArgs(i)
		_, err = txn.Stmt(stmt).Exec(args...)
		if err != nil {
			return err
		}
		if (i-start)%c.batchSize == 1 {
			err = txn.Commit()
			if err != nil {
				return err
			}
			txn, err = db.Begin()
			if err != nil {
				return err
			}
		}
		atomic.AddInt64(&c.insertCount, 1)
	}
	atomic.AddInt64(&t.InsertedRows, int64(end-start))
	return txn.Commit()
}

func (s *LoadDataSuit) checkTableExist(db *sql.DB, t *TableInfo) bool {
	colNames := t.getColumnNames()
	query := fmt.Sprintf("select %v from %v limit 1", strings.Join(colNames, ","), t.DBTableName())
	_, err := db.Exec(query)
	if err != nil {
		if match, _ := regexp.MatchString(".*Table.*doesn't exist.*", err.Error()); match {
			fmt.Printf("table %v doesn't exists\n", t.DBTableName())
		}
		return false
	}
	return true
}

func (s *LoadDataSuit) checkTableRowMatch(db *sql.DB, t *TableInfo, rows int) bool {
	query := fmt.Sprintf("select count(1) from %v", t.DBTableName())
	match := true
	err := util.QueryRows(db, query, func(row, cols []string) error {
		if len(row) != 1 {
			match = false
			return nil
		}
		cnt, _ := strconv.Atoi(row[0])
		match = cnt == rows
		if !match {
			fmt.Printf("table %v current rows is %v, expected rows id %v\n",
				t.DBTableName(), cnt, rows)
		}
		return nil
	})
	if err != nil {
		fmt.Printf("table %v rows count error: %v\n", t.DBTableName(), err)
		return false
	}
	return match
}

func (t *TableInfo) getColumnNames() []string {
	names := []string{}
	for _, col := range t.Columns {
		names = append(names, col.Name)
	}
	return names
}

func (t *TableInfo) createSQL() string {
	sql := fmt.Sprintf("CREATE TABLE `%s` (", t.TableName)
	cols := t.Columns
	for i, col := range cols {
		if i > 0 {
			sql += ", "
		}
		sql += fmt.Sprintf("`%s` %s", col.Name, col.getDefinition())
	}
	for i, idx := range t.Indexs {
		switch idx.Tp {
		case NormalIndex:
			sql += fmt.Sprintf(", index idx%v (%v)", i, strings.Join(idx.Columns, ","))
		case UniqueIndex:
			sql += fmt.Sprintf(", unique index idx%v (%v)", i, strings.Join(idx.Columns, ","))
		case PrimaryKey:
			sql += fmt.Sprintf(", primary key (%v)", strings.Join(idx.Columns, ","))
		}
	}
	sql += ")"
	return sql
}

func (t *TableInfo) GenInsertSQL(num int) string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	buf.WriteString(fmt.Sprintf("insert into %v values (", t.DBTableName()))
	for i, col := range t.Columns {
		if i > 0 {
			buf.WriteString(",")
		}
		v := col.seqValue(int64(num))
		buf.WriteString(fmt.Sprintf("'%v'", v))
	}
	buf.WriteString(")")
	return buf.String()
}

func (t *TableInfo) GenPrepareInsertSQL() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	buf.WriteString(fmt.Sprintf("insert into %v values (", t.DBTableName()))
	for i := range t.Columns {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString("?")
	}
	buf.WriteString(")")
	return buf.String()
}

func (t *TableInfo) GenPrepareInsertStmtArgs(num int) []interface{} {
	args := make([]interface{}, 0, len(t.Columns))
	for _, col := range t.Columns {
		v := col.seqValue(int64(num))
		args = append(args, v)
	}
	return args
}

func (t *TableInfo) DBTableName() string {
	return t.DBName + "." + t.TableName
}
