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

	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/util"
)

type LoadDataSuite struct {
	cfg         *config.Config
	insertCount int64
	batchSize   int
}

func NewLoadDataSuite(cfg *config.Config) *LoadDataSuite {
	return &LoadDataSuite{
		cfg:       cfg,
		batchSize: 100,
	}
}

func (c *LoadDataSuite) SetBatchSize(n int) {
	c.batchSize = n
}

func (c *LoadDataSuite) Prepare(t *TableInfo, rows, regionRowNum int) error {
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

func (c *LoadDataSuite) CreateTable(t *TableInfo, dropIfExist bool) error {
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

func (c *LoadDataSuite) createTable(db *sql.DB, t *TableInfo) error {
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

func (c *LoadDataSuite) splitTableRegion(db *sql.DB, t *TableInfo, rows, regionRowNum int) {
	if rows <= regionRowNum || regionRowNum <= 0 {
		return
	}
	regionNum := rows / regionRowNum
	if regionNum <= 1 {
		return
	}
	fmt.Printf("split %v regions for table %v\n", regionNum, t.DBTableName())

	split := fmt.Sprintf("split table %v between (0) and (%v) regions %v;", t.DBTableName(), rows, regionNum)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err := db.ExecContext(ctx, split)
	if err != nil {
		fmt.Printf("split region error: %v\n", err)
	}
	cancel()
}

func (c *LoadDataSuite) LoadData(t *TableInfo, rows int) error {
	fmt.Printf("start insert %v rows into table %v\n", rows, t.DBTableName())
	// prepare data.
	step := (rows / c.cfg.Thread) + 1
	if step < c.batchSize {
		return c.insertData(t, 0, rows)
	}
	var wg sync.WaitGroup
	errCh := make(chan error, c.cfg.Thread)
	for i := 0; i < c.cfg.Thread; i++ {
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

func (c *LoadDataSuite) insertData(t *TableInfo, start, end int) error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	var err error
	batchSize := c.batchSize
	stmt, err := db.Prepare(t.GenPrepareInsertSQL(batchSize))
	if err != nil {
		return err
	}

	i := 0
	for i = start; (i + batchSize) < end; i = i + batchSize {
		args := t.GenPrepareInsertStmtArgs(batchSize, i)
		_, err = stmt.Exec(args...)
		if err != nil {
			return err
		}
		atomic.AddInt64(&c.insertCount, int64(batchSize))
	}
	for ; i < end; i++ {
		_, err := db.Exec(t.GenInsertSQL(i))
		if err != nil {
			return err
		}
		atomic.AddInt64(&c.insertCount, 1)
	}
	atomic.AddInt64(&t.InsertedRows, int64(end-start))
	return nil
}

func (s *LoadDataSuite) checkTableExist(db *sql.DB, t *TableInfo) bool {
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

func (s *LoadDataSuite) checkTableRowMatch(db *sql.DB, t *TableInfo, rows int) bool {
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
		idxName := idx.Name
		if idxName == "" {
			idxName = "idx" + strconv.Itoa(i)
		}
		switch idx.Tp {
		case NormalIndex:
			sql += fmt.Sprintf(", index %v (%v)", idxName, strings.Join(idx.Columns, ","))
		case UniqueIndex:
			sql += fmt.Sprintf(", unique index %v (%v)", idxName, strings.Join(idx.Columns, ","))
		case PrimaryKey:
			sql += fmt.Sprintf(", primary key (%v)", strings.Join(idx.Columns, ","))
		}
	}
	sql += ")"
	sql += t.PartitionDef
	return sql
}

func (t *TableInfo) GenInsertSQL(num int) string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	buf.WriteString("insert into ")
	buf.WriteString(t.DBTableName())
	buf.WriteString(" values (")
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

func (t *TableInfo) GenBatchInsertSQL(id int, batch int) string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	buf.WriteString("insert into ")
	buf.WriteString(t.DBTableName())
	buf.WriteString(" values ")
	for row := 0; row < batch; row++ {
		if row > 0 {
			buf.WriteString(",")
		}
		buf.WriteString("(")
		for i, col := range t.Columns {
			if i > 0 {
				buf.WriteString(",")
			}
			v := col.seqValue(int64(id + row))
			buf.WriteString(fmt.Sprintf("'%v'", v))
		}
		buf.WriteString(")")
	}
	return buf.String()
}

func (t *TableInfo) GenPrepareInsertSQL(rows int) string {
	buf := bytes.NewBuffer(make([]byte, 0, 16*rows))
	buf.WriteString(fmt.Sprintf("insert into %v values ", t.DBTableName()))
	for row := 0; row < rows; row++ {
		if row > 0 {
			buf.WriteString(",")
		}
		buf.WriteString("(")
		for i := range t.Columns {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString("?")
		}
		buf.WriteString(")")
	}
	return buf.String()
}

func (t *TableInfo) GenPrepareInsertStmtArgs(rows, num int) []interface{} {
	args := make([]interface{}, 0, len(t.Columns)*rows)
	for row := 0; row < rows; row++ {
		for _, col := range t.Columns {
			v := col.seqValue(int64(num + row))
			args = append(args, v)
		}
	}
	return args
}

func (t *TableInfo) DBTableName() string {
	return t.DBName + "." + t.TableName
}
