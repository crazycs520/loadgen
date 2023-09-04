package payload

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/crazycs520/loadgen/data"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/util"
)

type CreateDBTableSuite struct {
	cfg *config.Config

	dbNamePrefix string
	dbCount      int
	tableCount   int
	columnCount  int
	indexCount   int
	ignoreErr    bool
	dropDB       bool
}

func (c *CreateDBTableSuite) Name() string {
	return "create-db-table"
}

func NewCreateDBTableSuite(cfg *config.Config) cmd.CMDGenerater {
	suite := &CreateDBTableSuite{
		cfg:         cfg,
		dbCount:     1,
		tableCount:  10,
		columnCount: 10,
		indexCount:  2,
	}
	return suite
}

func (c *CreateDBTableSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "create-db-table",
		Short:        "create dbs and tables load",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().StringVarP(&c.dbNamePrefix, flagDBPrefix, "", "dbstest", "db name prefix")
	cmd.Flags().IntVarP(&c.dbCount, flagDBs, "", 1, "db count")
	cmd.Flags().IntVarP(&c.tableCount, flagTables, "", 10, "table count per db")
	cmd.Flags().IntVarP(&c.columnCount, flagColumns, "", 10, "column count per table")
	cmd.Flags().IntVarP(&c.indexCount, flagIndexes, "", 2, "index count per table")
	cmd.Flags().BoolVarP(&c.ignoreErr, flagIgnore, "", true, "ignore error when create table failed")
	cmd.Flags().BoolVarP(&c.dropDB, "drop-db", "", false, "drop db before create")
	return cmd
}

func (c *CreateDBTableSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

type DBTableTask struct {
	dbName    string
	tableName string
}

func (c *CreateDBTableSuite) Run() error {
	fmt.Printf("%v config:%v\ndb-prefix: %v, db-count: %v, table-count:%v, column-count: %v, ignore-err: %v\n",
		c.Name(), c.cfg.String(), c.dbNamePrefix, c.dbCount, c.tableCount, c.columnCount, c.ignoreErr)
	startTime := time.Now()
	if err := c.createDB(); err != nil {
		return err
	}
	fmt.Printf("create %v db finish\n", c.dbCount)

	taskCh := make(chan DBTableTask, 10)
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Thread; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db := util.GetSQLCli(c.cfg)
			defer func() {
				db.Close()
			}()
			for {
				select {
				case task, ok := <-taskCh:
					if !ok {
						return
					}
					err := c.createTable(db, task)
					if err != nil {
						fmt.Printf("create table failed, table: %v.%v err: %v\n", task.dbName, task.tableName, err)
						if !c.ignoreErr {
							return
						}
					}
				}
			}
		}()
	}

	for dbi := 1; dbi <= c.dbCount; dbi++ {
		dbName := fmt.Sprintf("%v%v", c.dbNamePrefix, dbi)
		for ti := 1; ti <= c.tableCount; ti++ {
			tableName := fmt.Sprintf("t%v", ti)
			taskCh <- DBTableTask{
				dbName:    dbName,
				tableName: tableName,
			}
		}
	}
	close(taskCh)
	wg.Wait()
	fmt.Printf("create %v database, total %v tables, cost: %v\n", c.dbCount, c.tableCount*c.dbCount, time.Since(startTime).String())
	return nil
}

func (c *CreateDBTableSuite) createDB() error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	for dbi := 1; dbi <= c.dbCount; dbi++ {
		dbName := fmt.Sprintf("%v%v", c.dbNamePrefix, dbi)
		if c.dropDB {
			_, err := db.Exec(fmt.Sprintf("drop database if exists `%v`", dbName))
			if err != nil {
				return err
			}
		}
		_, err := db.ExecContext(context.Background(), fmt.Sprintf("create database `%v`", dbName))
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *CreateDBTableSuite) createTable(db *sql.DB, task DBTableTask) error {
	cols := make([]data.ColumnDef, 0, c.columnCount)
	indexes := make([]data.IndexInfo, 0, c.indexCount)
	for i := 0; i < c.columnCount; i++ {
		tp := "bigint"
		if i%3 == 1 {
			tp = "varchar(100)"
		} else if i%3 == 2 {
			tp = "decimal(48,10)"
		}
		col := data.ColumnDef{
			Name: fmt.Sprintf("c%v", i),
			Tp:   tp,
		}
		cols = append(cols, col)
	}
	for i := 0; i < c.indexCount && i < c.columnCount; i++ {
		idx := data.IndexInfo{
			Name:    fmt.Sprintf("idx%v", i),
			Tp:      data.NormalIndex,
			Columns: []string{fmt.Sprintf("c%v", i)},
		}
		indexes = append(indexes, idx)
	}

	tblInfo, err := data.NewTableInfo(task.dbName, task.tableName, cols, indexes)
	if err != nil {
		return err
	}

	_, err = db.Exec(tblInfo.CreateSQL())
	return err
}
