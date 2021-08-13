package payload

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/spf13/cobra"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/data"
	"github.com/crazycs520/loadgen/util"
)

type WriteAutoIncSuite struct {
	cfg       *config.Config
	tableName string
	rows      int64
	inc       int64
	*basicWriteSuite
}

func NewWriteAutoIncSuite(cfg *config.Config) cmd.CMDGenerater {
	suite := &WriteAutoIncSuite{cfg: cfg, inc: -1}
	basic := NewBasicWriteSuite(cfg, suite)
	suite.basicWriteSuite = basic
	return suite
}

func (c *WriteAutoIncSuite) Name() string {
	return writeAutoIncSuiteName
}

func (c *WriteAutoIncSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "write-auto-inc",
		Short:        "payload for write auto increment",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().Int64VarP(&c.rows, "rows", "", 1000000, "total insert rows")
	return cmd
}

func (c *WriteAutoIncSuite) RunE(cmd *cobra.Command, args []string) error {
	fmt.Println("thread:", c.cfg.Thread)
	return c.Run()
}

func (c *WriteAutoIncSuite) Run() error {
	err := c.prepare()
	if err != nil {
		return err
	}
	fmt.Printf("start to do write auto increment\n")

	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Thread; i++ {
		wg.Add(1)
		go func() {
			db := util.GetSQLCli(c.cfg)
			defer func() {
				db.Close()
			}()

			err := c.update(db)
			if err != nil {
				fmt.Println(err.Error())
			}
			wg.Done()
		}()
	}
	wg.Wait()

	return err
}

func (c *WriteAutoIncSuite) UpdateTableDef(_ *data.TableInfo) {
}

func (c *WriteAutoIncSuite) prepare() error {
	c.tableName = "t_" + strings.ReplaceAll(c.Name(), "-", "_")
	tblInfo, err := data.NewTableInfo(c.cfg.DBName, c.tableName, []data.ColumnDef{
		{
			Name:     "auto",
			Tp:       "bigint",
			Property: "primary key auto_increment",
		},
		{
			Name: "a",
			Tp:   "bigint",
		},
		{
			Name: "b",
			Tp:   "bigint",
		},
		{
			Name:         "c",
			Tp:           "timestamp(6)",
			DefaultValue: "current_timestamp(6)",
		},
	}, []data.IndexInfo{
		{
			Name:    "idx0",
			Tp:      data.UniqueIndex,
			Columns: []string{"a"},
		},
	})
	if err != nil {
		return err
	}
	c.tblInfo = tblInfo
	load := data.NewLoadDataSuit(c.cfg)
	return load.CreateTable(c.tblInfo, false)
}

func (c *WriteAutoIncSuite) update(db *sql.DB) error {

	stmt, err := db.Prepare(c.genPrepareSQL())
	if err != nil {
		return err
	}

	for {
		inc := atomic.AddInt64(&c.inc, 1)
		if inc > c.rows {
			return nil
		}
		args := c.genPrepareArgs(inc)
		_, err := stmt.Exec(args...)
		if err != nil {
			return err
		}
	}
}

func (c *WriteAutoIncSuite) genPrepareSQL() string {
	return "insert into " + c.tblInfo.DBTableName() + " (a,b) values (?, ?);"
}

func (c *WriteAutoIncSuite) genPrepareArgs(inc int64) []interface{} {
	return []interface{}{inc, inc}
}
