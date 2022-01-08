package payload

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/data"
	"github.com/crazycs520/loadgen/util"
	"github.com/spf13/cobra"
)

type WriteWideTableSuite struct {
	cfg     *config.Config
	tblInfo *data.TableInfo

	rows          int
	ignore        bool
	IntCols       int
	DoubleCols    int
	VarcharCols   int
	VarcharSize   int
	TimestampCols int
}

const (
	defIntCols       = 50
	defDoubleCols    = 50
	defVarcharCols   = 50
	defTimestampCols = 1
	defVarcharSize   = 128
)

func NewWriteWideTableSuite(cfg *config.Config) cmd.CMDGenerater {
	return &WriteWideTableSuite{
		cfg:           cfg,
		rows:          defRowsOfNormalOLTP,
		IntCols:       defIntCols,
		DoubleCols:    defDoubleCols,
		VarcharCols:   defVarcharCols,
		VarcharSize:   defVarcharSize,
		TimestampCols: defTimestampCols,
		ignore:        false,
	}
}

func (c *WriteWideTableSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          writeWideTableSuiteName,
		Short:        "payload of write wide load",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.rows, flagRows, "", defRowsOfNormalOLTP, "the initial rows size before test")
	cmd.Flags().IntVarP(&c.IntCols, flagIntCols, "", defIntCols, "int column num")
	cmd.Flags().IntVarP(&c.DoubleCols, flagDoubleCols, "", defDoubleCols, "double column num")
	cmd.Flags().IntVarP(&c.VarcharCols, flagVarcharCols, "", defVarcharCols, "varchar column num")
	cmd.Flags().IntVarP(&c.VarcharSize, flagVarcharSize, "", defVarcharSize, "varchar size")
	cmd.Flags().IntVarP(&c.TimestampCols, flagTimestampSize, "", defTimestampCols, "timestamp column num")
	cmd.Flags().BoolVarP(&c.ignore, flagIgnore, "", defIgnoreOfNormalOLTP, "ignore exec sql error if the value is true")
	return cmd
}

func (c *WriteWideTableSuite) ParseCmd(combinedCmd string) bool {
	return ParsePayloadCmd(combinedCmd, writeWideTableSuiteName, func(flag, value string) error {
		switch flag {
		case flagRows:
			v, err := strconv.Atoi(value)
			if err != nil {
				return err
			}
			c.rows = v
		}
		return nil
	})
}

func (c *WriteWideTableSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *WriteWideTableSuite) prepare() error {
	tableName := "t_wide"
	cols := make([]data.ColumnDef, 0, 128)
	idx := 0
	for i := 0; i < c.TimestampCols; i++ {
		idx++
		cols = append(cols, data.ColumnDef{
			Name: fmt.Sprintf("t%v", idx),
			Tp:   "timestamp",
		})
	}
	for i := 0; i < c.IntCols; i++ {
		idx++
		cols = append(cols, data.ColumnDef{
			Name: fmt.Sprintf("c%v", idx),
			Tp:   "bigint",
		})
	}
	for i := 0; i < c.DoubleCols; i++ {
		idx++
		cols = append(cols, data.ColumnDef{
			Name: fmt.Sprintf("c%v", idx),
			Tp:   "double",
		})
	}
	for i := 0; i < c.VarcharCols; i++ {
		idx++
		cols = append(cols, data.ColumnDef{
			Name:     fmt.Sprintf("c%v", idx),
			Tp:       fmt.Sprintf("varchar(%v)", c.VarcharSize),
			FillFull: true,
		})
	}
	tblInfo, err := data.NewTableInfo(c.cfg.DBName, tableName, cols, nil)
	if err != nil {
		return err
	}
	tblInfo.PartitionDef = "PARTITION BY RANGE( UNIX_TIMESTAMP(t1) ) INTERVAL 1 month ( PARTITION p2022_12 VALUES LESS THAN( UNIX_TIMESTAMP('2022-12-31 23:59:59') ));"
	c.tblInfo = tblInfo
	load := data.NewLoadDataSuite(c.cfg)
	return load.Prepare(tblInfo, 0, 0)
}

func (c *WriteWideTableSuite) Run() error {
	err := c.prepare()
	if err != nil {
		fmt.Println("prepare table meet error: ", err)
		return err
	}

	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Thread; i++ {
		wg.Add(1)
		go func(isMain bool) {
			defer wg.Done()
			c.runSQLLoad(isMain)
		}(i == 0)
	}
	wg.Wait()
	return nil
}

func (c *WriteWideTableSuite) runSQLLoad(isMain bool) {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()

	insertStmt := c.getInsertPrepareStmt(db)
	last := time.Now()
	for {
		c.doInsertJob(insertStmt)
		cnt := c.tblInfo.GetInsertedRowSize()
		if cnt >= int64(c.rows) {
			return
		}
		if isMain {
			if time.Since(last) >= time.Second*5 {
				last = time.Now()
				fmt.Printf("inserted rows: %v\n", cnt)
			}
		}
	}
}

func (c *WriteWideTableSuite) getInsertPrepareStmt(db *sql.DB) *sql.Stmt {
	sql := c.tblInfo.GenPrepareInsertSQL(1)
	stmt, err := db.Prepare(sql)
	c.handleError(err, sql)
	return stmt
}

func (c *WriteWideTableSuite) doInsertJob(stmt *sql.Stmt) {
	cnt := c.tblInfo.AddInsertedRowSize(1)
	args := c.tblInfo.GenPrepareInsertStmtArgs(1, int(cnt))
	_, err := stmt.Exec(args...)
	c.handleError(err, "insert")
}

func (c *WriteWideTableSuite) handleError(err error, hint string) {
	if err == nil {
		return
	}
	fmt.Println(err, hint)
	if c.ignore {
		return
	}
	os.Exit(1)
}
