package payload

import (
	"fmt"
	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/data"
	"github.com/crazycs520/loadgen/util"
	"github.com/spf13/cobra"
)

type BigInsertSuite struct {
	cfg *config.Config

	colCnt     int
	batch      int
	rows       int
	usePrepare bool

	tableName string
	tableInfo *data.TableInfo
}

func NewBigInsertSuite(cfg *config.Config) cmd.CMDGenerater {
	return &BigInsertSuite{
		cfg:    cfg,
		colCnt: 8,
		batch:  10,
	}
}

func (c *BigInsertSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "big-insert",
		Short:        "payload of big-insert in 1 statements",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.colCnt, flagColCnt, "", 200, "the table column count")
	cmd.Flags().IntVarP(&c.batch, flagBatchSize, "", 500, "the insert batch size")
	cmd.Flags().IntVarP(&c.rows, flagRows, "", 100000000, "the total insert rows")
	cmd.Flags().BoolVarP(&c.usePrepare, flagUsePrepare, "", false, "whether use prepare")
	return cmd
}

func (c *BigInsertSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *BigInsertSuite) prepare() error {
	c.tableName = "t_big_insert"
	defCols := []data.ColumnDef{
		{
			Name: "a",
			Tp:   "bigint",
		},
		{
			Name:         "b",
			Tp:           "timestamp(6)",
			DefaultValue: "current_timestamp(6)",
		},
		{
			Name: "c",
			Tp:   "varchar(100)",
		},
	}
	cols := make([]data.ColumnDef, 0, c.colCnt)
	for i := 0; i < c.colCnt; i++ {
		col := defCols[i%len(defCols)]
		col.Name = fmt.Sprintf("c%v", i+1)
		cols = append(cols, col)
	}
	tblInfo, err := data.NewTableInfo(c.cfg.DBName, c.tableName, cols, nil)
	if err != nil {
		return err
	}
	c.tableInfo = tblInfo
	load := data.NewLoadDataSuite(c.cfg)
	return load.CreateTable(tblInfo, true)
}

func (c *BigInsertSuite) Run() error {
	err := c.prepare()
	if err != nil {
		fmt.Println("prepare table meet error: ", err)
		return err
	}

	if !c.usePrepare {
		db := util.GetSQLCli(c.cfg)
		defer func() {
			db.Close()
		}()

		sql := c.tableInfo.GenBatchInsertSQL(0, c.batch)
		fmt.Printf("sql length is: %v\n", len(sql))

		for i := 0; i < c.rows; i += c.batch {
			_, err := db.Exec(c.tableInfo.GenBatchInsertSQL(i, c.batch))
			if err != nil {
				return err
			}
		}
	} else {
		load := data.NewLoadDataSuite(c.cfg)
		load.SetBatchSize(c.batch)
		err = load.LoadData(c.tableInfo, c.rows)
		if err != nil {
			fmt.Printf("insert data error: %v\n", err)
		}
	}
	return nil
}
