package payload

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/data"
)

type WriteTimestampIndexSuite struct {
	cfg       *config.Config
	tableName string
	rows      int
	tblInfo   *data.TableInfo
}

func NewWriteTimestampIndexSuite(cfg *config.Config) cmd.CMDGenerater {
	return &WriteTimestampIndexSuite{
		cfg: cfg,
	}
}

func (c *WriteTimestampIndexSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "write-timestamp-index",
		Short:        "payload for test insert into table whose content is a column of timestamp and the column's index.",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.rows, "rows", "r", 100_000, "total insert rows")
	return cmd
}

func (c *WriteTimestampIndexSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *WriteTimestampIndexSuite) Run() error {
	err := c.prepare()
	if err != nil {
		fmt.Printf("prepare table meet error: %v", err)
		return err
	}

	load := data.NewLoadDataSuite(c.cfg)
	load.SetBatchSize(500)
	err = load.LoadData(c.tblInfo, c.rows)
	if err != nil {
		fmt.Printf("insert data error: %v\n", err)
		return err
	}
	return nil
}

func (c *WriteTimestampIndexSuite) prepare() error {
	c.tableName = "t_write_timestamp_index"
	tblInfo, err := data.NewTableInfo(c.cfg.DBName, c.tableName, []data.ColumnDef{
		{
			Name:         "ts",
			Tp:           "timestamp(6)",
			DefaultValue: "current_timestamp(6)",
		},
	}, []data.IndexInfo{
		{
			Name:    "idx0",
			Tp:      data.NormalIndex,
			Columns: []string{"ts"},
		},
	})
	if err != nil {
		return err
	}
	c.tblInfo = tblInfo
	load := data.NewLoadDataSuite(c.cfg)
	return load.CreateTable(c.tblInfo, false)
}
