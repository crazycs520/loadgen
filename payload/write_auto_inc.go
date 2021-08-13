package payload

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/data"
)

type WriteAutoIncSuite struct {
	cfg       *config.Config
	tableName string
	rows      int
	*basicWriteSuite
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
	cmd.Flags().IntVarP(&c.rows, "rows", "", 1000000, "total insert rows")
	return cmd
}

func (c *WriteAutoIncSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *WriteAutoIncSuite) Run() error {
	err := c.prepare()
	if err != nil {
		return err
	}
	fmt.Printf("start to do write auto increment\n")

	load := data.NewLoadDataSuit(c.cfg)
	load.SetBatchSize(500)
	err = load.LoadData(c.tblInfo, c.rows)
	if err != nil {
		fmt.Printf("insert data err: %v\n", err)
	}
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

func NewWriteAutoIncSuite(cfg *config.Config) cmd.CMDGenerater {
	suite := &WriteAutoIncSuite{cfg: cfg}
	basic := NewBasicWriteSuite(cfg, suite)
	suite.basicWriteSuite = basic
	return suite
}
