package payload

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/data"
)

type WriteHotSuite struct {
	cfg       *config.Config
	tableName string
	tblInfo   *data.TableInfo

	rows int
}

func NewWriteHotSuite(cfg *config.Config) cmd.CMDGenerater {
	return &WriteHotSuite{
		cfg: cfg,
	}
}

func (c *WriteHotSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "write-hot",
		Short:        "payload of write hot, such as insert with auto_increment, timestamp index",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.rows, "rows", "", 10000000, "total insert rows")
	return cmd
}

func (c *WriteHotSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *WriteHotSuite) prepare() error {
	c.tableName = "t_write_hot"
	tblInfo, err := data.NewTableInfo(c.cfg.DBName, c.tableName, []data.ColumnDef{
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
		{
			Name: "d",
			Tp:   "varchar(100)",
		},
		{
			Name: "e",
			Tp:   "varchar(100)",
		},
		{
			Name: "f",
			Tp:   "varchar(100)",
		},
		{
			Name: "g",
			Tp:   "varchar(100)",
		},
		{
			Name: "h",
			Tp:   "varchar(100)",
		},
		{
			Name: "i",
			Tp:   "varchar(100)",
		},
		{
			Name: "j",
			Tp:   "varchar(100)",
		},
		{
			Name: "k",
			Tp:   "varchar(100)",
		},
		{
			Name: "l",
			Tp:   "varchar(100)",
		},
		{
			Name: "m",
			Tp:   "varchar(100)",
		},
		{
			Name: "n",
			Tp:   "varchar(100)",
		},
		{
			Name: "o",
			Tp:   "varchar(100)",
		},
		{
			Name: "p",
			Tp:   "varchar(100)",
		},
		{
			Name: "q",
			Tp:   "varchar(100)",
		},
		{
			Name: "r",
			Tp:   "varchar(100)",
		},
		{
			Name: "s",
			Tp:   "varchar(100)",
		},
		{
			Name: "t",
			Tp:   "varchar(100)",
		},
	}, []data.IndexInfo{
		{
			Name:    "idx0",
			Tp:      data.NormalIndex,
			Columns: []string{"c", "a", "d"},
		},
	})
	if err != nil {
		return err
	}
	c.tblInfo = tblInfo
	load := data.NewLoadDataSuite(c.cfg)
	return load.CreateTable(tblInfo, true)
}

func (c *WriteHotSuite) Run() error {
	err := c.prepare()
	if err != nil {
		fmt.Println("prepare table meet error: ", err)
		return err
	}

	load := data.NewLoadDataSuite(c.cfg)
	load.SetBatchSize(500)
	err = load.LoadData(c.tblInfo, c.rows)
	if err != nil {
		fmt.Printf("insert data error: %v\n", err)
	}
	return nil
}
