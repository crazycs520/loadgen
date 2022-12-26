package payload

import (
	"fmt"
	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/data"
	"github.com/spf13/cobra"
)

type FKPrepareSuite struct {
	cfg *config.Config

	rows int

	parentTable *data.TableInfo
}

func NewFKPrepareSuite(cfg *config.Config) cmd.CMDGenerater {
	return &FKPrepareSuite{
		cfg: cfg,
	}
}

func (c *FKPrepareSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "fk-prepare",
		Short:        "create foreign key parent/child table, and insert some data into parent table",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.rows, flagRows, "", 100000, "the total insert rows")
	return cmd
}

func (c *FKPrepareSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *FKPrepareSuite) prepare() error {
	parentTblInfo, childTblInfo, err := getParentAndChildTableInfo(c.cfg.DBName)
	if err != nil {
		return err
	}
	load := data.NewLoadDataSuite(c.cfg)
	err = load.CreateTable(childTblInfo, true)
	if err != nil {
		return err
	}
	c.parentTable = parentTblInfo
	return load.CreateTable(c.parentTable, true)
}

func getParentAndChildTableInfo(dbName string) (*data.TableInfo, *data.TableInfo, error) {
	parentTable := "fk_parent"
	childTable := "fk_child"
	cols := []data.ColumnDef{
		{
			Name: "id",
			Tp:   "bigint",
		},
		{
			Name:         "ts",
			Tp:           "timestamp(6)",
			DefaultValue: "current_timestamp(6)",
		},
		{
			Name: "name",
			Tp:   "varchar(100)",
		},
	}
	idxs := []data.IndexInfo{
		{
			Tp:      data.PrimaryKey,
			Columns: []string{"id"},
		},
	}
	childCols := append(cols, data.ColumnDef{
		Name: "pid",
		Tp:   "bigint",
	})
	childTblInfo, err := data.NewTableInfo(dbName, childTable, childCols, idxs)
	if err != nil {
		return nil, nil, err
	}
	parentTblInfo, err := data.NewTableInfo(dbName, parentTable, cols, idxs)
	return parentTblInfo, childTblInfo, err
}

func (c *FKPrepareSuite) Run() error {
	err := c.prepare()
	if err != nil {
		fmt.Println("prepare table meet error: ", err)
		return err
	}

	load := data.NewLoadDataSuite(c.cfg)
	load.SetBatchSize(1000)
	err = load.LoadData(c.parentTable, c.rows)
	if err != nil {
		fmt.Printf("insert data error: %v\n", err)
	}
	return nil
}
