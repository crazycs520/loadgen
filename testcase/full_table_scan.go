package testcase

import (
	"context"
	"fmt"
	"github.com/crazycs520/load/cmd"
	"github.com/crazycs520/load/config"
	"github.com/crazycs520/load/data"
	"github.com/crazycs520/load/util"
	"github.com/spf13/cobra"
	"sync"
)

type FullTableScanSuite struct {
	cfg       *config.Config
	tableName string
	tblInfo   *data.TableInfo

	rows         int
	networkIO    bool
	copCalculate bool
}

func NewFullTableScanSuite(cfg *config.Config) cmd.CMDGenerater {
	return &FullTableScanSuite{
		cfg: cfg,
	}
}

func (c *FullTableScanSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "full-table-scan",
		Short:        "stress test for full table scan",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.rows, "rows", "", 1000000, "test table rows")
	cmd.Flags().BoolVarP(&c.networkIO, "net-io", "", false, "full table scan with TiKV return all rows that cause too many network IO")
	cmd.Flags().BoolVarP(&c.copCalculate, "cop-calculate", "", true, "full table scan with TiKV do many calculation")
	return cmd
}

func (c *FullTableScanSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *FullTableScanSuite) prepare() error {
	c.tableName = "t_full_table_scan"
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
			Name: "c",
			Tp:   "timestamp(6)",
		},
		{
			Name: "d",
			Tp:   "varchar(100)",
		},
		{
			Name: "e",
			Tp:   "decimal(48,10)",
		},
	}, []data.IndexInfo{
		{
			Tp:      data.PrimaryKey,
			Columns: []string{"a"},
		},
	})
	if err != nil {
		return err
	}
	c.tblInfo = tblInfo
	load := data.NewLoadDataSuit(c.cfg)
	return load.Prepare(tblInfo, c.rows, c.rows/2000)
}

func (c *FullTableScanSuite) Run() error {
	ctx := context.Background()
	err := c.prepare()
	if err != nil {
		fmt.Println("prepare data meet error: ", err)
		return err
	}
	var query string
	if c.copCalculate {
		query = fmt.Sprintf("select sum(a+b+e), sum(a*b), sum(a*e), sum(b*e), sum(a*b*e) from %v;", c.tblInfo.DBTableName())
	}
	if c.networkIO {
		query = fmt.Sprintf("select * from %v;", c.tblInfo.DBTableName())
	}
	fmt.Printf("start to do full table scan query: %v\n", query)
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := execSQLLoop(ctx, c.cfg, func() string {
				return query
			})
			if err != nil {
				fmt.Println(err.Error())
			}
		}()
	}
	wg.Wait()
	return nil
}

func (c *FullTableScanSuite) exec(genSQL func() string) error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	for {
		sql := genSQL()
		_, err := db.Exec(sql)
		if err != nil {
			return err
		}
	}
}
