package payload

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/crazycs520/load/cmd"
	"github.com/crazycs520/load/config"
	"github.com/crazycs520/load/data"
	"github.com/crazycs520/load/util"
	"github.com/spf13/cobra"
)

type FullTableScanSuite struct {
	cfg       *config.Config
	tableName string
	tblInfo   *data.TableInfo

	rows int
	agg  bool
}

func NewFullTableScanSuite(cfg *config.Config) cmd.CMDGenerater {
	return &FullTableScanSuite{
		cfg:  cfg,
		rows: defRowsOfFullTableScan,
		agg:  defAggOfFullTableScan,
	}
}

func (c *FullTableScanSuite) ParseCmd(combinedCmd string) bool {
	return ParsePayloadCmd(combinedCmd, fullTableScanSuiteName, func(flag, value string) error {
		switch flag {
		case flagRows:
			v, err := strconv.Atoi(value)
			if err != nil {
				return err
			}
			c.rows = v
		case flagAgg:
			v, err := strconv.ParseBool(value)
			if err != nil {
				return err
			}
			c.agg = v
		default:
			return fmt.Errorf("unknow flag %v", flag)
		}
		return nil
	})
}

const (
	defRowsOfFullTableScan = 1000000
	defAggOfFullTableScan  = true
)

func (c *FullTableScanSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          fullTableScanSuiteName,
		Short:        "payload of full table scan",
		RunE:         c.RunE,
		SilenceUsage: true,
	}

	cmd.Flags().IntVarP(&c.rows, flagRows, "", defRowsOfFullTableScan, "test table rows")
	cmd.Flags().BoolVarP(&c.agg, flagAgg, "", defAggOfFullTableScan, "full table scan with TiKV return all rows if false, or do many aggregation if true")
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
	fmt.Printf("%v config: %v: %v, %v: %v\n", fullTableScanSuiteName, flagRows, c.rows, flagAgg, c.agg)
	ctx := context.Background()
	err := c.prepare()
	if err != nil {
		fmt.Println("prepare data meet error: ", err)
		return err
	}
	var query string
	if c.agg {
		query = fmt.Sprintf("select sum(a+b+e), sum(a*b), sum(a*e), sum(b*e), sum(a*b*e) from %v;", c.tblInfo.DBTableName())
	} else {
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
