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

const (
	defRowsOfFullIndexScan = 1000000
	defAggOfFullIndexScan  = true
)

type FullIndexScanSuite struct {
	cfg       *config.Config
	tableName string
	tblInfo   *data.TableInfo

	rows int
	agg  bool
}

func NewFullIndexScanSuite(cfg *config.Config) cmd.CMDGenerater {
	return &FullIndexScanSuite{
		cfg:  cfg,
		rows: defRowsOfFullIndexScan,
		agg:  defAggOfFullIndexScan,
	}
}

func (c *FullIndexScanSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          fullIndexScanSuiteName,
		Short:        "payload of full table scan",
		RunE:         c.RunE,
		SilenceUsage: true,
	}

	cmd.Flags().IntVarP(&c.rows, flagRows, "", defRowsOfFullIndexScan, "the table total rows")
	cmd.Flags().BoolVarP(&c.agg, flagAgg, "", defAggOfFullIndexScan, "full scan with TiKV return all rows if false, or do some aggregation if true")
	return cmd
}

func (c *FullIndexScanSuite) ParseCmd(combinedCmd string) bool {
	return ParsePayloadCmd(combinedCmd, fullIndexScanSuiteName, func(flag, value string) error {
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

func (c *FullIndexScanSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *FullIndexScanSuite) prepare() error {
	c.tableName = "t_full_index_scan"
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
		{
			Name:    "idx1",
			Tp:      data.NormalIndex,
			Columns: []string{"b"},
		},
	})
	if err != nil {
		return err
	}
	c.tblInfo = tblInfo
	load := data.NewLoadDataSuit(c.cfg)
	return load.Prepare(tblInfo, c.rows, c.rows/2000)
}

func (c *FullIndexScanSuite) Run() error {
	fmt.Printf("%v config: %v: %v, %v: %v\n", fullIndexScanSuiteName, flagRows, c.rows, flagAgg, c.agg)
	ctx := context.Background()
	err := c.prepare()
	if err != nil {
		fmt.Println("prepare data meet error: ", err)
		return err
	}
	var query string
	if c.agg {
		query = fmt.Sprintf("select sum(b), avg(b), min(b), max(b) from %v use index (idx1);", c.tblInfo.DBTableName())
	} else {
		query = fmt.Sprintf("select b from %v use index (idx1);", c.tblInfo.DBTableName())
	}
	fmt.Printf("start to do full index scan query: %v\n", query)
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

func (c *FullIndexScanSuite) exec(genSQL func() string) error {
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
