package payload

import (
	"context"
	"fmt"
	"github.com/crazycs520/load/cmd"
	"github.com/crazycs520/load/config"
	"github.com/crazycs520/load/data"
	"github.com/crazycs520/load/util"
	"github.com/spf13/cobra"
	"strconv"
	"sync"
)

const (
	defRowsOfIndexLookUp = 100000
	defAggOfIndexLookUp  = true
)

type FullIndexLookUpSuite struct {
	cfg     *config.Config
	tblInfo *data.TableInfo

	rows int
	agg  bool
}

func NewFullIndexLookUpSuite(cfg *config.Config) cmd.CMDGenerater {
	return &FullIndexLookUpSuite{
		cfg:  cfg,
		rows: defRowsOfIndexLookUp,
		agg:  defAggOfIndexLookUp,
	}
}

func (c *FullIndexLookUpSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          fullIndexLookupSuiteName,
		Short:        "payload of full index lookup scan",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.rows, flagRows, "", defRowsOfIndexLookUp, "the table total rows")
	cmd.Flags().BoolVarP(&c.agg, flagAgg, "", defAggOfIndexLookUp, "full scan with TiKV return all rows if false, or do some aggregation if true")
	return cmd
}

func (c *FullIndexLookUpSuite) ParseCmd(combinedCmd string) bool {
	return ParsePayloadCmd(combinedCmd, fullIndexLookupSuiteName, func(flag, value string) error {
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

func (c *FullIndexLookUpSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *FullIndexLookUpSuite) prepare() error {
	tableName := "t_index_lookup"
	tblInfo, err := data.NewTableInfo(c.cfg.DBName, tableName, []data.ColumnDef{
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
	}, []data.IndexInfo{
		{
			Name:    "idx0",
			Tp:      data.NormalIndex,
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

func (c *FullIndexLookUpSuite) Run() error {
	ctx := context.Background()
	err := c.prepare()
	if err != nil {
		fmt.Println("prepare data meet error: ", err)
		return err
	}
	var query string
	if c.agg {
		query = fmt.Sprintf("select sum(a*b) from %v use index (idx0)", c.tblInfo.DBTableName())
	} else {
		query = fmt.Sprintf("select * from %v use index (idx0)", c.tblInfo.DBTableName())
	}
	fmt.Printf("start to do index lookup query: %v\n", query)
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

func (c *FullIndexLookUpSuite) exec(genSQL func() string) error {
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
