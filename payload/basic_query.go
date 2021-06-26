package payload

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/crazycs520/load/config"
	"github.com/crazycs520/load/data"
	"github.com/crazycs520/load/util"
	"github.com/spf13/cobra"
)

const (
	defRowsOfBasicQuery = 100000
	defAggOfBasicQuery  = true
)

type QuerySuite interface {
	Name() string
	GenQuerySQL() string
}

type basicQuerySuite struct {
	cfg        *config.Config
	tblInfo    *data.TableInfo
	querySuite QuerySuite

	rows int
	agg  bool
}

func NewBasicQuerySuite(cfg *config.Config, querySuite QuerySuite) *basicQuerySuite {
	return &basicQuerySuite{
		cfg:        cfg,
		querySuite: querySuite,
		rows:       defRowsOfBasicQuery,
		agg:        defAggOfBasicQuery,
	}
}

func (c *basicQuerySuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          c.querySuite.Name(),
		Short:        "payload of " + c.querySuite.Name(),
		RunE:         c.RunE,
		SilenceUsage: true,
	}

	cmd.Flags().IntVarP(&c.rows, flagRows, "", defRowsOfBasicQuery, "the table total rows")
	cmd.Flags().BoolVarP(&c.agg, flagAgg, "", defAggOfBasicQuery, "full scan with TiKV return all rows if false, or do some aggregation if true")
	return cmd
}

func (c *basicQuerySuite) ParseCmd(combinedCmd string) bool {
	return ParsePayloadCmd(combinedCmd, c.querySuite.Name(), func(flag, value string) error {
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

func (c *basicQuerySuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *basicQuerySuite) prepare() error {
	tableName := "t_" + strings.ReplaceAll(c.querySuite.Name(), "-", "_")
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
		{
			Name: "e",
			Tp:   "decimal(48,10)",
		},
	}, []data.IndexInfo{
		{
			Name:    "idx0",
			Tp:      data.NormalIndex,
			Columns: []string{"b"},
		},
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
	return load.Prepare(tblInfo, c.rows, c.rows/100000)
}

func (c *basicQuerySuite) Run() error {
	fmt.Printf("%v config: %v: %v, %v: %v\n", c.querySuite.Name(), flagRows, c.rows, flagAgg, c.agg)
	ctx := context.Background()
	err := c.prepare()
	if err != nil {
		fmt.Println("prepare data meet error: ", err)
		return err
	}
	query := c.querySuite.GenQuerySQL()
	fmt.Printf("start to query: %v\n", query)
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

func (c *basicQuerySuite) exec(genSQL func() string) error {
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
