package payload

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

type IndexLookUpWrongPlan struct {
	cfg       *config.Config
	tableName string
	tblInfo   *data.TableInfo

	rows        int
	insertCount int64
}

func NewIndexLookUpWrongPlan(cfg *config.Config) cmd.CMDGenerater {
	return &IndexLookUpWrongPlan{
		cfg: cfg,
	}
}

func (c *IndexLookUpWrongPlan) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "index-lookup",
		Short:        "stress test for index lookup in wrong plan.",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.rows, "rows", "", 100000, "test table rows")
	return cmd
}

func (c *IndexLookUpWrongPlan) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *IndexLookUpWrongPlan) prepare() error {
	c.tableName = "t_index_lookup"
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
	}, []data.IndexInfo{
		{
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

func (c *IndexLookUpWrongPlan) Run() error {
	ctx := context.Background()
	err := c.prepare()
	if err != nil {
		fmt.Println("prepare data meet error: ", err)
		return err
	}
	query := fmt.Sprintf("select sum(a*b) from %v use index (idx0) where a < 1000000;", c.tblInfo.DBTableName())
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

func (c *IndexLookUpWrongPlan) exec(genSQL func() string) error {
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
