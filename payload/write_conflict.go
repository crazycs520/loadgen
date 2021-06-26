package payload

import (
	"fmt"
	"github.com/crazycs520/load/cmd"
	"github.com/crazycs520/load/config"
	"github.com/crazycs520/load/data"
	"github.com/crazycs520/load/util"
	"github.com/spf13/cobra"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
)

type WriteConflictSuite struct {
	cfg       *config.Config
	tableName string
	tblInfo   *data.TableInfo

	probability int
	conflictErr int64
}

func NewWriteConflictSuite(cfg *config.Config) cmd.CMDGenerater {
	return &WriteConflictSuite{
		cfg: cfg,
	}
}

func (c *WriteConflictSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "write-conflict",
		Short:        "test write conflict case",
		Long:         `test for write conflict case`,
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.probability, "probability", "", 100, "conflict probability, rand( n )")
	return cmd
}

func (c *WriteConflictSuite) RunE(cmd *cobra.Command, args []string) error {
	fmt.Printf("probability: %v\nconcurrency: %v\n", c.probability, c.cfg.Concurrency)
	return c.Run()
}

func (c *WriteConflictSuite) prepare() error {
	c.tableName = "t_write_conflict"
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

func (c *WriteConflictSuite) Run() error {
	err := c.prepare()
	if err != nil {
		return err
	}
	fmt.Printf("start to do write conflict load: %v\n", c.genSQL())
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			err := c.update()
			if err != nil {
				fmt.Println(err.Error())
			}
		}()
	}
	wg.Wait()
	return nil
}

func (c *WriteConflictSuite) update() error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	for {
		sql := c.genSQL()
		_, err := db.Exec(sql)
		if err != nil {
			if strings.Contains(err.Error(), "Write conflict") {
				atomic.AddInt64(&c.conflictErr, 1)
				continue
			}
			return err
		}
	}
}

func (c *WriteConflictSuite) genSQL() string {
	id := rand.Intn(c.probability)
	return fmt.Sprintf("insert into %v (a,b) values (%v, %v) on duplicate key update b=b+1;", c.tblInfo.DBTableName(), id, 1)
}
