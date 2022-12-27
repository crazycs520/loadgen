package payload

import (
	"fmt"
	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/data"
	"github.com/crazycs520/loadgen/util"
	"github.com/spf13/cobra"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type FKInsertChildSuite struct {
	cfg *config.Config

	rows       int
	parentRows int
	batch      int
	check      bool
	rand       bool

	insertedRows int64

	parentTable *data.TableInfo
}

func NewFKInsertChildSuite(cfg *config.Config) cmd.CMDGenerater {
	return &FKInsertChildSuite{
		cfg: cfg,
	}
}

func (c *FKInsertChildSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "fk-insert-child",
		Short:        "insert data into child table",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.rows, flagRows, "", 1000000, "the total insert child rows")
	cmd.Flags().IntVarP(&c.parentRows, "parent-row", "", 100000, "the total rows of parent")
	cmd.Flags().IntVarP(&c.batch, flagBatchSize, "", 100, "the batch size of insert")
	cmd.Flags().BoolVarP(&c.check, "fk-check", "", true, "whether enable foreign key checks")
	cmd.Flags().BoolVarP(&c.rand, "rand-pid", "", false, "whether use rand pid")
	return cmd
}

func (c *FKInsertChildSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *FKInsertChildSuite) Run() error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	db.Exec("truncate table fk_child;")
	_, childTblInfo, err := getParentAndChildTableInfo(c.cfg.DBName)
	if err != nil {
		return err
	}
	fmt.Printf("[%v] starting, fk-check: %v, batch-size: %v, rand: %v, parent-rows: %v,insert %v rows with %v threads into child table \n",
		time.Now().Format(time.RFC3339), c.check, c.batch, c.rand, c.parentRows, c.rows, c.cfg.Thread)
	start := time.Now()
	if c.cfg.Thread == 0 {
		c.cfg.Thread = 1
	}
	step := c.rows / c.cfg.Thread
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Thread; i++ {
		start := i * step
		end := start + step
		if end > c.rows {
			end = c.rows
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = c.insertIntoChildTable(childTblInfo, start, end, c.batch)
			if err != nil {
				fmt.Printf("insert failed, error: %v\n", err)
			}
		}()
	}
	go func() {
		last := time.Now()
		lastInserted := int64(0)
		for {
			time.Sleep(10 * time.Second)
			now := time.Now()
			insertedRows := atomic.LoadInt64(&c.insertedRows)
			total := insertedRows - lastInserted
			cost := now.Sub(last)
			last = now
			lastInserted = insertedRows
			qps := float64(total) / cost.Seconds()
			fmt.Printf("insert %.2f rows per second\n", qps)
		}
	}()
	wg.Wait()
	fmt.Printf("fk-check: %v, batch-size: %v, rand: %v, parent-rows: %v,insert %v rows with %v threads into child table cost %v \n",
		c.check, c.batch, c.rand, c.parentRows, c.rows, c.cfg.Thread, time.Since(start).String())
	return nil
}

func (c *FKInsertChildSuite) insertIntoChildTable(t *data.TableInfo, start, end, batch int) error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	if c.check {
		db.Exec("set @@foreign_key_checks=1;")
	} else {
		db.Exec("set @@foreign_key_checks=0;")
	}

	sql := t.GenPrepareInsertSQL(batch)
	stmt, err := db.Prepare(sql)
	if err != nil {
		return err
	}
	for i := start; i < end; i += batch {
		args := c.genPrepareInsertStmtArgs(t, batch, i)
		_, err = stmt.Exec(args...)
		if err != nil {
			return err
		}
		atomic.AddInt64(&c.insertedRows, int64(batch))
	}
	return err
}

func (c *FKInsertChildSuite) genPrepareInsertStmtArgs(t *data.TableInfo, batch, num int) []interface{} {
	args := make([]interface{}, 0, len(t.Columns)*batch)
	for row := 0; row < batch; row++ {
		num++
		for _, col := range t.Columns {
			var v interface{}
			if col.Name == "pid" {
				if c.rand {
					v = col.SeqValue(int64(rand.Intn(c.parentRows)))
				} else {
					v = col.SeqValue(int64(num % c.parentRows))
				}
			} else {
				v = col.SeqValue(int64(num))
			}
			args = append(args, v)
		}
	}
	return args
}
