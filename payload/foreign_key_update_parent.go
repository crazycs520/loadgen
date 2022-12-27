package payload

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/util"
	"github.com/spf13/cobra"
)

type FKUpdateParentSuite struct {
	cfg *config.Config

	rows  int
	check bool

	updatedRows int64
}

func NewFKUpdateParentSuite(cfg *config.Config) cmd.CMDGenerater {
	return &FKUpdateParentSuite{
		cfg: cfg,
	}
}

func (c *FKUpdateParentSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "fk-update-parent",
		Short:        "update from foreign key parent table",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.rows, flagRows, "", 100000, "the total insert rows")
	cmd.Flags().BoolVarP(&c.check, "fk-check", "", true, "whether enable foreign key checks")

	return cmd
}

func (c *FKUpdateParentSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *FKUpdateParentSuite) Run() error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	fmt.Printf("[%v] starting update parent table, fk-check: %v,  parent-rows: %v, thread: %v\n",
		time.Now().Format(time.RFC3339), c.check, c.rows, c.cfg.Thread)
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
			err := c.updateFromParentTable(start, end)
			if err != nil {
				fmt.Printf("insert failed, error: %v\n", err)
			}
		}()
	}
	go func() {
		last := time.Now()
		lastUpdated := int64(0)
		for {
			time.Sleep(10 * time.Second)
			now := time.Now()
			updatedRows := atomic.LoadInt64(&c.updatedRows)
			total := updatedRows - lastUpdated
			cost := now.Sub(last)
			last = now
			lastUpdated = updatedRows
			qps := float64(total) / cost.Seconds()
			fmt.Printf("update %.2f rows per second\n", qps)
		}
	}()
	wg.Wait()
	fmt.Printf("[%v] finish update parent table, fk-check: %v,  parent-rows: %v, thread: %v, cost: %v\n",
		time.Now().Format(time.RFC3339), c.check, c.rows, c.cfg.Thread, time.Since(start).String())
	return nil
}

func (c *FKUpdateParentSuite) updateFromParentTable(start, end int) error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	if c.check {
		db.Exec("set @@foreign_key_checks=1;")
	} else {
		db.Exec("set @@foreign_key_checks=0;")
	}

	for i := start; i < end; i += 1 {
		sql := fmt.Sprintf("update fk_parent set id=id+200000000 where id=%v;", i)
		_, err := db.Exec(sql)
		if err != nil {
			return err
		}
		atomic.AddInt64(&c.updatedRows, 1)
	}
	return nil
}
