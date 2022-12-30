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

type FKDeleteParentSuite struct {
	cfg *config.Config

	rows          int
	check         bool
	manualCascade bool

	deletedRows int64
}

func NewFKDeleteParentSuite(cfg *config.Config) cmd.CMDGenerater {
	return &FKDeleteParentSuite{
		cfg: cfg,
	}
}

func (c *FKDeleteParentSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "fk-delete-parent",
		Short:        "delete from foreign key parent table",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.rows, flagRows, "", 100000, "the total insert rows")
	cmd.Flags().BoolVarP(&c.check, "fk-check", "", true, "whether enable foreign key checks")
	cmd.Flags().BoolVarP(&c.manualCascade, "manual-cascade", "", false, "whether manual cascade delete child table")
	return cmd
}

func (c *FKDeleteParentSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *FKDeleteParentSuite) Run() error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	fmt.Printf("[%v] starting delete parent table, fk-check: %v, manual-cascade: %v,  parent-rows: %v, thread: %v\n",
		time.Now().Format(time.RFC3339), c.check, c.manualCascade, c.rows, c.cfg.Thread)
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
			err := c.deleteFromParentTable(start, end)
			if err != nil {
				fmt.Printf("insert failed, error: %v\n", err)
			}
		}()
	}
	go func() {
		last := time.Now()
		lastDeleted := int64(0)
		for {
			time.Sleep(10 * time.Second)
			now := time.Now()
			deletedRows := atomic.LoadInt64(&c.deletedRows)
			total := deletedRows - lastDeleted
			cost := now.Sub(last)
			last = now
			lastDeleted = deletedRows
			qps := float64(total) / cost.Seconds()
			fmt.Printf("delete %.2f rows per second\n", qps)
		}
	}()
	wg.Wait()
	fmt.Printf("[%v] finish delete parent table, fk-check: %v, manual-cascade: %v,  parent-rows: %v, thread: %v, cost: %v, avg_ops: %.1f\n",
		time.Now().Format(time.RFC3339), c.check, c.manualCascade, c.rows, c.cfg.Thread, time.Since(start).String(), float64(c.rows)/time.Since(start).Seconds())
	return nil
}

func (c *FKDeleteParentSuite) deleteFromParentTable(start, end int) error {
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
		if c.manualCascade {
			txn, err := db.Begin()
			if err != nil {
				return err
			}
			sql := fmt.Sprintf("delete from fk_parent where id=%v;", i)
			_, err = txn.Exec(sql)
			if err != nil {
				return err
			}
			sql = fmt.Sprintf("delete from fk_child where pid=%v;", i)
			_, err = txn.Exec(sql)
			if err != nil {
				return err
			}
			err = txn.Commit()
			if err != nil {
				return err
			}
		} else {
			sql := fmt.Sprintf("delete from fk_parent where id=%v;", i)
			_, err := db.Exec(sql)
			if err != nil {
				return err
			}
		}
		atomic.AddInt64(&c.deletedRows, 1)
	}
	return nil
}
