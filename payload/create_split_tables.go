package payload

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/util"
)

type CreateSplitTablesSuite struct {
	cfg *config.Config

	tables  int
	regions int
}

func NewCreateSplitTablesSuite(cfg *config.Config) cmd.CMDGenerater {
	return &CreateSplitTablesSuite{
		cfg:     cfg,
		tables:  100,
		regions: 1000,
	}
}

func (c *CreateSplitTablesSuite) Name() string {
	return "create-split-tables"
}

func (c *CreateSplitTablesSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          c.Name(),
		Short:        "create many tables, then split regions for every table",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.tables, flagTables, "", 100, "the number of tables to create")
	cmd.Flags().IntVarP(&c.regions, flagRegions, "", 1000, "the number of regions to split for every table")
	return cmd
}

func (c *CreateSplitTablesSuite) ParseCmd(combinedCmd string) bool {
	return ParsePayloadCmd(combinedCmd, c.Name(), func(flag, value string) error {
		switch flag {
		case flagTables:
			v, err := strconv.Atoi(value)
			if err != nil {
				return err
			}
			c.tables = v
		case flagRegions:
			v, err := strconv.Atoi(value)
			if err != nil {
				return err
			}
			c.regions = v
		default:
			return fmt.Errorf("unknow flag %v", flag)
		}
		return nil
	})
}

func (c *CreateSplitTablesSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *CreateSplitTablesSuite) Run() error {
	if c.tables < 0 {
		return fmt.Errorf("%s must be >= 0", flagTables)
	}
	if c.regions <= 0 {
		return fmt.Errorf("%s must be > 0", flagRegions)
	}

	fmt.Printf("%s config: %s=%d, %s=%d, thread=%d\n",
		c.Name(), flagTables, c.tables, flagRegions, c.regions, c.cfg.Thread)

	if err := c.ensureDatabase(); err != nil {
		return err
	}

	totalStart := time.Now()

	createStart := time.Now()
	if err := c.createTables(); err != nil {
		return err
	}
	createCost := time.Since(createStart)
	fmt.Printf("create table wait cost: %v\n", createCost)

	splitStart := time.Now()
	if err := c.splitTables(); err != nil {
		return err
	}
	splitCost := time.Since(splitStart)

	fmt.Printf("split table wait cost: %v\n", splitCost)
	fmt.Printf("total cost: %v\n", time.Since(totalStart))
	return nil
}

func (c *CreateSplitTablesSuite) ensureDatabase() error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()

	_, err := db.Exec(fmt.Sprintf("create database if not exists `%s`", c.cfg.DBName))
	return err
}

func (c *CreateSplitTablesSuite) createTables() error {
	return c.runTasks(func() (func(int) error, func(), error) {
		db := util.GetSQLCli(c.cfg)
		runTask := func(idx int) error {
			_, err := db.Exec(c.createTableSQL(idx))
			return err
		}
		cleanup := func() {
			db.Close()
		}
		return runTask, cleanup, nil
	})
}

func (c *CreateSplitTablesSuite) splitTables() error {
	return c.runTasks(func() (func(int) error, func(), error) {
		db := util.GetSQLCli(c.cfg)
		runTask := func(idx int) error {
			_, err := db.Exec(c.splitTableSQL(idx))
			return err
		}
		cleanup := func() {
			db.Close()
		}
		return runTask, cleanup, nil
	})
}

func (c *CreateSplitTablesSuite) tableName(idx int) string {
	return fmt.Sprintf("t_%d", idx)
}

func (c *CreateSplitTablesSuite) createTableSQL(idx int) string {
	return fmt.Sprintf(
		"create table %s (id integer not null auto_increment, k integer default '0' not null, c char(120) default '' not null, pad char(60) default '' not null, primary key (id))",
		c.tableName(idx),
	)
}

func (c *CreateSplitTablesSuite) splitTableSQL(idx int) string {
	return fmt.Sprintf(
		"split table %s between (0) and (10000000) regions %d",
		c.tableName(idx),
		c.regions,
	)
}

func (c *CreateSplitTablesSuite) runTasks(newWorker func() (func(int) error, func(), error)) error {
	if c.tables == 0 {
		return nil
	}

	workers := c.cfg.Thread
	if workers <= 0 {
		workers = 1
	}

	taskCh := make(chan int, workers)
	stopCh := make(chan struct{})
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)

	stopWithError := func(err error) {
		if err == nil {
			return
		}
		errOnce.Do(func() {
			firstErr = err
			close(stopCh)
		})
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			runTask, cleanup, err := newWorker()
			if err != nil {
				stopWithError(err)
				return
			}
			if cleanup != nil {
				defer cleanup()
			}

			for {
				select {
				case <-stopCh:
					return
				case idx, ok := <-taskCh:
					if !ok {
						return
					}
					if err := runTask(idx); err != nil {
						stopWithError(err)
						return
					}
				}
			}
		}()
	}

sendLoop:
	for idx := 0; idx < c.tables; idx++ {
		select {
		case <-stopCh:
			break sendLoop
		case taskCh <- idx:
		}
	}
	close(taskCh)
	wg.Wait()
	return firstErr
}
