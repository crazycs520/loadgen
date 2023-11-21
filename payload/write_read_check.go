package payload

import (
	"fmt"
	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/util"
	"github.com/spf13/cobra"
	"strings"
)

type WriteReadCheckSuite struct {
	cfg   *config.Config
	batch int
}

func NewWriteReadCheckSuite(cfg *config.Config) cmd.CMDGenerater {
	return &WriteReadCheckSuite{
		cfg: cfg,
	}
}

func (c *WriteReadCheckSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "write-read-check",
		Short:        "write-read-check workload",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.batch, flagBatch, "", 10000, "the total insert rows of each thread")
	return cmd
}

func (c *WriteReadCheckSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *WriteReadCheckSuite) Run() error {
	log("starting write-read-check workload, thread: %v", c.cfg.Thread)

	c.createTable()
	errCh := make(chan error, c.cfg.Thread)
	batch := 1000000
	for i := 0; i < c.cfg.Thread; i++ {
		start := i * batch
		end := (i + 1) * batch
		go func(start, end int) {
			err := c.runLoad(start, end)
			errCh <- err
		}(start, end)
	}
	for i := 0; i < c.cfg.Thread; i++ {
		err := <-errCh
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *WriteReadCheckSuite) createTable() error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	sqls := []string{
		`drop table if exists t1;`,
		`create table t1 (id varchar(64), val int, unique index id(id))`,
	}
	for _, sql := range sqls {
		err := execSQLWithLog(db, sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *WriteReadCheckSuite) runLoad(start, end int) error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	checkQueryResult := func(query string, expected string) error {
		result := ""
		err := util.QueryRows(db, query, func(row, cols []string) error {
			result = strings.Join(row, ",")
			return nil
		})
		if err != nil {
			return err
		}
		if result != expected {
			return fmt.Errorf("query with wrong result, expected: %v, actual: %v", expected, result)
		}
		return nil
	}
	for i := start; i < end; i++ {
		insert := fmt.Sprintf("insert into t1 values ('%v', %v)", i, i)
		err := execSQLWithLog(db, insert)
		if err != nil {
			return err
		}
		query := fmt.Sprintf("select * from t1 where id = '%v'", i)
		err = checkQueryResult(query, fmt.Sprintf("%v,%v", i, i))
		if err != nil {
			return err
		}

		update := fmt.Sprintf("update t1 set val = %v where id = '%v'", i+1, i)
		err = execSQLWithLog(db, update)
		if err != nil {
			return err
		}
		err = checkQueryResult(query, fmt.Sprintf("%v,%v", i, i+1))
		if err != nil {
			return err
		}
		delete := fmt.Sprintf("delete from t1 where id = '%v'", i)
		err = execSQLWithLog(db, delete)
		if err != nil {
			return err
		}
		err = checkQueryResult(query, "")
		if err != nil {
			return err
		}
	}
	return nil
}
