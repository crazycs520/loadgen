package payload

import (
	"database/sql"
	"fmt"
	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/util"
	"github.com/spf13/cobra"
	"math/rand"
	"strings"
	"time"
)

type WriteReadCheck2Suite struct {
	cfg            *config.Config
	batch          int
	logSQL         bool
	blobColumnSize int
	loadCases      int
}

func NewWriteReadCheck2Suite(cfg *config.Config) cmd.CMDGenerater {
	return &WriteReadCheck2Suite{
		cfg: cfg,
	}
}

func (c *WriteReadCheck2Suite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "write-read-check2",
		Short:        "write-read-check2 workload",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.batch, flagBatch, "", 10000, "the total insert rows of each thread")
	cmd.Flags().BoolVarP(&c.logSQL, "log", "", false, "print sql log?")
	cmd.Flags().IntVarP(&c.blobColumnSize, "blob-column-size", "", 1024, "the blob column size")
	cmd.Flags().IntVarP(&c.loadCases, "load", "", 0, "the blob column size")
	return cmd
}

func (c *WriteReadCheck2Suite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *WriteReadCheck2Suite) Run() error {
	log("starting write-read-check workload, thread: %v, load-case: %v", c.cfg.Thread, c.loadCases)

	err := c.createTable()
	if err != nil {
		return err
	}
	errCh := make(chan error, c.cfg.Thread)
	batch := 1000000
	for i := 0; i < c.cfg.Thread; i++ {
		start := i * batch
		end := (i + 1) * batch
		go func(start, end int) {
			var err error
			switch c.loadCases {
			case 1:
				err = c.runLoad1(start, end)
			case 2:
				err = c.runLoad2(start, end)
			default:
				err = c.runLoad0(start, end)
			}
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

func (c *WriteReadCheck2Suite) createTable() error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	sqls := []string{
		`drop table if exists t1;`,
		`create table t1 (pk varchar(64), id varchar(64), val int, txt blob, unique index (pk), index idx1(id), index idx2(val), index idx3(txt(10)), index idx4(txt(20)), index idx5(txt(50)), index idx6(txt(100)));`,
		`split table t1 between (0) and (200000000) regions 200;`,
		`split table t1 index idx1 by ('');`,
		`split table t1 index idx2 by (1);`,
		`split table t1 index idx3 by ('');`,
		`split table t1 index idx4 by ('');`,
		`split table t1 index idx5 by ('');`,
		`split table t1 index idx6 by ('');`,
	}
	for _, sql := range sqls {
		err := c.execSQLWithLog(db, sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *WriteReadCheck2Suite) runLoad0(start, end int) error {
	db := util.GetSQLCli(c.cfg)
	db2 := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
		db2.Close()
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
	for i := start; i < end; i += 2 {
		txt := genRandStr(c.blobColumnSize)
		insert := fmt.Sprintf("insert into t1 values ('%v','%v', %v, '%v')", i, i, i, txt)
		err := c.execSQLWithLog(db, insert)
		if err != nil {
			return err
		}
		update := fmt.Sprintf("update t1 set val = %v where id = '%v'", i+1, i)
		err = c.execSQLWithLog(db, update)
		if err != nil {
			return err
		}
		query := fmt.Sprintf("select id,val from t1 where id = '%v'", i)
		err = checkQueryResult(query, fmt.Sprintf("%v,%v", i, i+1))
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *WriteReadCheck2Suite) runLoad1(start, end int) error {
	db := util.GetSQLCli(c.cfg)
	db2 := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
		db2.Close()
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
	for i := start; i < end; i += 2 {
		txt := genRandStr(c.blobColumnSize)
		insert := fmt.Sprintf("insert into t1 values ('%v','%v', %v, '%v')", i, i, i, txt)
		err := c.execSQLWithLog(db, insert)
		if err != nil {
			return err
		}
		update := fmt.Sprintf("update t1 set val = %v where id = '%v'", i+1, i)
		err = c.execSQLWithLog(db, update)
		if err != nil {
			return err
		}
		delete := fmt.Sprintf("delete from t1 where pk = '%v' and val = %v", i, i)
		if rand.Intn(10) < 5 {
			delete = fmt.Sprintf("delete from t1 where id = '%v' and val = %v", i, i)
		}
		err = c.execSQLWithLog(db, delete)
		if err != nil {
			return err
		}

		query := fmt.Sprintf("select id,val from t1 where id = '%v'", i)
		err = checkQueryResult(query, fmt.Sprintf("%v,%v", i, i+1))
		if err != nil {
			return err
		}

		step := 100
		if i%step == 0 {
			// delete old data
			begin := i - step
			if begin < start {
				begin = start
			}
			delete = fmt.Sprintf("delete from t1 where id >= '%v' and id < '%v'", begin, i-10)
			err = c.execSQLWithLog(db, delete)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *WriteReadCheck2Suite) runLoad2(start, end int) error {
	db := util.GetSQLCli(c.cfg)
	db2 := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
		db2.Close()
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
	for i := start; i < end; i += 2 {
		txt := genRandStr(c.blobColumnSize)
		insert := fmt.Sprintf("insert into t1 values ('%v','%v', %v, '%v')", i, i, i, txt)
		err := c.execSQLWithLog(db, insert)
		if err != nil {
			return err
		}
		update := fmt.Sprintf("update t1 set val = %v where id = '%v'", i+1, i)
		err = c.execSQLWithLog(db, update)
		if err != nil {
			return err
		}
		delete := fmt.Sprintf("delete from t1 where pk = '%v' and val = %v", i, i)
		if rand.Intn(10) < 5 {
			delete = fmt.Sprintf("delete from t1 where id = '%v' and val = %v", i, i)
		}
		err = c.execSQLWithLog(db, delete)
		if err != nil {
			return err
		}

		query := fmt.Sprintf("select id,val from t1 where id = '%v'", i)
		err = checkQueryResult(query, fmt.Sprintf("%v,%v", i, i+1))
		if err != nil {
			err = checkQueryResult("admin check table t1", "")
			if err != nil {
				return err
			}
		}

		step := 100
		if i%step == 0 {
			// delete old data
			begin := i - step
			if begin < start {
				begin = start
			}
			delete = fmt.Sprintf("delete from t1 where id >= '%v' and id < '%v'", begin, i-10)
			err = c.execSQLWithLog(db, delete)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *WriteReadCheck2Suite) execSQLWithLog(db *sql.DB, sql string, args ...any) error {
	start := time.Now()
	_, err := db.Exec(sql, args...)
	if err != nil || c.logSQL {
		log("exec sql: %v, err: %v, cost: %s", sql, err, time.Since(start).String())
	}
	if err != nil {
		return fmt.Errorf("exec sql: %v failed, err: %v", sql, err)
	}
	return nil
}
