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
	"sync"
	"time"
)

type WriteReadCheckSuite struct {
	cfg            *config.Config
	batch          int
	logSQL         bool
	blobColumnSize int
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
	cmd.Flags().BoolVarP(&c.logSQL, "log", "", false, "print sql log?")
	cmd.Flags().IntVarP(&c.blobColumnSize, "blob-column-size", "", 1024, "the blob column size")
	return cmd
}

func (c *WriteReadCheckSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *WriteReadCheckSuite) Run() error {
	log("starting write-read-check workload, thread: %v", c.cfg.Thread)

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
		`create table t1 (id varchar(64), val int, txt blob, index idx1(id), index idx2(val), index idx3(txt(10)), index idx4(txt(20)), index idx5(txt(50)), index idx6(txt(100)));`,
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

func (c *WriteReadCheckSuite) runLoad(start, end int) error {
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
	for i := start; i < end; i++ {
		txt := genRandStr(c.blobColumnSize)
		insert := fmt.Sprintf("insert into t1 values ('%v', %v, '%v')", i, i, txt)
		err := c.execSQLWithLog(db, insert)
		if err != nil {
			return err
		}
		query := fmt.Sprintf("select id,val from t1 where id = '%v'", i)
		err = checkQueryResult(query, fmt.Sprintf("%v,%v", i, i))
		if err != nil {
			return err
		}

		update := fmt.Sprintf("update t1 set val = %v where id = '%v'", i+1, i)
		err = c.execSQLWithLog(db, update)
		if err != nil {
			return err
		}
		queryResultAfterUpdate := fmt.Sprintf("%v,%v", i, i+1)
		err = checkQueryResult(query, queryResultAfterUpdate)
		if err != nil {
			return err
		}
		var wg sync.WaitGroup
		var deleteError error
		wg.Add(1)
		go func() {
			defer wg.Done()
			delete := fmt.Sprintf("delete from t1 where id = '%v'", i)
			deleteError = c.execSQLWithLog(db2, delete)
		}()
		result := ""
		err = util.QueryRows(db, query, func(row, cols []string) error {
			result = strings.Join(row, ",")
			return nil
		})
		if err != nil {
			return err
		}
		if result != "" && result != queryResultAfterUpdate {
			return fmt.Errorf("query with wrong result, expected1: %v, expected2: %v actual: %v", "", queryResultAfterUpdate, result)
		}
		wg.Wait()
		if deleteError != nil {
			return err
		}
		err = checkQueryResult(query, "")
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *WriteReadCheckSuite) execSQLWithLog(db *sql.DB, sql string, args ...any) error {
	start := time.Now()
	_, err := db.Exec(sql, args...)
	if err != nil || c.logSQL {
		log("exec sql: %v, err: %v, cost: %s", sql, err, time.Since(start).String())
	}
	return err
}

const charSet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._"

func genRandStr(length int) string {
	buf := make([]byte, 0, length)
	for len(buf) < length {
		n := rand.Int()
		for n > 0 && len(buf) < length {
			v := charSet[n%len(charSet)]
			buf = append(buf, byte(v))
			n /= len(charSet)
		}
	}
	return string(buf)
}
