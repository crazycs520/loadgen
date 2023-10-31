package payload

import (
	"bytes"
	"database/sql"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/util"
	"github.com/spf13/cobra"
)

type Oncall6533Suite struct {
	cfg *config.Config
}

func NewOncall6533Suite(cfg *config.Config) cmd.CMDGenerater {
	return &Oncall6533Suite{
		cfg: cfg,
	}
}

func (c *Oncall6533Suite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "oncall-6533",
		Short:        "oncall-6533 workload",
		RunE:         c.RunE,
		SilenceUsage: true,
	}

	return cmd
}

func (c *Oncall6533Suite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *Oncall6533Suite) Run() error {
	log("starting oncall-6533 workload")

	dbs := make([]*sql.DB, 0, 10)
	for i := 0; i < 10; i++ {
		db := util.GetSQLCli(c.cfg)
		execSQLWithLog(db, "use test;")
		dbs = append(dbs, db)
	}
	defer func() {
		for _, db := range dbs {
			db.Close()
		}
	}()

	totalRows := 20000
	c.createTable(dbs[0])
	c.insertRows(dbs[0], totalRows)

	batch := 10
	for i := 0; i < totalRows; i += batch {
		tasks := c.genUpdateIdxs(i, batch)
		var wg sync.WaitGroup
		wg.Add(len(tasks))
		for i := range tasks {
			go func(id int, task []int) {
				defer wg.Done()
				c.updateRows(dbs[id], task)
			}(i+1, tasks[i])
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Microsecond * time.Duration(rand.Intn(5000)))
			sql := fmt.Sprintf("select count(*) from t where info>= '%v' and info <= '%v';", c.genInfo(i), c.genInfo(i+batch))
			util.QueryRows(dbs[0], sql, func(row, cols []string) error {
				return nil
			})
		}()
		wg.Wait()
		err := execSQLWithLog(dbs[0], "admin check table t;")
		if err != nil {
			log("admin check table t failed, err: %v", err)
			return err
		}
	}
	return nil
}

func (c *Oncall6533Suite) genUpdateIdxs(start int, batch int) [][]int {
	tasks := make([][]int, rand.Intn(4)+2)
	for i := 0; i < batch; i++ {
		idx := rand.Intn(len(tasks))
		tasks[idx] = append(tasks[idx], start+i)
	}
	for i := 0; i < len(tasks); i++ {
		v := rand.Intn(batch) + start
		exist := false
		for _, tv := range tasks[i] {
			if tv == v {
				exist = true
				break
			}
		}
		if exist {
			idx := (i + 1) % len(tasks)
			tasks[idx] = append(tasks[idx], v)
		} else {
			tasks[i] = append(tasks[i], v)
		}
	}
	return tasks
}

func (c *Oncall6533Suite) createTable(db *sql.DB) error {
	sqls := []string{
		`drop table if exists t;`,
		`create table t (info varchar(32), tp varchar(15), update_by varchar(45), update_date datetime default current_timestamp, task_status varchar(1) , primary key(info), index idx(tp, update_date, task_status, update_by));`,
	}
	for _, sql := range sqls {
		err := execSQLWithLog(db, sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Oncall6533Suite) insertRows(db *sql.DB, rows int) error {
	batch := 500
	values := bytes.NewBuffer(nil)
	cnt := 0
	for i := 0; i < rows; i++ {
		if values.Len() > 0 {
			values.WriteString(",")
		}
		values.WriteString(fmt.Sprintf("('%s', '%v', '%v', %v)", c.genInfo(i), c.genTp(i), c.genTaskStatus(i), "now() - interval 1 hour"))
		cnt++
		if cnt == batch {
			sql := `insert into t (info, tp, task_status, update_date) values ` + values.String() + `;`
			err := execSQLWithLog(db, sql)
			if err != nil {
				return err
			}
			cnt = 0
			values.Reset()
		}
	}
	if values.Len() > 0 {
		sql := `insert into t (info, tp, task_status, update_date) values ` + values.String() + `;`
		err := execSQLWithLog(db, sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Oncall6533Suite) updateRows(db *sql.DB, idxs []int) error {
	if len(idxs) == 0 {
		return nil
	}
	infos := make([]string, len(idxs))
	for i, v := range idxs {
		infos[i] = c.genInfo(v)
	}
	updateBy := c.genUpdateBy(rand.Intn(10))
	sql := fmt.Sprintf("update t set t.update_date = now(), t.update_by = '%v' where t.info in ('%s') and IFNULL(t.update_by, '') = '';",
		updateBy, strings.Join(infos, "', '"))

	return execSQLWithLog(db, sql)
}

func execSQLWithLog(db *sql.DB, sql string, args ...any) error {
	start := time.Now()
	_, err := db.Exec(sql, args...)
	log("exec sql: %v, err: %v, cost: %s", sql, err, time.Since(start).String())
	return err
}

func log(format string, a ...any) {
	msg := fmt.Sprintf(format, a...)
	fmt.Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), msg)
}

func (c *Oncall6533Suite) genInfo(idx int) string {
	return fmt.Sprintf("info_%v", idx)
}

func (c *Oncall6533Suite) genTp(idx int) string {
	return fmt.Sprintf("tp_%v", idx%5)
}

func (c *Oncall6533Suite) genUpdateBy(idx int) string {
	return fmt.Sprintf("user_%v", idx)
}

func (c *Oncall6533Suite) genTaskStatus(idx int) string {
	return strconv.Itoa((idx % 3) + 1)
}

func (c *Oncall6533Suite) genUpdateDate() string {
	return "now()"
}
