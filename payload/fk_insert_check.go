package payload

import (
	"bytes"
	"database/sql"
	"fmt"

	"github.com/crazycs520/loadgen/util"
	"github.com/spf13/cobra"
	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
)

type FKInsertCheckSuite struct {
	cfg       *config.Config
	db *sql.DB
}

func NewFKInsertCheckSuite(cfg *config.Config) cmd.CMDGenerater {
	return &FKInsertCheckSuite{
		cfg: cfg,
	}
}

func (c *FKInsertCheckSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "fk-insert-check",
		Short:        "payload of insert with foreign key check",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	return cmd
}

func (c *FKInsertCheckSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *FKInsertCheckSuite) prepare() error {
	c.db = util.GetSQLCli(c.cfg)
	prepareSQLs := []string{
		"set @@global.tidb_enable_foreign_key=1",
		"set @@foreign_key_checks=1",
		"drop table if exists t1,t2",
		"create table t1 (id int key, name varchar(10));",
		"create table t2 (id int, pid int, unique index(id), foreign key fk(pid) references t1(id) ON UPDATE CASCADE ON DELETE CASCADE);",
		"insert into t1 values (0, ''), (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'c')",
	}
	for _,sql := range prepareSQLs{
		_, err := c.db.Exec(sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *FKInsertCheckSuite) Run() error {
	err := c.prepare()
	if err != nil {
		fmt.Println("prepare table meet error: ", err)
		return err
	}
	fmt.Println("started")
	cnt :=0
	for {
		cnt++
		c.db.Exec("begin")
		buf := bytes.NewBuffer(nil)
		buf.WriteString("insert into t2 values ")
		for i:=0;i<20;i++{
			if i>0 {
				buf.WriteString(", ")
			}
			buf.WriteString(fmt.Sprintf("(%v, %v)", i+cnt, (i+cnt)%6))
		}
		buf.WriteString(" on duplicate key update id=id+1")
		_,err := c.db.Exec(buf.String())
		if err != nil {
			fmt.Println("exec meet error: ", err)
			return err
		}
		c.db.Exec("rollback")
	}
}

