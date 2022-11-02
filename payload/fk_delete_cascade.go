package payload

import (
	"database/sql"
	"fmt"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/util"
	"github.com/spf13/cobra"
)

type FKDeleteCascadeSuite struct {
	cfg       *config.Config
	db *sql.DB
}

func NewFKDeleteCascadeSuite(cfg *config.Config) cmd.CMDGenerater {
	return &FKDeleteCascadeSuite{
		cfg: cfg,
	}
}

func (c *FKDeleteCascadeSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "fk-delete-cascade",
		Short:        "payload of insert with foreign key check",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	return cmd
}

func (c *FKDeleteCascadeSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *FKDeleteCascadeSuite) prepare() error {
	c.db = util.GetSQLCli(c.cfg)
	prepareSQLs := []string{
		"set @@global.tidb_enable_foreign_key=1",
		"set @@foreign_key_checks=1",
		"drop table if exists t1,t2",
		"create table t1 (id int key, name varchar(10));",
		"create table t2 (id int, pid int, unique index(id), foreign key fk(pid) references t1(id) ON UPDATE CASCADE ON DELETE CASCADE);",
		"insert into t1 values (0, ''), (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'c'), (6, ''), (7, 'a'), (8, 'b'), (9, 'c'), (10, 'd')",
		"insert into t2 values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10)",
	}
	for _,sql := range prepareSQLs{
		_, err := c.db.Exec(sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *FKDeleteCascadeSuite) Run() error {
	err := c.prepare()
	if err != nil {
		fmt.Println("prepare table meet error: ", err)
		return err
	}
	fmt.Println("started")
	for {
		c.db.Exec("begin")
		_,err := c.db.Exec("delete from t1")
		if err != nil {
			fmt.Println("exec meet error: ", err)
			return err
		}
		c.db.Exec("rollback")
	}
}
