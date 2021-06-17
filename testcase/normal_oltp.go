package testcase

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"sync"

	"github.com/crazycs520/load/cmd"
	"github.com/crazycs520/load/config"
	"github.com/crazycs520/load/data"
	"github.com/crazycs520/load/util"
	"github.com/spf13/cobra"
)

type NormalOLTPSuite struct {
	cfg       *config.Config
	tableName string
	tblInfo   *data.TableInfo

	rows       int
	doInsert   bool
	doSelect   bool
	doPointGet bool
	doUpdate   bool
	ignore     bool
}

func NewNormalOLTPSuite(cfg *config.Config) cmd.CMDGenerater {
	return &NormalOLTPSuite{
		cfg: cfg,
	}
}

func (c *NormalOLTPSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "normal-oltp",
		Short:        "normal oltp test, such as select, point-get, insert, update",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.rows, "rows", "", 10000, "the initial rows size before test")
	cmd.Flags().BoolVarP(&c.doInsert, "insert", "", true, "do insert if the value is true")
	cmd.Flags().BoolVarP(&c.doUpdate, "update", "", true, "do update if the value is true")
	cmd.Flags().BoolVarP(&c.doSelect, "select", "", true, "do select if the value is true")
	cmd.Flags().BoolVarP(&c.doPointGet, "point-get", "", true, "do select with point-get if the value is true")
	cmd.Flags().BoolVarP(&c.ignore, "ignore", "", true, "ignore exec sql error if the value is true")
	return cmd
}

func (c *NormalOLTPSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *NormalOLTPSuite) prepare() error {
	c.tableName = "t_normal_oltp"
	tblInfo, err := data.NewTableInfo(c.cfg.DBName, c.tableName, []data.ColumnDef{
		{
			Name: "a",
			Tp:   "bigint",
		},
		{
			Name: "b",
			Tp:   "bigint",
		},
		{
			Name:         "c",
			Tp:           "timestamp(6)",
			DefaultValue: "current_timestamp(6)",
		},
		{
			Name: "d",
			Tp:   "varchar(50)",
		},
	}, []data.IndexInfo{
		{
			Tp:      data.UniqueIndex,
			Columns: []string{"a"},
		},
		{
			Tp:      data.NormalIndex,
			Columns: []string{"c", "d"},
		},
	})
	if err != nil {
		return err
	}
	c.tblInfo = tblInfo
	load := data.NewLoadDataSuit(c.cfg)
	return load.Prepare(tblInfo, c.rows, c.rows/2000)
}

func (c *NormalOLTPSuite) Run() error {
	err := c.prepare()
	if err != nil {
		fmt.Println("prepare table meet error: ", err)
		return err
	}

	loadStr := ""
	if c.doInsert {
		loadStr += " insert"
	}
	if c.doUpdate {
		loadStr += " update"
	}
	if c.doSelect {
		loadStr += " select"
	}
	if c.doPointGet {
		loadStr += " point-get"
	}
	fmt.Printf("start to run normal oltp load: %v\n", loadStr)
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.runSQLLoad()
		}()
	}
	wg.Wait()
	return nil
}

func (c *NormalOLTPSuite) runSQLLoad() {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	for {
		if c.doInsert {
			c.doInsertJob(db)
		}
		if c.doUpdate {
			c.doUpdateJob(db)
		}
		if c.doSelect {
			c.doSelectJob(db)
		}
		if c.doPointGet {
			c.doPointGetJob(db)
		}
	}
}

func (c *NormalOLTPSuite) doInsertJob(db *sql.DB) {
	cnt := c.tblInfo.AddInsertedRowSize(1)
	sql := c.tblInfo.GenInsertSQL(int(cnt))
	_, err := db.Exec(sql)
	c.handleError(err, sql)
}

func (c *NormalOLTPSuite) doUpdateJob(db *sql.DB) {
	cnt := rand.Intn(int(c.tblInfo.GetInsertedRowSize()))
	sql := fmt.Sprintf("update %v set b = b+1 where a = %v", c.tblInfo.DBTableName(), cnt)
	_, err := db.Exec(sql)
	c.handleError(err, sql)
}

func (c *NormalOLTPSuite) doSelectJob(db *sql.DB) {
	cnt := rand.Intn(int(c.tblInfo.GetInsertedRowSize()))
	sql := fmt.Sprintf("select sum(a+b) from %v where a >= %v and a <= %v", c.tblInfo.DBTableName(), cnt, cnt+10)
	_, err := db.Exec(sql)
	c.handleError(err, sql)
}

func (c *NormalOLTPSuite) doPointGetJob(db *sql.DB) {
	cnt := rand.Intn(int(c.tblInfo.GetInsertedRowSize()))
	sql := fmt.Sprintf("select * from %v where a = %v", c.tblInfo.DBTableName(), cnt)
	_, err := db.Exec(sql)
	c.handleError(err, sql)
}

func (c *NormalOLTPSuite) handleError(err error, hint string) {
	if err == nil {
		return
	}
	fmt.Println(err, hint)
	if c.ignore {
		return
	}
	os.Exit(1)
}
