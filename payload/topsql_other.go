package payload

import (
	"fmt"
	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/data"
	"github.com/crazycs520/loadgen/util"
	"github.com/spf13/cobra"
	"strconv"
	"time"
)

type TopSQLOtherSuite struct {
	cfg     *config.Config
	tblInfo *data.TableInfo

	rows      int
	doPrepare bool
	doExecute bool
}

func NewTopSQLOtherSuite(cfg *config.Config) cmd.CMDGenerater {
	return &TopSQLOtherSuite{
		cfg:       cfg,
		rows:      1000000,
		doPrepare: false,
		doExecute: false,
	}
}

func (c *TopSQLOtherSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "topsql-other",
		Short:        "topsql other calculation loadgen",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.rows, flagRows, "", 1000000, "the initial rows size before test")
	cmd.Flags().BoolVarP(&c.doPrepare, flagPrepare, "", false, "do prepare")
	cmd.Flags().BoolVarP(&c.doExecute, flagExecute, "", false, "do execute")
	return cmd
}

func (c *TopSQLOtherSuite) ParseCmd(combinedCmd string) bool {
	return false
}

func (c *TopSQLOtherSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *TopSQLOtherSuite) prepare() error {
	for i := 0; i < 10; i++ {
		tableName := "t_" + strconv.Itoa(i)
		tblInfo, err := data.NewTableInfo(c.cfg.DBName, tableName, []data.ColumnDef{
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
				Name:    "idx0",
				Tp:      data.UniqueIndex,
				Columns: []string{"a"},
			},
		})
		if err != nil {
			return err
		}
		c.tblInfo = tblInfo
		load := data.NewLoadDataSuite(c.cfg)
		err = load.Prepare(tblInfo, c.rows, c.rows/20000)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *TopSQLOtherSuite) Run() error {
	if c.doPrepare {
		fmt.Println("start to do prepare")
		err := c.prepare()
		fmt.Printf("finish prepared, err: %v\n", err)
	}
	if c.doExecute {
		fmt.Println("start to do execute")
		c.execute()
	}
	return nil
}

func (c *TopSQLOtherSuite) execute() {
	// 3 big query
	for i := 0; i < 10; i++ {
		tableName := "t_" + strconv.Itoa(i)
		go func(tb string) {
			db := util.GetSQLCli(c.cfg)
			defer func() {
				db.Close()
			}()
			for {
				rows, err := db.Query(fmt.Sprintf("select count(*) from %v", tb))
				if err != nil {
					fmt.Printf("query error: %v\n", err)
					return
				}
				for rows.Next() {
				}
				err = rows.Close()
				if err != nil {
					fmt.Printf("query error: %v\n", err)
					return
				}
			}

		}(tableName)
	}
	time.Sleep(time.Hour * 24 * 30)
}
