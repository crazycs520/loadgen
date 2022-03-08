package payload

import (
	"fmt"
	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/data"
	"github.com/crazycs520/loadgen/util"
	"github.com/spf13/cobra"
	"math/rand"
	"strconv"
	"time"
)

type ManyTablePointGetSuite struct {
	cfg *config.Config

	rows      int
	tables    int
	doPrepare bool
	doExecute bool
}

func NewManyTablePointGetSuite(cfg *config.Config) cmd.CMDGenerater {
	return &ManyTablePointGetSuite{
		cfg:       cfg,
		rows:      100,
		doPrepare: false,
		doExecute: false,
	}
}

func (c *ManyTablePointGetSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "many-table-point-get",
		Short:        "create many tables, then do point get",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.rows, flagRows, "", 100, "the initial rows size before test")
	cmd.Flags().IntVarP(&c.tables, flagTables, "", 100, "the number of create tables")
	cmd.Flags().BoolVarP(&c.doPrepare, flagPrepare, "", false, "do prepare")
	cmd.Flags().BoolVarP(&c.doExecute, flagExecute, "", false, "do execute")
	return cmd
}

func (c *ManyTablePointGetSuite) ParseCmd(combinedCmd string) bool {
	return false
}

func (c *ManyTablePointGetSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *ManyTablePointGetSuite) prepare() error {
	for i := 0; i < c.tables; i++ {
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
		load := data.NewLoadDataSuite(c.cfg)
		err = load.Prepare(tblInfo, c.rows, c.rows/20000)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *ManyTablePointGetSuite) Run() error {
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

func (c *ManyTablePointGetSuite) execute() {
	for i := 0; i < c.cfg.Thread; i++ {
		go func() {
			db := util.GetSQLCli(c.cfg)
			defer func() {
				db.Close()
			}()
			for {
				for i := 0; i < c.tables; i++ {
					rows, err := db.Query(fmt.Sprintf("select * from t_%v where a = %v", i, rand.Intn(c.rows)))
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
			}
		}()
	}
	time.Sleep(time.Hour * 24 * 30)
}
