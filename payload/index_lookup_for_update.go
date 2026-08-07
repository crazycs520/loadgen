package payload

import (
	"context"
	"fmt"
	"math/rand"
	"sync"

	"github.com/spf13/cobra"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/util"
)

type IndexLookupForUpdateSuite struct {
	*basicQuerySuite

	rowRange int
}

func (c *IndexLookupForUpdateSuite) Name() string {
	return indexLookupForUpdateSuiteName
}

func (c *IndexLookupForUpdateSuite) GenQueryPrepareStmt() string {
	return "select * from " + c.tblInfo.DBTableName() + " where b >= ? and b <= ? for update;"
}

func (c *IndexLookupForUpdateSuite) GenQueryArgs() []interface{} {
	n := rand.Intn(c.rows)
	if n+c.rowRange > c.rows {
		n = c.rows - c.rowRange
	}
	return []interface{}{n, n + c.rowRange}
}

func NewIndexLookupForUpdateSuite(cfg *config.Config) cmd.CMDGenerater {
	suite := &IndexLookupForUpdateSuite{
		rowRange: 1,
	}
	basic := NewBasicQuerySuite(cfg, suite)
	suite.basicQuerySuite = basic
	return suite
}

func (c *IndexLookupForUpdateSuite) Cmd() *cobra.Command {
	cmd := c.basicQuerySuite.Cmd()
	cmd.RunE = c.RunE
	cmd.Flags().IntVarP(&c.rowRange, "row-range", "", 1, "the point get rowid range is [rand-rowid, rand-rowid+row-range)")
	return cmd
}

func (c *IndexLookupForUpdateSuite) ParseCmd(combinedCmd string) bool {
	return false
}

func (c *IndexLookupForUpdateSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *IndexLookupForUpdateSuite) Run() error {
	fmt.Printf("%v config: %v: %v, %v: %v\n", c.querySuite.Name(), flagRows, c.rows, "row-range", c.rowRange)
	ctx := context.Background()
	err := c.prepare()
	if err != nil {
		fmt.Println("prepare data meet error: ", err)
		return err
	}
	fmt.Printf("start to do select for update\n")
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Thread; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db := util.GetSQLCli(c.cfg)
			defer func() {
				db.Close()
			}()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				txn, err := db.Begin()
				if err != nil {
					fmt.Println(err)
					return
				}

				rows, err := txn.Query(c.GenQueryPrepareStmt(), c.GenQueryArgs()...)
				if err != nil {
					fmt.Println(err)
					return
				}
				for rows.Next() {
				}
				rows.Close()
				txn.Commit()
			}
		}()
	}
	wg.Wait()
	return nil
}
