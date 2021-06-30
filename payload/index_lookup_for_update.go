package payload

import (
	"context"
	"fmt"
	"github.com/crazycs520/load/util"
	"github.com/spf13/cobra"
	"math/rand"
	"sync"

	"github.com/crazycs520/load/cmd"
	"github.com/crazycs520/load/config"
)

type IndexLookupForUpdateSuite struct {
	*basicQuerySuite

	rowRange int
}

func (c *IndexLookupForUpdateSuite) Name() string {
	return indexLookupForUpdateSuiteName
}

func (c *IndexLookupForUpdateSuite) GenQuerySQL() string {
	n := rand.Intn(c.rows)
	if n+c.rowRange > c.rows {
		n = c.rows - c.rowRange
	}
	return fmt.Sprintf("select * from %v where b >= %v and b <= %v for update", c.tblInfo.DBTableName(), n, n+c.rowRange)
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

				n := rand.Intn(c.rows)
				if n+c.rowRange > c.rows {
					n = c.rows - c.rowRange
				}
				query := fmt.Sprintf("select * from %v use index(idx0) where b >= %v and b <= %v for update", c.tblInfo.DBTableName(), n, n+c.rowRange)
				rows, err := txn.Query(query)
				if err != nil {
					fmt.Println(err)
					return
				}
				for rows.Next() {
				}
				rows.Close()
				//update := fmt.Sprintf("update %v set e=e+1 where b >= %v and b <= %v", c.tblInfo.DBTableName(), n, n+c.rowRange)
				//_, err = txn.Exec(update)
				//if err != nil {
				//	fmt.Println(err)
				//	return
				//}
				txn.Commit()
			}
		}()
	}
	wg.Wait()
	return nil
}
