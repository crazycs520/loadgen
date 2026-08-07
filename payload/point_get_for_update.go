package payload

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"

	"github.com/spf13/cobra"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/util"
)

type PointGetForUpdateGetSuite struct {
	*basicQuerySuite

	randRowID int
}

func (c *PointGetForUpdateGetSuite) Name() string {
	return pointGetForUpdateSuiteName
}

func (c *PointGetForUpdateGetSuite) GenQueryPrepareStmt() string {
	return "select * from " + c.tblInfo.DBTableName() + " where a = ? for update;"
}

func (c *PointGetForUpdateGetSuite) GenQueryArgs() []interface{} {
	return []interface{}{rand.Intn(c.randRowID)}
}

func NewPointGetForUpdateGetSuite(cfg *config.Config) cmd.CMDGenerater {
	suite := &PointGetForUpdateGetSuite{
		randRowID: 1,
	}
	basic := NewBasicQuerySuite(cfg, suite)
	suite.basicQuerySuite = basic
	return suite
}

func (c *PointGetForUpdateGetSuite) Cmd() *cobra.Command {
	cmd := c.basicQuerySuite.Cmd()
	cmd.RunE = c.RunE
	cmd.Flags().IntVarP(&c.randRowID, flagRandRowID, "", 1, "the point get rowid range is [0,rand-rowid)")
	c.basicQuerySuite.setInsertRows(c.randRowID + 10)
	return cmd
}

func (c *PointGetForUpdateGetSuite) ParseCmd(combinedCmd string) bool {
	return ParsePayloadCmd(combinedCmd, c.querySuite.Name(), func(flag, value string) error {
		switch flag {
		case flagRows:
			v, err := strconv.Atoi(value)
			if err != nil {
				return err
			}
			c.rows = v
		case flagIsAgg:
			v, err := strconv.ParseBool(value)
			if err != nil {
				return err
			}
			c.isAgg = v
		case flagRandRowID:
			v, err := strconv.Atoi(value)
			if err != nil {
				return err
			}
			c.randRowID = v
		default:
			return fmt.Errorf("unknow flag %v", flag)
		}
		return nil
	})
}

func (c *PointGetForUpdateGetSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *PointGetForUpdateGetSuite) Run() error {
	fmt.Printf("%v config: %v: %v, %v: %v\n", c.querySuite.Name(), flagRows, c.rows, flagRandRowID, c.randRowID)
	ctx := context.Background()
	err := c.prepare()
	if err != nil {
		fmt.Println("prepare data meet error: ", err)
		return err
	}
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
				_, err = txn.Exec(c.GenQueryPrepareStmt(), c.GenQueryArgs()...)
				if err != nil {
					fmt.Println(err)
					return
				}
				txn.Rollback()
			}
		}()
	}
	wg.Wait()
	return nil
}
