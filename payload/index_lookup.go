package payload

import (
	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/spf13/cobra"
	"math/rand"
)

type IndexLookUpSuite struct {
	*basicQuerySuite
	bound int
}

func (c *IndexLookUpSuite) Name() string {
	return indexLookUpSuiteName
}

func (c *IndexLookUpSuite) Cmd() *cobra.Command {
	cmd := c.basicQuerySuite.Cmd()
	cmd.Flags().IntVarP(&c.bound, flagBound, "", 1000, "index lookup scan range bound")
	return cmd
}

func (c *IndexLookUpSuite) GenQueryPrepareStmt() string {
	if c.isAgg {
		return "select count(e) from " + c.tblInfo.DBTableName() + " use index (idx0) where b >= ? and b < ?"
	}
	return "select * from " + c.tblInfo.DBTableName() + " use index (idx0) where b >= ? and b < ?"
}

func (c *IndexLookUpSuite) GenQueryArgs() []interface{} {
	var start int
	if c.rows > c.bound {
		start = rand.Intn(c.rows - c.bound)
	} else {
		start = 0
	}
	end := start + c.bound
	return []interface{}{start, end}
}

func NewIndexLookUpSuite(cfg *config.Config) cmd.CMDGenerater {
	suite := &IndexLookUpSuite{}
	basic := NewBasicQuerySuite(cfg, suite)
	suite.basicQuerySuite = basic
	return suite
}
