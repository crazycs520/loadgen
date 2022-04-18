package payload

import (
	"math/rand"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/spf13/cobra"
)

type RandPointGetSuite struct {
	*basicQuerySuite

	selectRows int
}

func (c *RandPointGetSuite) Name() string {
	return randPointGetSuiteName
}

func (c *RandPointGetSuite) GenQueryPrepareStmt() string {
	return "select * from " + c.tblInfo.DBTableName() + " where a = ?;"
}

func (c *RandPointGetSuite) Cmd() *cobra.Command {
	cmd := c.basicQuerySuite.Cmd()
	cmd.Flags().IntVarP(&c.selectRows, "select-rows", "", -1, "the range of rows to be selected, -1 for all rows")
	return cmd
}

func (c *RandPointGetSuite) GenQueryArgs() []interface{} {
	randRows := c.selectRows
	if randRows < 0 {
		randRows = c.rows
	}
	return []interface{}{rand.Intn(randRows)}
}

func NewRandPointGetSuite(cfg *config.Config) cmd.CMDGenerater {
	suite := &RandPointGetSuite{}
	basic := NewBasicQuerySuite(cfg, suite)
	suite.basicQuerySuite = basic
	return suite
}
