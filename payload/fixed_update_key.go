package payload

import (
	"sync/atomic"
	"time"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/spf13/cobra"
)

type FixedUpdateKeySuite struct {
	*basicQuerySuite

	inc   int64
	rowID int
}

func (c *FixedUpdateKeySuite) Name() string {
	return "fix-update-key"
}

func (c *FixedUpdateKeySuite) Cmd() *cobra.Command {
	cmd := c.basicQuerySuite.Cmd()
	c.basicQuerySuite.setInsertRows(10)
	cmd.Flags().IntVarP(&c.rowID, flagRowID, "", 1, "the fixed row id to update")
	return cmd
}

func NewFixedUpdateKeySuite(cfg *config.Config) cmd.CMDGenerater {
	suite := &FixedUpdateKeySuite{
		inc:   -1,
		rowID: 1,
	}
	basic := NewBasicQuerySuite(cfg, suite)
	suite.basicQuerySuite = basic
	return suite
}

func (c *FixedUpdateKeySuite) GenQueryPrepareStmt() string {
	return "update " + c.tblInfo.DBTableName() + " set b=?, c=?, e=? where a = ?"
}

func (c *FixedUpdateKeySuite) GenQueryArgs() []interface{} {
	inc := atomic.AddInt64(&c.inc, 1)
	return []interface{}{inc, time.Now(), inc, c.rowID}
}
