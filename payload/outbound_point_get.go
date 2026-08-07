package payload

import (
	"math/rand"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
)

type OutboundPointGetSuite struct {
	*basicQuerySuite
}

func (c *OutboundPointGetSuite) Name() string {
	return outboundPointGetSuiteName
}

func (c *OutboundPointGetSuite) GenQueryPrepareStmt() string {
	return "select * from " + c.tblInfo.DBTableName() + " where a = ?;"
}

func (c *OutboundPointGetSuite) GenQueryArgs() []interface{} {
	n := c.rows + rand.Intn(c.rows)
	return []interface{}{n}
}

func NewOutboundPointGetSuite(cfg *config.Config) cmd.CMDGenerater {
	suite := &OutboundPointGetSuite{}
	basic := NewBasicQuerySuite(cfg, suite)
	suite.basicQuerySuite = basic
	return suite
}
