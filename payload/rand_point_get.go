package payload

import (
	"fmt"
	"math/rand"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
)

type RandPointGetSuite struct {
	*basicQuerySuite
}

func (c *RandPointGetSuite) Name() string {
	return randPointGetSuiteName
}

func (c *RandPointGetSuite) GenQuerySQL() string {
	n := rand.Intn(c.rows)
	return fmt.Sprintf("select * from %v where a = %v", c.tblInfo.DBTableName(), n)
}

func (c *RandPointGetSuite) GenQueryPrepareStmt() string {
	return "select * from " + c.tblInfo.DBTableName() + " where a = ?;"
}

func (c *RandPointGetSuite) GenQueryArgs() []interface{} {
	return []interface{}{rand.Intn(c.rows)}
}

func NewRandPointGetSuite(cfg *config.Config) cmd.CMDGenerater {
	suite := &RandPointGetSuite{}
	basic := NewBasicQuerySuite(cfg, suite)
	suite.basicQuerySuite = basic
	return suite
}
