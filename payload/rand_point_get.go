package payload

import (
	"fmt"
	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"math/rand"
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

func NewRandPointGetSuite(cfg *config.Config) cmd.CMDGenerater {
	suite := &RandPointGetSuite{}
	basic := NewBasicQuerySuite(cfg, suite)
	suite.basicQuerySuite = basic
	return suite
}
