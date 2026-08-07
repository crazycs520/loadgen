package payload

import (
	"fmt"
	"math/rand"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
)

type RandRangeTableScanSuite struct {
	*basicQuerySuite
}

func (c *RandRangeTableScanSuite) Name() string {
	return randRangeTableScanSuiteName
}

func (c *RandRangeTableScanSuite) GenQueryPrepareStmt() string {
	if c.isAgg {
		return fmt.Sprintf("select sum(a+b+e), sum(a*b), sum(a*e), sum(b*e), sum(a*b*e) from %v where a >= ? and a <= ?", c.tblInfo.DBTableName())
	}
	return fmt.Sprintf("select * from %v where a >= ? and a <= ?", c.tblInfo.DBTableName())
}

func (c *RandRangeTableScanSuite) GenQueryArgs() []interface{} {
	n1 := rand.Intn(c.rows) + 1
	n2 := rand.Intn(c.rows) + 1
	if n1 > n2 {
		n1, n2 = n2, n1
	}
	return []interface{}{n1, n2}
}

func NewRandRangeTableScanSuite(cfg *config.Config) cmd.CMDGenerater {
	suite := &RandRangeTableScanSuite{}
	basic := NewBasicQuerySuite(cfg, suite)
	suite.basicQuerySuite = basic
	return suite
}
