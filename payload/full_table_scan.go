package payload

import (
	"fmt"
	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
)

type FullTableScanSuite struct {
	*basicQuerySuite
}

func (c *FullTableScanSuite) Name() string {
	return fullTableScanSuiteName
}

func (c *FullTableScanSuite) GenQuerySQL() string {
	if c.agg {
		return fmt.Sprintf("select sum(a+b+e), sum(a*b), sum(a*e), sum(b*e), sum(a*b*e) from %v;", c.tblInfo.DBTableName())
	}
	return fmt.Sprintf("select * from %v;", c.tblInfo.DBTableName())

}

func NewFullTableScanSuite(cfg *config.Config) cmd.CMDGenerater {
	suite := &FullTableScanSuite{}
	basic := NewBasicQuerySuite(cfg, suite)
	suite.basicQuerySuite = basic
	return suite
}
