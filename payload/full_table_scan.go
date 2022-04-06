package payload

import (
	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
)

type FullTableScanSuite struct {
	*basicQuerySuite
}

func (c *FullTableScanSuite) Name() string {
	return fullTableScanSuiteName
}

func (c *FullTableScanSuite) GenQueryPrepareStmt() string {
	if c.isAgg {
		return "select sum(a+b+e), sum(a*b), sum(a*e), sum(b*e), sum(a*b*e) from " + c.tblInfo.DBTableName() + ";"
	}
	return "select * from " + c.tblInfo.DBTableName() + ";"
}

func (c *FullTableScanSuite) GenQueryArgs() []interface{} {
	return nil
}

func NewFullTableScanSuite(cfg *config.Config) cmd.CMDGenerater {
	suite := &FullTableScanSuite{}
	basic := NewBasicQuerySuite(cfg, suite)
	suite.basicQuerySuite = basic
	return suite
}
