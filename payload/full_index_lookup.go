package payload

import (
	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
)

type FullIndexLookUpSuite struct {
	*basicQuerySuite
}

func (c *FullIndexLookUpSuite) Name() string {
	return fullIndexLookupSuiteName
}

func (c *FullIndexLookUpSuite) GenQueryPrepareStmt() string {
	if c.isAgg {
		return "select sum(a+e), max(c) from " + c.tblInfo.DBTableName() + " use index (idx0)"
	}
	return "select * from " + c.tblInfo.DBTableName() + "use index (idx0)"
}

func (c *FullIndexLookUpSuite) GenQueryArgs() []interface{} {
	return nil
}

func NewFullIndexLookUpSuite(cfg *config.Config) cmd.CMDGenerater {
	suite := &FullIndexLookUpSuite{}
	basic := NewBasicQuerySuite(cfg, suite)
	suite.basicQuerySuite = basic
	return suite
}
