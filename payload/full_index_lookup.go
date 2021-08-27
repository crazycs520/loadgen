package payload

import (
	"sync/atomic"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
)

type FullIndexLookUpSuite struct {
	inc int64
	*basicQuerySuite
}

func (c *FullIndexLookUpSuite) Name() string {
	return fullIndexLookupSuiteName
}

func (c *FullIndexLookUpSuite) GenQueryPrepareStmt() string {
	if c.isAgg {
		if c.isBack {
			return "select sum(a+e), max(c) from " + c.tblInfo.DBTableName() + " use index (idx0) where e=?"
		}
		return "select sum(a+e), max(c) from " + c.tblInfo.DBTableName() + " use index (idx0)"
	}
	if c.isBack {
		return "select * from " + c.tblInfo.DBTableName() + " use index (idx0) where e=?"
	}
	return "select * from " + c.tblInfo.DBTableName() + " use index (idx0)"
}

func (c *FullIndexLookUpSuite) GenQueryArgs() []interface{} {
	if c.isBack {
		return []interface{}{atomic.AddInt64(&c.inc, 1)}
	}
	return nil
}

func NewFullIndexLookUpSuite(cfg *config.Config) cmd.CMDGenerater {
	suite := &FullIndexLookUpSuite{inc: -1}
	basic := NewBasicQuerySuite(cfg, suite)
	suite.basicQuerySuite = basic
	return suite
}
