package payload

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
)

type RandBatchPointGetSuite struct {
	*basicQuerySuite
	batchSize int
}

func (c *RandBatchPointGetSuite) Name() string {
	return randBatchPointGetSuiteName
}

func (c *RandBatchPointGetSuite) GenQueryPrepareStmt() string {
	return "select * from " + c.tblInfo.DBTableName() + " where a in (?);"
}

func (c *RandBatchPointGetSuite) GenQueryArgs() []interface{} {
	vs := make([]string, 0, c.batchSize)
	for i := 0; i < c.batchSize; i++ {
		n := rand.Intn(c.rows)
		vs = append(vs, strconv.Itoa(n))
	}
	return []interface{}{strings.Join(vs, ",")}
}

func NewRandBatchPointGetSuite(cfg *config.Config) cmd.CMDGenerater {
	suite := &RandBatchPointGetSuite{
		batchSize: 100,
	}
	basic := NewBasicQuerySuite(cfg, suite)
	suite.basicQuerySuite = basic
	return suite
}

func (c *RandBatchPointGetSuite) Cmd() *cobra.Command {
	cmd := c.basicQuerySuite.Cmd()
	cmd.Flags().IntVarP(&c.batchSize, flagBatchSize, "", 100, "the batch size of batch point get")
	return cmd
}

func (c *RandBatchPointGetSuite) ParseCmd(combinedCmd string) bool {
	return ParsePayloadCmd(combinedCmd, c.querySuite.Name(), func(flag, value string) error {
		switch flag {
		case flagRows:
			v, err := strconv.Atoi(value)
			if err != nil {
				return err
			}
			c.rows = v
		case flagIsAgg:
			v, err := strconv.ParseBool(value)
			if err != nil {
				return err
			}
			c.isAgg = v
		case flagBatchSize:
			v, err := strconv.Atoi(value)
			if err != nil {
				return err
			}
			c.batchSize = v
		default:
			return fmt.Errorf("unknow flag %v", flag)
		}
		return nil
	})
}
