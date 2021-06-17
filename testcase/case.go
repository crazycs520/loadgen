package testcase

import (
	"github.com/crazycs520/load/cmd"
)

func init() {
	cmd.RegisterCaseCmd(NewIndexLookUpWrongPlan)
	cmd.RegisterCaseCmd(NewWriteHotSuite)
	cmd.RegisterCaseCmd(NewNormalOLTPSuite)
}
