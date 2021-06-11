package cmd

import (
	"github.com/crazycs520/load/config"
	"github.com/spf13/cobra"
)

type CaseTest struct {
	*App
}

type CMDGenerater interface {
	Cmd() *cobra.Command
}

var caseCmds = []func(config *config.Config) CMDGenerater{}

func (b *CaseTest) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "case",
		Short:        "run case test",
		Long:         `specify the test case, and run the test case`,
		RunE:         b.RunE,
		SilenceUsage: true,
	}

	for _, gen := range caseCmds {
		child := gen(b.cfg)
		cmd.AddCommand(child.Cmd())
	}
	return cmd
}

func (b *CaseTest) RunE(cmd *cobra.Command, args []string) error {
	return cmd.Help()
}

func RegisterCaseCmd(c func(config *config.Config) CMDGenerater) {
	caseCmds = append(caseCmds, c)
}
