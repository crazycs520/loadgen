package cmd

import (
	"fmt"
	"sync"

	"github.com/crazycs520/loadgen/config"
	"github.com/spf13/cobra"
)

type PayloadCMD struct {
	*App
}

type CMDGenerater interface {
	Cmd() *cobra.Command
	Run() error
}

type CMDParser interface {
	ParseCmd(combinedCmd string) bool
}

var payloadCmdGenerators = []func(config *config.Config) CMDGenerater{}

func (b *PayloadCMD) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "payload",
		Short:        "run the specified payload",
		RunE:         b.RunE,
		SilenceUsage: true,
	}

	for _, gen := range payloadCmdGenerators {
		child := gen(b.cfg)
		cmd.AddCommand(child.Cmd())
	}
	return cmd
}

func (b *PayloadCMD) RunE(cmd *cobra.Command, args []string) error {
	return cmd.Help()
}

func RegisterCaseCmd(c func(config *config.Config) CMDGenerater) {
	payloadCmdGenerators = append(payloadCmdGenerators, c)
}

func RunCombinedPayloads(config *config.Config, payloads []string) error {
	var wg sync.WaitGroup
	for _, p := range payloads {
		valid := false
		for _, gen := range payloadCmdGenerators {
			c := gen(config)
			command, ok := c.(CMDParser)
			if !ok {
				continue
			}
			valid = command.ParseCmd(p)
			if !valid {
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				c.Run()
			}()
			break
		}
		if !valid {
			fmt.Printf("unknow payload cmd: %v, please check the payload subcommand\n", p)
		}
	}
	wg.Wait()
	return nil
}
