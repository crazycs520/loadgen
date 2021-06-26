package cmd

import (
	"database/sql"
	"fmt"
	"github.com/crazycs520/load/config"
	"github.com/crazycs520/load/util"
	"github.com/spf13/cobra"
)

type App struct {
	cfg      *config.Config
	payloads []string
}

func NewApp() *App {
	return &App{
		cfg: &config.Config{},
	}
}

func (app *App) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "loadgen",
		Short:        "loadgen uses to generate some database load",
		RunE:         app.RunE,
		SilenceUsage: true,
	}

	cmd.PersistentFlags().StringVarP(&app.cfg.Host, "host", "", "127.0.0.1", "database host ip")
	cmd.PersistentFlags().IntVarP(&app.cfg.Port, "port", "P", 4000, "database service port")
	cmd.PersistentFlags().StringVarP(&app.cfg.User, "user", "u", "root", "database user name")
	cmd.PersistentFlags().StringVarP(&app.cfg.Password, "password", "p", "", "database user password")
	cmd.PersistentFlags().StringVarP(&app.cfg.DBName, "db", "d", "test", "database name")
	cmd.PersistentFlags().IntVarP(&app.cfg.Concurrency, "concurrency", "f", 5, "app concurrency")
	cmd.PersistentFlags().StringArrayVarP(&app.payloads, "payload", "", nil, "specified the payload")

	bench := BenchSQL{App: app}
	cmd.AddCommand(bench.Cmd())

	payload := PayloadCMD{App: app}
	cmd.AddCommand(payload.Cmd())
	return cmd
}

func (app *App) RunE(cmd *cobra.Command, args []string) error {
	fmt.Printf("\n--------------------------------------\n")
	fmt.Printf("current config:\n%v\n", app.cfg.String())
	if len(app.payloads) > 0 {
		fmt.Println("you specified payload is ", app.payloads)
		return RunCombinedPayloads(app.cfg, app.payloads)
	}
	err := cmd.Help()
	if err != nil {
		return err
	}
	return nil
}

func (app *App) GetSQLCli() *sql.DB {
	return util.GetSQLCli(app.cfg)
}
