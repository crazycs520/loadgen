package main

import (
	"github.com/crazycs520/load/cmd"
	_ "github.com/crazycs520/load/testcase"
	"log"
)

func main() {
	app := cmd.NewApp()
	err := app.Cmd().Execute()
	if err != nil {
		log.Fatalln(err)
	}
}
