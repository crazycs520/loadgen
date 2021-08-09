package main

import (
	"github.com/crazycs520/loadgen/cmd"
	_ "github.com/crazycs520/loadgen/payload"
	"log"
)

func main() {
	app := cmd.NewApp()
	err := app.Cmd().Execute()
	if err != nil {
		log.Fatalln(err)
	}
}
