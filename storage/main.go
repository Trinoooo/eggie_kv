package main

import (
	"github.com/Trinoooo/eggie_kv/storage/cli"
	"log"
	"os"
)

func main() {
	wrapper := cli.NewWrapper()
	if err := wrapper.Run(os.Args); err != nil {
		log.Fatalln(err)
	}
}
