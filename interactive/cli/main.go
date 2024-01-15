//go:build unix

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/chzyer/readline"
	"github.com/urfave/cli/v2"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

var opStrToOpType = map[string]consts.OperatorType{
	"get": consts.OperatorTypeGet,
	"GET": consts.OperatorTypeGet,
	"set": consts.OperatorTypeSet,
	"SET": consts.OperatorTypeSet,
}

func main() {
	wrapper := NewCliWrapper()
	if err := wrapper.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

var (
	flagHost = &cli.StringFlag{
		Name:    "host",
		Aliases: []string{"h"},
		Value:   "127.0.0.1",
		Usage:   "server host name.",
		EnvVars: []string{"EGGIE_KV_HOST"},
	}
	flagPort = &cli.Int64Flag{
		Name:    "port",
		Aliases: []string{"p"},
		Value:   8014,
		Usage:   "server port number, 0 < port < 65535 are available.",
		Action: func(c *cli.Context, port int64) error {
			if port <= 0 || port > 65535 {
				return errors.New("invalid params")
			}
			return nil
		},
		EnvVars: []string{"EGGIE_KV_PORT"},
	}
)

type CliWrapper struct {
	app *cli.App
}

func NewCliWrapper() *CliWrapper {
	wrapper := &CliWrapper{
		app: &cli.App{
			Name:    "eggie_kv_client",
			Usage:   "client for - a simple kv store based on memory",
			Version: "0.0.1.231216_alpha",
		},
	}
	wrapper.modifyDefaultHelp()
	wrapper.withFlags()
	wrapper.withAction()
	wrapper.withAuthor()
	return wrapper
}

func (wrapper *CliWrapper) Run(args []string) error {
	return wrapper.app.Run(args)
}

func (wrapper *CliWrapper) modifyDefaultHelp() {
	cli.HelpFlag = &cli.BoolFlag{
		Name: "help",
	}
}

func (wrapper *CliWrapper) withFlags() {
	wrapper.app.Flags = []cli.Flag{
		flagHost,
		flagPort,
	}
}

func (wrapper *CliWrapper) withAction() {
	wrapper.app.Action = func(ctx *cli.Context) error {
		url := fmt.Sprintf("http://%s:%d/", ctx.String("host"), ctx.Int64("port"))
		input, err := readline.NewEx(&readline.Config{
			Prompt: "> ",
			AutoComplete: readline.NewPrefixCompleter(
				readline.PcItem("get"),
				readline.PcItem("GET"),
				readline.PcItem("set"),
				readline.PcItem("SET"),
			),
			HistoryFile: fmt.Sprintf("/tmp/eggie_kv/cli/cmd_history_%s", time.Now().Format("20060102")),
		})
		if err != nil {
			log.Fatal(err)
		}
		input.CaptureExitSignal()
		for {
			str, err := input.Readline()
			if err != nil {
				if errors.Is(err, readline.ErrInterrupt) || errors.Is(err, io.EOF) {
					return errors.New("exit")
				}
				log.Println(err)
				continue
			}
			if strings.EqualFold(str, "exit") {
				return nil
			}
			handleInput(str, url)
		}
	}
}

func handleInput(input, url string) {
	var cmd, key string
	_, err := fmt.Sscanf(input, "%s %s", &cmd, &key)
	if err != nil {
		log.Println("error occur when parse form input, err: ", err)
		return
	}

	kvReq := &consts.KvRequest{
		OperationType: opStrToOpType[cmd],
		Key:           []byte(key),
	}

	reqBytes, err := json.Marshal(kvReq)
	if err != nil {
		log.Println("error occur when marshal req, err: ", err)
		return
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(reqBytes))
	if err != nil {
		log.Println("error occur when http post, err: ", err)
		return
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println("error occur when read resp body bytes, err: ", err)
		return
	}

	kvResp := &consts.KvResponse{}
	err = json.Unmarshal(bodyBytes, kvResp)
	if err != nil {
		log.Println("error occur when unmarshal resp, err: ", err)
		return
	}

	fmt.Printf("# %s\n", string(kvResp.Data))
}

func (wrapper *CliWrapper) withAuthor() {
	wrapper.app.Authors = []*cli.Author{
		{
			Name:  "Trino",
			Email: "sujun.trinoooo@gmail.com",
		},
	}
}
