//go:build unix

package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/Trinoooo/eggie_kv/cli/handle"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/chzyer/readline"
	"github.com/urfave/cli/v2"
)

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
		cancelCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		serverUrl, err := url.Parse(fmt.Sprintf("http://%s:%d/", ctx.String("host"), ctx.Int64("port")))
		if err != nil {
			log.Println("error occur when parse server url, err: ", err)
			return nil
		}

		client := &handle.ClientWrapper{
			Url:  serverUrl,
			Http: &http.Client{},
			Ctx:  &cancelCtx,
		}

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
		defer input.Close()

		cSignal := make(chan os.Signal, 1)
		signal.Notify(cSignal, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-cSignal
			cancel()
		}()

		for {
			select {
			case <-cancelCtx.Done():
				return nil
			default:
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
				client.HandleInput(str)
			}
		}
	}
}

func (wrapper *CliWrapper) withAuthor() {
	wrapper.app.Authors = []*cli.Author{
		{
			Name:  "Trino",
			Email: "sujun.trinoooo@gmail.com",
		},
	}
}
