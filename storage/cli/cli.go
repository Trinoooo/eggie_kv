package cli

import (
	"fmt"
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/Trinoooo/eggie_kv/errs"
	"github.com/Trinoooo/eggie_kv/storage/core/ragdoll/logs"
	"github.com/Trinoooo/eggie_kv/storage/server"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

var (
	flagHost = &cli.StringFlag{
		Name:    "host",
		Aliases: []string{"h"},
		Value:   "127.0.0.1",
		Usage:   "server host name.",
		EnvVars: []string{consts.Host},
	}
	flagPort = &cli.Int64Flag{
		Name:    "port",
		Aliases: []string{"p"},
		Value:   8014,
		Usage:   "server port number, 0 < port < 65535 are available.",
		Action: func(c *cli.Context, port int64) error {
			if port <= 0 || port > 65535 {
				e := errs.NewInvalidParamErr()
				logs.Error(e.Error(), zap.String(consts.Params, "port"), zap.Int64(consts.Value, port))
				return e
			}
			return nil
		},
		EnvVars: []string{consts.Port},
	}
	flagSegmentSize = &cli.Int64Flag{
		Name:    "max-segment-size",
		Aliases: []string{"s"},
		Value:   consts.KB * 4,
		Usage:   "max size per segment file, 0 < size <= 1GB are available.",
		Action: func(context *cli.Context, size int64) error {
			if size < 0 || size > consts.GB {
				e := errs.NewInvalidParamErr()
				logs.Error(e.Error(), zap.String(consts.Params, "size"), zap.Int64(consts.Value, size))
				return e
			}
			return nil
		},
		EnvVars: []string{"EGGIE_KV_MAX_SEGMENT_SIZE"},
	}
	flagConnection = &cli.Int64Flag{
		Name:    "max-connect-number",
		Aliases: []string{"c"},
		Value:   200,
		Usage:   "max connection number, 0 < number <= 4000 are available.",
		Action: func(context *cli.Context, number int64) error {
			if number < 0 || number > 4000 {
				e := errs.NewInvalidParamErr()
				logs.Error(e.Error(), zap.String(consts.Params, "number"), zap.Int64(consts.Value, number))
				return e
			}
			return nil
		},
		EnvVars: []string{"EGGIE_KV_MAX_CONNECT_NUMBER"},
	}
	flagDurable = &cli.BoolFlag{
		Name:    "durable",
		Aliases: []string{"d"},
		Value:   false,
		Usage:   "set this flag to make data durable.",
		EnvVars: []string{consts.Durable},
	}
)

type Wrapper struct {
	app *cli.App
}

func NewWrapper() *Wrapper {
	wrapper := &Wrapper{
		app: &cli.App{
			Name:    "eggie_kv",
			Usage:   "a simple kv store based on memory",
			Version: "0.0.1.231216_alpha",
		},
	}
	wrapper.modifyDefaultHelp()
	wrapper.withFlags()
	wrapper.withAction()
	wrapper.withAuthor()
	return wrapper
}

func (wrapper *Wrapper) Run(args []string) error {
	return wrapper.app.Run(args)
}

func (wrapper *Wrapper) modifyDefaultHelp() {
	cli.HelpFlag = &cli.BoolFlag{
		Name: "help",
	}
	cli.AppHelpTemplate = consts.HelpTemplate
}

func (wrapper *Wrapper) withFlags() {
	wrapper.app.Flags = []cli.Flag{
		flagHost,
		flagPort,
		flagSegmentSize,
		flagConnection,
		flagDurable,
	}
}

func (wrapper *Wrapper) withAction() {
	wrapper.app.Action = func(ctx *cli.Context) error {
		srv, err := server.NewServer()
		if err != nil {
			return err
		}

		http.HandleFunc("/", srv.Server)
		go func() {
			addr := fmt.Sprintf("%s:%d", ctx.String("host"), ctx.Int64("port"))
			if err := http.ListenAndServe(addr, nil); err != nil {
				// 父协程没recover也会一起panic，导致程序崩溃
				log.Fatal(err)
			}
		}()
		sig := make(chan os.Signal)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)
		for range sig {
			log.Info("shutdown...")
		}
		return nil
	}
}

func (wrapper *Wrapper) withAuthor() {
	wrapper.app.Authors = []*cli.Author{
		{
			Name:  "Trino",
			Email: "sujun.trinoooo@gmail.com",
		},
	}
}
