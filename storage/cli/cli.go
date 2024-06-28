package cli

import (
	"fmt"
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/Trinoooo/eggie_kv/errs"
	"github.com/Trinoooo/eggie_kv/storage/core/ragdoll/logs"
	"github.com/Trinoooo/eggie_kv/storage/server"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
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
				logs.Error(e.Error(), zap.String(consts.LogFieldParams, "port"), zap.Int64(consts.LogFieldValue, port))
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
				logs.Error(e.Error(), zap.String(consts.LogFieldParams, "size"), zap.Int64(consts.LogFieldValue, size))
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
				logs.Error(e.Error(), zap.String(consts.LogFieldParams, "number"), zap.Int64(consts.LogFieldValue, number))
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
		srv, err := server.NewReactorServer([4]byte{127, 0, 0, 1}, 9999)
		if err != nil {
			return err
		}

		go func() {
			// bugfix: 使用缓冲通道避免执行信号处理程序（下面的for）之前有信号到达会被丢弃
			sig := make(chan os.Signal, 5)
			signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
			for range sig {
				logs.Info("shutdown...")
				logs.Error(fmt.Sprintf("server shutdown, err: %v", srv.Close()))
			}
		}()

		return srv.Serve()
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
