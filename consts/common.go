package consts

import (
	"fmt"
	"os"
)

const (
	B = 1 << (iota * 10)
	KB
	MB
	GB
)

const HelpTemplate = `NAME:
   {{.Name}} - {{.Usage}}
USAGE:
   {{.HelpName}} {{if .VisibleFlags}}[global options]{{end}}{{if .Commands}} command [command options]{{end}} {{if .ArgsUsage}}{{.ArgsUsage}}{{else}}[arguments...]{{end}}
   {{if len .Authors}}
AUTHOR:
   {{range .Authors}}{{ . }}{{end}}
   {{end}}{{if .Commands}}
COMMANDS:
{{range .Commands}}{{if not .HideHelp}}   {{join .Names ", "}}{{ "\t"}}{{.Usage}}{{ "\n" }}{{end}}{{end}}{{end}}{{if .VisibleFlags}}
GLOBAL OPTIONS:
   {{range .VisibleFlags}}{{.}}
   {{end}}{{end}}{{if .Copyright }}
COPYRIGHT:
   {{.Copyright}}
   {{end}}{{if .Version}}
VERSION:
   {{.Version}}
   {{end}}
`

type OperatorType int64

const (
	OperatorTypeUnknown OperatorType = 0
	OperatorTypeGet     OperatorType = 1
	OperatorTypeSet     OperatorType = 2
)

var (
	BaseDir = fmt.Sprintf("%s/eggie_kv", os.Getenv("HOME"))
	TmpDir  = "/tmp/eggie_kv"
)
