package consts

import (
	"fmt"
	"github.com/mitchellh/go-homedir"
)

const (
	Core = "core"
)

func init() {
	home, _ := homedir.Dir()
	BaseDir = fmt.Sprintf("%s/eggie_kv", home)
	DefaultConfigPath = fmt.Sprintf("%s/config", BaseDir)
}

var (
	BaseDir           string
	DefaultConfigPath string
)
