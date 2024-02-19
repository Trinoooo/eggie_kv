package consts

import (
	"fmt"
	"github.com/Trinoooo/eggie_kv/storage/logs"
	"github.com/mitchellh/go-homedir"
)

const (
	Core = "core"
)

func init() {
	home, err := homedir.Dir()
	if err != nil {
		logs.Fatal(err.Error())
	}
	BaseDir = fmt.Sprintf("%s/eggie_kv", home)
	DefaultConfigPath = fmt.Sprintf("%s/config/", BaseDir)
}

var (
	BaseDir           string
	DefaultConfigPath string
)

const (
	Ragdoll = "ragdoll"
)
