package consts

import (
	"fmt"
	"github.com/mitchellh/go-homedir"
	log "github.com/sirupsen/logrus"
)

const (
	Core = "core"
)

func init() {
	home, err := homedir.Dir()
	if err != nil {
		log.Fatal(err)
	}
	BaseDir = fmt.Sprintf("%s/eggie_kv", home)
	DefaultConfigPath = fmt.Sprintf("%s/config/", BaseDir)
}

var (
	BaseDir           string
	DefaultConfigPath string
)
