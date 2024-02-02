package iface

import (
	"github.com/spf13/viper"
)

type ICore interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
}

type Builder func(config *viper.Viper) (ICore, error)
