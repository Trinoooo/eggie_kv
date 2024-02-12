package logs

import (
	"github.com/Trinoooo/eggie_kv/utils"
	"go.uber.org/zap"
)

var Logger *zap.Logger

func init() {
	var err error
	option := zap.AddCaller()
	if utils.IsTest() {
		Logger, err = zap.NewDevelopment(option)
	} else {
		Logger, err = zap.NewProduction(option)
	}

	if err != nil {
		panic(err)
	}
}
