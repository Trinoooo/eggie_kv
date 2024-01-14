package utils

import (
	"github.com/Trinoooo/eggie_kv/consts"
	"os"
)

func IsTest() bool {
	return os.Getenv(consts.Env) == "test"
}
