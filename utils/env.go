package utils

import (
	"github.com/Trinoooo/eggie_kv/consts"
	"os"
)

func IsTest() bool {
	return os.Getenv(consts.Env) == "test"
}

func GetValueOnEnv(prod, test interface{}) interface{} {
	if IsTest() {
		return test
	}
	return prod
}
