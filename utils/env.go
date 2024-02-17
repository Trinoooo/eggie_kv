package utils

import (
	"os"
)

func Env() string {
	return os.Getenv("EGGIE_KV_ENV")
}

func IsTest() bool {
	return Env() == "test"
}

func GetValueOnEnv(prod, test interface{}) interface{} {
	if IsTest() {
		return test
	}
	return prod
}
