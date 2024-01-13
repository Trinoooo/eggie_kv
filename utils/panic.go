package utils

import log "github.com/sirupsen/logrus"

func HandlePanic(fn func()) {
	if r := recover(); r != nil {
		log.Fatal(r)
	}

	fn()
}
