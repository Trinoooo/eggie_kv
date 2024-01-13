package utils

import (
	log "github.com/sirupsen/logrus"
	"testing"
)

func TestHandlePanic(t *testing.T) {
	defer HandlePanic(func() {
		log.Info("finish")
	})

	panic("haha")
}
