package server

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestServerTransport(t *testing.T) {
	serverTransport, err := NewBaseServerTransport("127.0.0.1:9999")
	assert.Nil(t, err)

	err = serverTransport.Listen()
	assert.Nil(t, err)

	tp, err := serverTransport.Accept()
	assert.Nil(t, err)
}
