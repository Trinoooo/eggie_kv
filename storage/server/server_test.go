package server

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	addr = "localhost"
	host = "9999"

	fixSize     = 8
	concurrency = 1
)

func TestNewEServer(t *testing.T) {
	serverTransport, err := NewBaseServerTransport("")
	assert.Nil(t, err)
	processor := NewKvProcessor(new(EggieKvHandlerImpl)) // 似乎用new关键字搞个空指针更省空间
	_ = NewSimpleServer(
		processor,
		serverTransport,
		NewBufferedTransportFactory(1),
		NewFramedTransportFactory(),
		NewBinaryProtocolFactory(),
		NewBinaryProtocolFactory(),
	)
}

func TestEServer_Serve(t *testing.T) {

}
