package server

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewEServer(t *testing.T) {
	serverTransport, err := NewBaseServerTransport("")
	assert.Nil(t, err)
	processor := NewKvProcessor()
	processor.Register("Get", HandleGet)
	processor.Register("Set", HandleSet)
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
