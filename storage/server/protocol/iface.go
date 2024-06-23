package protocol

import (
	"github.com/Trinoooo/eggie_kv/storage/server/connections"
)

type IProtocol interface {
	Encoder
	Decoder
	GetConnection() connections.IConnection
}

type Encoder interface {
	WriteStructBegin() error
	WriteStructEnd() error
	WriteI64(v int64) error
	WriteString(v string) error
	WriteBytes(v []byte) error
}

type Decoder interface {
	ReadStructBegin() error
	ReadStructEnd() error
	ReadI64() (int64, error)
	ReadString() (string, error)
	ReadBytes() ([]byte, error)
}
