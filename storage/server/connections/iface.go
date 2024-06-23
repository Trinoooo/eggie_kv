package connections

import (
	"io"
	"net"
)

type IListener interface {
	Accept() (IConnection, error)
	io.Closer
}

type IConnection interface {
	io.ReadWriteCloser
	RemoteAddr() net.Addr
	LocalAddr() net.Addr
	RawFd() int
}
