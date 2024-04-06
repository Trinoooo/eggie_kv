package server

import (
	"bufio"
	"net"
)

var _ IServerTransport = &BaseServerTransport{}

type IServerTransport interface {
	Listen() error
	Accept() (ITransport, error)
	Close() error
}

type BaseServerTransport struct {
	Addr     net.Addr
	listener net.Listener
}

func NewBaseServerTransport(addr string) (*BaseServerTransport, error) {
	address, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &BaseServerTransport{
		Addr: address,
	}, nil
}

func (bst *BaseServerTransport) Listen() error {
	if bst.listener != nil {
		return nil
	}

	listener, err := net.Listen(bst.Addr.Network(), bst.Addr.String())
	if err != nil {
		return err
	}

	bst.listener = listener
	return nil
}

func (bst *BaseServerTransport) Accept() (ITransport, error) {
	conn, err := bst.listener.Accept()
	if err != nil {
		return nil, err
	}

	factory := NewBaseTransportFactory()
	tp := factory.Build(conn)
	return tp, nil
}

func (bst *BaseServerTransport) Close() error {
	return bst.listener.Close()
}

var _ ITransport = &BaseTransport{}
var _ ITransport = &BufferedTransport{}
var _ ITransport = &FramedTransport{}

type ITransport interface {
	Close() error
	Read(buf []byte) (int, error)
	Write(buf []byte) (int, error)
	Flush() error
}

type BaseTransport struct {
	conn net.Conn
	cfg  *TConfig
}

func (bt *BaseTransport) Close() error {
	return bt.conn.Close()
}

func (bt *BaseTransport) Read(buf []byte) (int, error) {
	return bt.Read(buf)
}

func (bt *BaseTransport) Write(buf []byte) (int, error) {
	return bt.conn.Write(buf)
}

func (bt *BaseTransport) Flush() error {
	return nil
}

type BaseTransportFactory struct {
	cfg *TConfig
}

func NewBaseTransportFactory() *BaseTransportFactory {
	return &BaseTransportFactory{
		cfg: &TConfig{
			Propagation: true,
		},
	}
}

func NewBaseTransportFactoryConf(cfg *TConfig) *BaseTransportFactory {
	return &BaseTransportFactory{
		cfg: cfg,
	}
}

func (b *BaseTransportFactory) Build(conn net.Conn) ITransport {
	return &BaseTransport{
		conn: conn,
		cfg:  b.cfg,
	}
}

type BufferedTransport struct {
	trans      ITransport
	readWriter *bufio.ReadWriter
}

func (bt *BufferedTransport) Close() error {
	err := bt.readWriter.Flush()
	if err != nil {
		return err
	}

	err = bt.trans.Close()
	if err != nil {
		return err
	}

	return nil
}

func (bt *BufferedTransport) Read(buf []byte) (int, error) {
	return bt.readWriter.Read(buf)
}

func (bt *BufferedTransport) Write(buf []byte) (int, error) {
	return bt.readWriter.Write(buf)
}

func (bt *BufferedTransport) Flush() error {
	return bt.readWriter.Flush()
}

type BufferedTransportFactory struct {
	bufSize int
}

func NewBufferedTransportFactory(bufSize int) *BufferedTransportFactory {
	return &BufferedTransportFactory{
		bufSize: bufSize,
	}
}

func (b *BufferedTransportFactory) Build(trans ITransport) ITransport {
	return &BufferedTransport{
		trans: trans,
		readWriter: bufio.NewReadWriter(
			bufio.NewReaderSize(trans, b.bufSize),
			bufio.NewWriterSize(trans, b.bufSize),
		),
	}
}

type FramedTransport struct {
	trans ITransport
}

func (ft *FramedTransport) Close() error {
	return nil
}

func (ft *FramedTransport) Read(buf []byte) (int, error) {
	return 0, nil
}

func (ft *FramedTransport) Write(buf []byte) (int, error) {
	return 0, nil
}

func (ft *FramedTransport) Flush() error {
	return nil
}

type FramedTransportFactory struct {
}

func NewFramedTransportFactory() *FramedTransportFactory {
	return &FramedTransportFactory{}
}

func (f *FramedTransportFactory) Build(trans ITransport) ITransport {
	return &FramedTransport{
		trans: trans,
	}
}

var _ ITransportFactory = &BufferedTransportFactory{}
var _ ITransportFactory = &FramedTransportFactory{}

type ITransportFactory interface {
	Build(trans ITransport) ITransport
}
