package server

import (
	"bufio"
	"errors"
	"net"
	"syscall"
	"time"
)

type connWrapper struct {
	conn net.Conn
}

func (w *connWrapper) Read(b []byte) (n int, err error) {
	return w.conn.Read(b)
}

func (w *connWrapper) Write(b []byte) (n int, err error) {
	return w.Write(b)
}

func (w *connWrapper) Close() error {
	defer func() {
		w.conn = nil
	}()
	return w.conn.Close()
}

func (w *connWrapper) LocalAddr() net.Addr {
	return w.conn.LocalAddr()
}

func (w *connWrapper) RemoteAddr() net.Addr {
	return w.conn.RemoteAddr()
}

func (w *connWrapper) SetDeadline(t time.Time) error {
	return w.conn.SetDeadline(t)
}

func (w *connWrapper) SetReadDeadline(t time.Time) error {
	return w.conn.SetReadDeadline(t)
}

func (w *connWrapper) SetWriteDeadline(t time.Time) error {
	return w.conn.SetWriteDeadline(t)
}

// isValid nil-safe
func (w *connWrapper) isValid() bool {
	return w != nil && w.conn != nil
}

func (w *connWrapper) checkConnectivity() bool {
	rawConn, ok := w.conn.(syscall.Conn)
	if !ok {
		return false
	}

	syscallConn, err := rawConn.SyscallConn()
	if err != nil {
		return false
	}

	var (
		n int
		e error
	)
	err = syscallConn.Read(func(fd uintptr) (done bool) {
		buf := make([]byte, 1)
		n, _, e = syscall.Recvfrom(int(fd), buf, syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
		return true
	})
	if err != nil {
		if errors.Is(err, net.ErrClosed) {
			w.conn.Close()
		}
		return false
	}

	return n == 0 && e == nil
}

func (w *connWrapper) IsOpen() bool {
	return !w.isValid() && w.checkConnectivity()
}

func newConnWrapper(conn net.Conn) *connWrapper {
	return &connWrapper{
		conn: conn,
	}
}

var _ IServerTransport = (*BaseServerTransport)(nil)

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

var _ ITransport = (*BaseTransport)(nil)
var _ ITransport = (*BufferedTransport)(nil)
var _ ITransport = (*FramedTransport)(nil)

type ITransport interface {
	Close() error
	Read(buf []byte) (int, error)
	Write(buf []byte) (int, error)
	Flush() error
	IsOpen() bool
}

type BaseTransport struct {
	conn *connWrapper
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

func (bt *BaseTransport) IsOpen() bool {
	return bt.conn.IsOpen()
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
		conn: newConnWrapper(conn),
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

func (bt *BufferedTransport) IsOpen() bool {
	return bt.trans.IsOpen()
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

func (ft *FramedTransport) IsOpen() bool {
	return ft.trans.IsOpen()
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

var _ ITransportFactory = (*BufferedTransportFactory)(nil)
var _ ITransportFactory = (*FramedTransportFactory)(nil)

type ITransportFactory interface {
	Build(trans ITransport) ITransport
}
