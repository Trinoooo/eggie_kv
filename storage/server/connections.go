package server

import "syscall"

type Conn struct {
	fd int
}

func (c *Conn) Read(buf []byte) (int, error) {
	return syscall.Read(c.fd, buf)
}

func (c *Conn) Write(buf []byte) (int, error) {
	return syscall.Write(c.fd, buf)
}

func (c *Conn) Close() error {
	return syscall.Close(c.fd)
}

type Listener struct {
	conn *Conn
}

func (l *Listener) Accept() (*Conn, error) {
	socket, _, err := syscall.Accept(l.conn.fd)
	if err != nil {
		return nil, err
	}

	if err = syscall.SetNonblock(socket, true); err != nil {
		return nil, err
	}

	return &Conn{
		fd: socket,
	}, nil
}

func (l *Listener) Close() error {
	return syscall.Close(l.conn.fd)
}

func Listen(addr [4]byte, port int) (*Listener, error) {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
	if err != nil {
		return nil, err
	}

	if err = syscall.Bind(fd, &syscall.SockaddrInet4{
		Port: port,
		Addr: addr,
	}); err != nil {
		return nil, err
	}

	if err = syscall.Listen(fd, syscall.SOMAXCONN); err != nil {
		return nil, err
	}

	return &Listener{
		conn: &Conn{
			fd: fd,
		},
	}, nil
}
