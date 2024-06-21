package server

import (
	"net"
	"syscall"
)

type Conn struct {
	fd    int
	laddr *syscall.SockaddrInet4
	raddr *syscall.SockaddrInet4
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

func (c *Conn) RemoteAddr() net.Addr {
	ipv4 := net.IPv4(c.raddr.Addr[0], c.raddr.Addr[1], c.raddr.Addr[2], c.raddr.Addr[3])
	return &net.TCPAddr{
		IP:   ipv4,
		Port: c.raddr.Port,
	}
}

func (c *Conn) LocalAddr() net.Addr {
	ipv4 := net.IPv4(c.laddr.Addr[0], c.laddr.Addr[1], c.laddr.Addr[2], c.laddr.Addr[3])
	return &net.TCPAddr{
		IP:   ipv4,
		Port: c.laddr.Port,
	}
}

type Listener struct {
	conn *Conn
}

func (l *Listener) Accept() (*Conn, error) {
	socket, sa, err := syscall.Accept(l.conn.fd)
	if err != nil {
		return nil, err
	}

	if err = syscall.SetNonblock(socket, true); err != nil {
		return nil, err
	}

	return &Conn{
		fd:    socket,
		laddr: l.conn.laddr,
		raddr: sa.(*syscall.SockaddrInet4),
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

	laddr := &syscall.SockaddrInet4{
		Port: port,
		Addr: addr,
	}
	if err = syscall.Bind(fd, laddr); err != nil {
		return nil, err
	}

	if err = syscall.Listen(fd, syscall.SOMAXCONN); err != nil {
		return nil, err
	}

	return &Listener{
		conn: &Conn{
			fd:    fd,
			laddr: laddr,
		},
	}, nil
}
