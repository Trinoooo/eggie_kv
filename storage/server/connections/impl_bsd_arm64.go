package connections

import (
	"net"
	"syscall"
)

const maxSoMaxConn = 500

type Connection struct {
	fd         int
	localAddr  *syscall.SockaddrInet4
	remoteAddr *syscall.SockaddrInet4
}

func (c *Connection) Read(buf []byte) (int, error) {
	return syscall.Read(c.fd, buf)
}

func (c *Connection) Write(buf []byte) (int, error) {
	return syscall.Write(c.fd, buf)
}

func (c *Connection) Close() error {
	return syscall.Close(c.fd)
}

func (c *Connection) RemoteAddr() net.Addr {
	ipv4 := net.IPv4(c.remoteAddr.Addr[0], c.remoteAddr.Addr[1], c.remoteAddr.Addr[2], c.remoteAddr.Addr[3])
	return &net.TCPAddr{
		IP:   ipv4,
		Port: c.remoteAddr.Port,
	}
}

func (c *Connection) LocalAddr() net.Addr {
	ipv4 := net.IPv4(c.localAddr.Addr[0], c.localAddr.Addr[1], c.localAddr.Addr[2], c.localAddr.Addr[3])
	return &net.TCPAddr{
		IP:   ipv4,
		Port: c.localAddr.Port,
	}
}

func (c *Connection) RawFd() int {
	return c.fd
}

type Listener struct {
	conn *Connection
}

func (l *Listener) Accept() (IConnection, error) {
	socket, sa, err := syscall.Accept(l.conn.fd)
	if err != nil {
		return nil, err
	}

	if err = syscall.SetNonblock(socket, true); err != nil {
		return nil, err
	}

	return &Connection{
		fd:         socket,
		localAddr:  l.conn.localAddr,
		remoteAddr: sa.(*syscall.SockaddrInet4),
	}, nil
}

func (l *Listener) Close() error {
	return syscall.Close(l.conn.fd)
}

func Listen(addr [4]byte, port int) (IListener, error) {
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

	if err = syscall.Listen(fd, maxSoMaxConn); err != nil {
		return nil, err
	}

	return &Listener{
		conn: &Connection{
			fd:        fd,
			localAddr: laddr,
		},
	}, nil
}
