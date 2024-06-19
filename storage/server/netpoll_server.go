package server

import (
	"context"
	"github.com/cloudwego/netpoll"
	"net"
)

// NetpollEventLoopServer 字节的NIO eventLoop 网络库
type NetpollEventLoopServer struct {
	listener  net.Listener
	eventLoop netpoll.EventLoop
}

func NewNetpollEventLoopServer(addr string, handler netpoll.OnRequest) (*NetpollEventLoopServer, error) {
	var srv = &NetpollEventLoopServer{}
	var err error

	srv.listener, err = netpoll.CreateListener("tcp", addr)
	if err != nil {
		return nil, err
	}

	srv.eventLoop, err = netpoll.NewEventLoop(handler)
	if err != nil {
		return nil, err
	}

	return srv, nil
}

func (els *NetpollEventLoopServer) Serve() error {
	return els.eventLoop.Serve(els.listener)
}

func (els *NetpollEventLoopServer) Close() error {
	return els.eventLoop.Shutdown(context.Background())
}
