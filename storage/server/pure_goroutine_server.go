package server

import (
	"context"
	"log"
	"net"
	"sync"
)

// PureGoroutineServer 没用多路IO复用，纯goroutine并发
type PureGoroutineServer struct {
	mutex           sync.Mutex
	serverTransport net.Listener
	handler         simpleHandler
	stop            chan struct{}
	done            sync.WaitGroup
}

func NewPureGoroutineServer(addr string, handler simpleHandler) (*PureGoroutineServer, error) {
	var srv = &PureGoroutineServer{
		handler: handler,
		stop:    make(chan struct{}),
	}
	var err error
	srv.serverTransport, err = net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return srv, nil
}

func (pgc *PureGoroutineServer) Serve() error {
	for {
		select {
		case <-pgc.stop:
			return nil
		default:
		}

		conn, err := pgc.serverTransport.Accept()
		if err != nil {
			pgc.mutex.Lock()
			select {
			case <-pgc.stop:
				pgc.mutex.Unlock()
				return nil
			default:
				log.Println(err)
				close(pgc.stop)
				pgc.mutex.Unlock()
				return err
			}
		}

		pgc.done.Add(2)
		ctx, cancel := context.WithCancel(context.Background())
		// bizHandler
		go func() {
			defer func() {
				if err := conn.Close(); err != nil {
					log.Println(err)
				}
				pgc.done.Done()
				cancel()
			}()
			pgc.handler(ctx, conn)
		}()
		// notifier
		go func() {
			defer pgc.done.Done()
			select {
			case <-pgc.stop:
				cancel()
			case <-ctx.Done():
			}
		}()
	}
}

func (pgc *PureGoroutineServer) Close() error {
	pgc.mutex.Lock()
	defer pgc.mutex.Unlock()
	err := pgc.serverTransport.Close()
	if err != nil {
		return err
	}
	close(pgc.stop)
	pgc.done.Wait()
	return nil
}
