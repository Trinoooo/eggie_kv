package server

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type SimpleServer struct {
	cfg      *TConfig
	wg       sync.WaitGroup
	closed   atomic.Bool
	stopChan chan struct{}

	processor       IProcessor
	serverTransport IServerTransport
	itranFactory    ITransportFactory
	otranFactory    ITransportFactory
	iprotFactory    IProtocolFactory
	oprotFactory    IProtocolFactory
}

func NewSimpleServer(
	processor IProcessor,
	serverTransport IServerTransport,
	itranFactory, otranFactory ITransportFactory,
	iprotFactory, oprotFactory IProtocolFactory,
) *SimpleServer {
	CheckConnectionInterval = 1 * time.Second
	return &SimpleServer{
		cfg: &TConfig{
			ServerStopTimeout: time.Duration(0),
		},
		stopChan:        make(chan struct{}),
		processor:       processor,
		serverTransport: serverTransport,
		itranFactory:    itranFactory,
		otranFactory:    otranFactory,
		iprotFactory:    iprotFactory,
		oprotFactory:    oprotFactory,
	}
}

func (s *SimpleServer) Serve() error {
	err := s.serverTransport.Listen()
	if err != nil {
		return err
	}
	return s.acceptLoop()
}

func (s *SimpleServer) acceptLoop() error {
	for {
		if closed := s.closed.Load(); closed {
			break
		}

		trans, err := s.serverTransport.Accept()
		if err != nil {
			return err
		}

		s.wg.Add(2)
		s.serve(trans)
	}
	return nil
}

func (s *SimpleServer) serve(trans ITransport) {
	ctx, cancel := context.WithCancel(context.Background())
	// 处理请求的协程，支持长链接
	go func() {
		defer s.wg.Done()
		defer cancel()
		_ = s.processLoop(ctx, trans)
		// todo：打日志，而不是返回错误。
	}()

	// 控制协程，负责终止处理协程
	go func() {
		defer s.wg.Done()
		select {
		case <-s.stopChan:
			// 关闭Trans，防止新请求打入。
			// 但无法中断正在执行的处理过程，也不应该终止。
			_ = trans.Close()
			// todo：打日志，而不是返回错误。
		case <-ctx.Done():
		}
	}()
}

func (s *SimpleServer) processLoop(ctx context.Context, trans ITransport) error {
	for {
		iprot := s.iprotFactory.Build(s.itranFactory.Build(trans))
		oprot := s.oprotFactory.Build(s.otranFactory.Build(trans))
		ok, err := s.processor.Process(ctx, iprot, oprot)
		if !ok || err != nil {
			// todo： 补充链接关闭特判
			return err
		}
	}
}

func (s *SimpleServer) Close() error {
	ok := s.closed.CompareAndSwap(false, true)
	if !ok {
		return nil
	}

	// 关闭serverTransport，避免新链接建立
	err := s.serverTransport.Close()
	if err != nil {
		return err
	}

	// 等子协程执行完毕，通过ctx.Done通知
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		s.wg.Wait()
		cancel()
	}()

	// 如果设置了Close超时时间，通过管道广播给子协程，超时后停止执行没执行完的handler
	if s.cfg.ServerStopTimeout > 0 {
		timer := time.NewTimer(s.cfg.ServerStopTimeout)
		select {
		case <-timer.C:
		case <-ctx.Done():
		}
		close(s.stopChan)
	}

	<-ctx.Done()
	return nil
}
