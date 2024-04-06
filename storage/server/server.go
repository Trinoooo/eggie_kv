package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/Trinoooo/eggie_kv/errs"
	"github.com/Trinoooo/eggie_kv/storage/core"
	"github.com/Trinoooo/eggie_kv/storage/core/iface"
	"github.com/Trinoooo/eggie_kv/storage/logs"
	"github.com/spf13/viper"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type Server struct {
	operatorHandlers map[consts.OperatorType]HandleFunc
	mws              []MiddlewareFunc
	core             iface.ICore
	config           *viper.Viper
}

func NewServer() (*Server, error) {
	srv := &Server{
		operatorHandlers: map[consts.OperatorType]HandleFunc{},
		mws:              make([]MiddlewareFunc, 0),
	}

	err := srv.withConfig()
	if err != nil {
		return nil, err
	}

	err = srv.withCore()
	if err != nil {
		return nil, err
	}

	srv.withMiddleware(
		ParamsValidateMw,
		LogMw,
	)
	srv.withHandler(consts.OperatorTypeGet, srv.HandleGet)
	srv.withHandler(consts.OperatorTypeSet, srv.HandleSet)
	return srv, nil
}

func (srv *Server) Server(resp http.ResponseWriter, req *http.Request) {
	kvReq, err := parseKvReq(req)
	if err != nil {
		logs.Error(fmt.Sprintf("parse kvReq errs: %#v", err))
		_, _ = resp.Write(mustMarshalKvResp(newExceptionResp(err)))
		return
	}

	handler, ok := srv.operatorHandlers[kvReq.OperationType]
	if !ok {
		logs.Warn(fmt.Sprintf("unsupported operation type: %#v", kvReq.OperationType))
		_, _ = resp.Write(mustMarshalKvResp(newExceptionResp(errs.NewUnsupportedOperatorTypeErr())))
		return
	}

	wrappedHandler := handler
	for _, mw := range srv.mws {
		wrappedHandler = mw(wrappedHandler)
	}

	kvResp, err := wrappedHandler(kvReq)
	if err != nil {
		logs.Error(fmt.Sprintf("execute handle failed: %#v", err))
		_, _ = resp.Write(mustMarshalKvResp(newExceptionResp(err)))
		return
	}

	_, _ = resp.Write(mustMarshalKvResp(kvResp))
}

func (srv *Server) withHandler(op consts.OperatorType, handler HandleFunc) {
	srv.operatorHandlers[op] = handler
}

func (srv *Server) withMiddleware(mw ...MiddlewareFunc) {
	srv.mws = append(srv.mws, mw...)
}

func (srv *Server) withCore() error {
	coreBuilder, exist := core.BuilderMap[srv.config.GetString(consts.Core)]
	if !exist {
		e := errs.NewCoreNotFoundErr()
		logs.Error(e.Error())
		return e
	}
	c, err := coreBuilder(srv.config)
	if err != nil {
		return err
	}
	srv.core = c
	return nil
}

func (srv *Server) withConfig() error {
	srv.config = viper.New()
	srv.config.AddConfigPath(consts.DefaultConfigPath)
	srv.config.SetConfigName("config")
	srv.config.SetConfigType("yaml")
	err := srv.config.ReadInConfig()
	if err != nil {
		return err
	}
	return nil
}

func parseKvReq(req *http.Request) (*consts.KvRequest, error) {
	kvReq := &consts.KvRequest{}

	bodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		e := errs.NewReadSocketErr().WithErr(err)
		logs.Error(e.Error())
		return nil, e
	}

	if err = json.Unmarshal(bodyBytes, kvReq); err != nil {
		e := errs.NewJsonUnmarshalErr().WithErr(err)
		logs.Error(e.Error())
		return nil, e
	}

	return kvReq, nil
}

func mustMarshalKvResp(resp *consts.KvResponse) []byte {
	respBytes, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	return respBytes
}

func newExceptionResp(err error) *consts.KvResponse {
	var kvErr = errs.NewUnknownErr()
	errors.As(err, &kvErr)
	return &consts.KvResponse{
		Code:    kvErr.Code(),
		Message: kvErr.Error(),
	}
}

func newSuccessResp(data []byte) *consts.KvResponse {
	return &consts.KvResponse{
		Message: "success",
		Code:    0,
		Data:    data,
	}
}

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
		_ = s.processLoop(trans)
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

func (s *SimpleServer) processLoop(trans ITransport) error {
	for {
		iprot := s.iprotFactory.Build(s.itranFactory.Build(trans))
		oprot := s.oprotFactory.Build(s.otranFactory.Build(trans))
		ok, err := s.processor.Process(iprot, oprot)
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
