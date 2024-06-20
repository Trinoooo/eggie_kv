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

type simpleHandler func(ctx context.Context, conn *Conn)
