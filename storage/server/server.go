package server

import (
	"encoding/json"
	"errors"
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/Trinoooo/eggie_kv/storage/core"
	"github.com/Trinoooo/eggie_kv/storage/core/iface"
	log "github.com/sirupsen/logrus"
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
		log.Error("parse kvReq errs:", err)
		_, _ = resp.Write(mustMarshalKvResp(newExceptionResp(err)))
		return
	}

	handler, ok := srv.operatorHandlers[kvReq.OperationType]
	if !ok {
		log.Warn("unsupported operation type:", kvReq.OperationType)
		_, _ = resp.Write(mustMarshalKvResp(newExceptionResp(consts.UnsupportedOperatorTypeErr)))
		return
	}

	wrappedHandler := handler
	for _, mw := range srv.mws {
		wrappedHandler = mw(wrappedHandler)
	}

	kvResp, err := wrappedHandler(kvReq)
	if err != nil {
		log.Error("execute handle failed:", err)
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
	coreBuilder := core.BuilderMap[srv.config.GetString(consts.Core)]
	c, err := coreBuilder(srv.config)
	if err != nil {
		return consts.BuildCoreErr
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
	if err = json.Unmarshal(bodyBytes, kvReq); err != nil {
		log.Error("json unmarshal errs:", err)
		return nil, consts.JsonUnmarshalErr
	}

	return kvReq, nil
}

func mustMarshalKvResp(resp *consts.KvResponse) []byte {
	respBytes, err := json.Marshal(resp)
	if err != nil {
		log.Error("json marshal errs:", err)
		panic(err)
	}
	return respBytes
}

func newExceptionResp(err error) *consts.KvResponse {
	var kvErr = consts.UnexpectErr
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
