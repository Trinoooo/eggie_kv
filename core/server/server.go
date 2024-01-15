package server

import (
	"encoding/json"
	"errors"
	"github.com/Trinoooo/eggie_kv/consts"
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
)

type Server struct {
	operatorHandler map[consts.OperatorType]HandleFunc
	mws             []MiddlewareFunc
}

func NewServer() *Server {
	return &Server{
		operatorHandler: map[consts.OperatorType]HandleFunc{},
		mws:             make([]MiddlewareFunc, 0),
	}
}

func (srv *Server) Server(resp http.ResponseWriter, req *http.Request) {
	kvReq, err := parseKvReq(req)
	if err != nil {
		log.Error("parse kvReq err:", err)
		_, _ = resp.Write(mustMarshalKvResp(newExceptionResp(err)))
		return
	}

	handler, ok := srv.operatorHandler[kvReq.OperationType]
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

func (srv *Server) WithHandler(op consts.OperatorType, handler HandleFunc) {
	srv.operatorHandler[op] = handler
}

func (srv *Server) WithMiddleware(mw ...MiddlewareFunc) {
	srv.mws = append(srv.mws, mw...)
}

func parseKvReq(req *http.Request) (*consts.KvRequest, error) {
	kvReq := &consts.KvRequest{}

	bodyBytes, err := io.ReadAll(req.Body)
	if err = json.Unmarshal(bodyBytes, kvReq); err != nil {
		log.Error("json unmarshal err:", err)
		return nil, consts.JsonUnmarshalErr
	}

	return kvReq, nil
}

func mustMarshalKvResp(resp *consts.KvResponse) []byte {
	respBytes, err := json.Marshal(resp)
	if err != nil {
		log.Error("json marshal err:", err)
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
