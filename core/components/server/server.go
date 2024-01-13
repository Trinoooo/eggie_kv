package server

import (
	"encoding/json"
	"errors"
	"github.com/Trinoooo/eggie_kv/consts"
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
)

type KvRequest struct {
	OperationType consts.OperatorType `json:"operation_type"`
	Key           []byte              `json:"key"`
	Value         []byte              `json:"value"`
}

type KvResponse struct {
	Code    int64  `json:"code"`
	Message string `json:"message"`
	Data    []byte `json:"data"`
}

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

func parseKvReq(req *http.Request) (*KvRequest, error) {
	kvReq := &KvRequest{}

	bodyBytes, err := io.ReadAll(req.Body)
	if err = json.Unmarshal(bodyBytes, kvReq); err != nil {
		log.Error("json unmarshal err:", err)
		return nil, consts.JsonUnmarshalErr
	}

	return kvReq, nil
}

func mustMarshalKvResp(resp *KvResponse) []byte {
	respBytes, err := json.Marshal(resp)
	if err != nil {
		log.Error("json marshal err:", err)
		panic(err)
	}
	return respBytes
}

func newExceptionResp(err error) *KvResponse {
	var kvErr = consts.UnexpectErr
	errors.As(err, &kvErr)
	return &KvResponse{
		Code:    kvErr.Code(),
		Message: kvErr.Error(),
	}
}

func newSuccessResp(data []byte) *KvResponse {
	return &KvResponse{
		Message: "success",
		Code:    0,
		Data:    data,
	}
}
