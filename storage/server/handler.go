package server

import (
	"github.com/Trinoooo/eggie_kv/consts"
)

func (srv *Server) HandleGet(req *consts.KvRequest) (*consts.KvResponse, error) {
	value, err := srv.core.Get(string(req.Key))
	if err != nil {
		return nil, err
	}
	return newSuccessResp(value), nil
}

func (srv *Server) HandleSet(req *consts.KvRequest) (*consts.KvResponse, error) {
	err := srv.core.Set(string(req.Key), req.Value)
	if err != nil {
		return nil, err
	}
	return newSuccessResp(nil), nil
}
