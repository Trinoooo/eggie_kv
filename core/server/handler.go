package server

import (
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/Trinoooo/eggie_kv/core/kv"
)

type HandleFunc func(request *consts.KvRequest) (*consts.KvResponse, error)

func HandleGet(req *consts.KvRequest) (*consts.KvResponse, error) {
	value, err := kv.Kv.Get(string(req.Key))
	if err != nil {
		return nil, err
	}
	return newSuccessResp(value), nil
}

func HandleSet(req *consts.KvRequest) (*consts.KvResponse, error) {
	err := kv.Kv.Set(string(req.Key), req.Value)
	if err != nil {
		return nil, err
	}
	return newSuccessResp(nil), nil
}
