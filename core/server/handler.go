package server

import (
	"github.com/Trinoooo/eggie_kv/core/kv"
)

type HandleFunc func(request *KvRequest) (*KvResponse, error)

func HandleGet(req *KvRequest) (*KvResponse, error) {
	value, err := kv.Kv.Get(string(req.Key))
	if err != nil {
		return nil, err
	}
	return newSuccessResp(value), nil
}

func HandleSet(req *KvRequest) (*KvResponse, error) {
	err := kv.Kv.Set(string(req.Key), req.Value)
	if err != nil {
		return nil, err
	}
	return newSuccessResp(nil), nil
}
