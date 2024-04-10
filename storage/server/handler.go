package server

import (
	"github.com/Trinoooo/eggie_kv/storage/cli"
)

type EggieKvHandlerImpl struct {
}

func NewEggieKvHandlerImpl() *EggieKvHandlerImpl {
	return &EggieKvHandlerImpl{}
}

func (e *EggieKvHandlerImpl) HandleGet(req *HandleGetArgs) (*HandleGetResult, error) {
	resp := NewHandleGetResult()
	v, err := cli.Core.Get(string(req.Key))
	if err != nil {
		return nil, err
	}
	resp.Data = v
	resp.Code = 0
	resp.Message = "success"
	return resp, nil
}

func (e *EggieKvHandlerImpl) HandleSet(req *HandleSetArgs) (*HandleSetResult, error) {
	resp := NewHandleSetResult()
	err := cli.Core.Set(string(req.Key), req.Value)
	if err != nil {
		return nil, err
	}
	resp.Code = 0
	resp.Message = "success"
	return resp, nil
}
