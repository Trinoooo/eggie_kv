package server

import (
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/luci/go-render/render"
	log "github.com/sirupsen/logrus"
)

type MiddlewareFunc func(handleFn HandleFunc) HandleFunc

func LogMw(handleFn HandleFunc) HandleFunc {
	return func(req *KvRequest) (*KvResponse, error) {
		log.Info("req:", render.Render(req))
		resp, err := handleFn(req)
		log.Infof("resp: %#v, err: %#v", render.Render(resp), err)
		return resp, err
	}
}

func ParamsValidateMw(handleFn HandleFunc) HandleFunc {
	return func(req *KvRequest) (*KvResponse, error) {
		reqKeyLength := len(req.Key)
		if reqKeyLength <= 0 || reqKeyLength > consts.KB {
			return newExceptionResp(consts.InvalidParamErr), consts.InvalidParamErr
		}

		reqValueLength := len(req.Value)
		if reqValueLength < 0 || reqValueLength > consts.MB {
			return newExceptionResp(consts.InvalidParamErr), consts.InvalidParamErr
		}

		return handleFn(req)
	}
}
