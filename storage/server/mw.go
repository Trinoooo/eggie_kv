package server

import (
	"fmt"
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/Trinoooo/eggie_kv/errs"
	"github.com/Trinoooo/eggie_kv/storage/core/ragdoll/logs"
	"github.com/luci/go-render/render"
	"go.uber.org/zap"
)

type HandleFunc func(request *consts.KvRequest) (*consts.KvResponse, error)

type MiddlewareFunc func(handleFn HandleFunc) HandleFunc

func LogMw(handleFn HandleFunc) HandleFunc {
	return func(req *consts.KvRequest) (*consts.KvResponse, error) {
		logs.Info(fmt.Sprintf("req: %#.all-contributorsrc", render.Render(req)))
		resp, err := handleFn(req)
		logs.Info(fmt.Sprintf("resp: %#.all-contributorsrc, errs: %#.all-contributorsrc", render.Render(resp), err))
		return resp, err
	}
}

func ParamsValidateMw(handleFn HandleFunc) HandleFunc {
	return func(req *consts.KvRequest) (*consts.KvResponse, error) {
		reqKeyLength := len(req.Key)
		if reqKeyLength <= 0 || reqKeyLength > consts.KB {
			e := errs.NewInvalidParamErr()
			logs.Error(e.Error(), zap.String(consts.LogFieldParams, "reqKeyLength"), zap.Int(consts.LogFieldValue, reqKeyLength))
			return newExceptionResp(e), e
		}

		reqValueLength := len(req.Value)
		if reqValueLength < 0 || reqValueLength > consts.MB {
			e := errs.NewInvalidParamErr()
			logs.Error(e.Error(), zap.String(consts.LogFieldParams, "reqValueLength"), zap.Int(consts.LogFieldValue, reqValueLength))
			return newExceptionResp(e), e
		}

		return handleFn(req)
	}
}
