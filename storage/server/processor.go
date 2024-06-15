package server

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var CheckConnectionInterval time.Duration

type IProcessor interface {
	Process(ctx context.Context, iprot, oprot IProtocol) (bool, error)
}

type KvProcessor struct {
	registerMap map[string]ProcessorFunc
}

func (p *KvProcessor) Process(ctx context.Context, iprot, oprot IProtocol) (bool, error) {
	var err, finalErr error
	name, _, seqId, err := iprot.ReadMessageBegin()
	if err != nil {
		return false, err
	}

	processor, ok := p.registerMap[name]
	if !ok { // 当找不到processor时，读完请求，并返回异常响应。
		finalErr = // not found
		if err = iprot.Skip(TTYPE_STRUCT); err != nil && finalErr != nil {
			finalErr = err
		}
		if err = iprot.ReadMessageEnd(); err != nil && finalErr != nil {
			finalErr = err
		}
		if err = oprot.WriteMessageBegin(name, MESSAGE_TYPE_EXCEPTION, seqId); err != nil && finalErr != nil {
			finalErr = err
		}
		if err = oprot.WriteMessageEnd(); err != nil && finalErr != nil {
			finalErr = err
		}
		if err = oprot.Flush(); err != nil && finalErr != nil {
			finalErr = err
		}

		if finalErr != nil {
			return false, err
		}
	}

	tickerCtx, tickerCancel := context.WithCancel(ctx)
	if CheckConnectionInterval > 0 {
		var cancelCause func(err error)
		ctx, cancelCause = context.WithCancelCause(ctx)
		go func() {
			ticker := time.NewTicker(CheckConnectionInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C: // 定期检查链接活性
					// 如果链接被对端关闭，那么通过ctx通知processor
					if !iprot.Transport().IsOpen() {
						cancelCause() // abandon
						return
					}
				case <-tickerCtx.Done(): // 取消定时器
					return
				}
			}
		}()
	}

	defer tickerCancel()
	ok, err = processor.Process(ctx, seqId, iprot, oprot)
	if !ok || err != nil {
		// 处理失败，可能的原因有很多，框架层的职责是处理“框架错误”
		if errors.Is(err) || errors.Is(context.Cause(ctx), ) { // abandon
			iprot.Transport().Close()
			return // abandon
		}

		if err = oprot.WriteMessageBegin(name, MESSAGE_TYPE_EXCEPTION, seqId); err != nil && finalErr == nil {
			finalErr = err
		}
		// Exception.Write
		if err = oprot.WriteMessageEnd(); err != nil && finalErr == nil {
			finalErr = err
		}
		if err = oprot.Flush(); err != nil && finalErr == nil {
			finalErr = err
		}
		return false, finalErr
	}
	return ok, err
}

type ProcessorFunc interface {
	Process(ctx context.Context, seqId int32, iprot, oprot IProtocol) (bool, error)
}

func (p *KvProcessor) Register(name string, processorFunc ProcessorFunc) {
	p.registerMap[name] = processorFunc
}

func NewKvProcessor(handler EggieKvHandler) *KvProcessor {
	processor := &KvProcessor{
		registerMap: make(map[string]ProcessorFunc),
	}
	processor.Register("HandleGet", &HandleGetProcessor{handler})
	processor.Register("HandleSet", &HandleSetProcessor{handler})
	return processor
}

type EggieKvHandler interface {
	HandleGet(ctx context.Context, args *HandleGetArgs) (*HandleGetResult, error)
	HandleSet(ctx context.Context, args *HandleSetArgs) (*HandleSetResult, error)
}

type OperatorType int64

const (
	OperatorTypeUnknown OperatorType = 0
	OperatorTypeGet     OperatorType = 1
	OperatorTypeSet     OperatorType = 2
)

type HandleGetArgs struct {
	Ops   OperatorType
	Key   []byte
	Value []byte
}

func NewHandleGetArgs() *HandleGetArgs {
	return &HandleGetArgs{
		Ops:   TTYPE_VOID,
		Key:   nil,
		Value: nil,
	}
}

func (a *HandleGetArgs) Read(iprot IProtocol) error {
	_, err := iprot.ReadStructBegin()
	if err != nil {
		return err
	}

	// thrift 这里使用for-case处理，我理解是方便元编程生成代码
	// 自己手搓就怎么舒服怎么来了
	err = a.WrapReadField(iprot, a.ReadField1)
	if err != nil {
		return err
	}

	err = a.WrapReadField(iprot, a.ReadField2)
	if err != nil {
		return err
	}

	err = a.WrapReadField(iprot, a.ReadField3)
	if err != nil {
		return err
	}

	err = iprot.ReadStructEnd()
	if err != nil {
		return err
	}

	return nil
}

func (a *HandleGetArgs) WrapReadField(iprot IProtocol, fn func(iprot IProtocol) error) error {
	_, _, _, err := iprot.ReadFieldBegin()
	if err != nil {
		return err
	}

	err = fn(iprot)
	if err != nil {
		return err
	}

	err = iprot.ReadFieldEnd()
	if err != nil {
		return err
	}
	return nil
}

func (a *HandleGetArgs) ReadField1(iprot IProtocol) error {
	i64, err := iprot.ReadI64()
	if err != nil {
		return err
	}
	a.Ops = OperatorType(i64)
	return nil
}

func (a *HandleGetArgs) ReadField2(iprot IProtocol) error {
	str, err := iprot.ReadString()
	if err != nil {
		return err
	}
	a.Key = []byte(str)
	return nil
}

func (a *HandleGetArgs) ReadField3(iprot IProtocol) error {
	str, err := iprot.ReadString()
	if err != nil {
		return err
	}
	a.Value = []byte(str)
	return nil
}

type HandleGetResult struct {
	Code    int64
	Message string
	Data    []byte
}

func NewHandleGetResult() *HandleGetResult {
	return &HandleGetResult{
		Data: nil,
	}
}

func (r *HandleGetResult) Write(oprot IProtocol) error {
	err := oprot.WriteStructBegin("HandleGetResult")
	if err != nil {
		return err
	}
	err = r.WriteField1(oprot)
	if err != nil {
		return err
	}
	err = r.WriteField2(oprot)
	if err != nil {
		return err
	}
	err = r.WriteField3(oprot)
	if err != nil {
		return err
	}
	err = oprot.WriteStructEnd()
	if err != nil {
		return err
	}
	return nil
}

func (r *HandleGetResult) WriteField1(oprot IProtocol) error {
	err := oprot.WriteFieldBegin("Code", TTYPE_I64, 1)
	if err != nil {
		return err
	}
	err = oprot.WriteI64(r.Code)
	if err != nil {
		return err
	}
	err = oprot.WriteFieldEnd()
	if err != nil {
		return err
	}
	return nil
}

func (r *HandleGetResult) WriteField2(oprot IProtocol) error {
	err := oprot.WriteFieldBegin("Message", TTYPE_STRING, 2)
	if err != nil {
		return err
	}
	err = oprot.WriteString(r.Message)
	if err != nil {
		return err
	}
	err = oprot.WriteFieldEnd()
	if err != nil {
		return err
	}
	return nil
}

func (r *HandleGetResult) WriteField3(oprot IProtocol) error {
	err := oprot.WriteFieldBegin("Data", TTYPE_STRING, 2)
	if err != nil {
		return err
	}
	err = oprot.WriteString(string(r.Data))
	if err != nil {
		return err
	}
	err = oprot.WriteFieldEnd()
	if err != nil {
		return err
	}
	return nil
}

type HandleSetArgs struct {
	Ops   OperatorType
	Key   []byte
	Value []byte
}

func NewHandleSetArgs() *HandleSetArgs {
	return &HandleSetArgs{}
}

func (a *HandleSetArgs) Read(iprot IProtocol) error {
	_, err := iprot.ReadStructBegin()
	if err != nil {
		return err
	}

	// thrift 这里使用for-case处理，我理解是方便元编程生成代码
	// 自己手搓就怎么舒服怎么来了
	err = a.WrapReadField(iprot, a.ReadField1)
	if err != nil {
		return err
	}

	err = a.WrapReadField(iprot, a.ReadField2)
	if err != nil {
		return err
	}

	err = a.WrapReadField(iprot, a.ReadField3)
	if err != nil {
		return err
	}

	err = iprot.ReadStructEnd()
	if err != nil {
		return err
	}

	return nil
}

func (a *HandleSetArgs) WrapReadField(iprot IProtocol, fn func(iprot IProtocol) error) error {
	_, _, _, err := iprot.ReadFieldBegin()
	if err != nil {
		return err
	}

	err = fn(iprot)
	if err != nil {
		return err
	}

	err = iprot.ReadFieldEnd()
	if err != nil {
		return err
	}
	return nil
}

func (a *HandleSetArgs) ReadField1(iprot IProtocol) error {
	i64, err := iprot.ReadI64()
	if err != nil {
		return err
	}
	a.Ops = OperatorType(i64)
	return nil
}

func (a *HandleSetArgs) ReadField2(iprot IProtocol) error {
	str, err := iprot.ReadString()
	if err != nil {
		return err
	}
	a.Key = []byte(str)
	return nil
}

func (a *HandleSetArgs) ReadField3(iprot IProtocol) error {
	str, err := iprot.ReadString()
	if err != nil {
		return err
	}
	a.Value = []byte(str)
	return nil
}

type HandleSetResult struct {
	Code    int64
	Message string
	Data    []byte
}

func NewHandleSetResult() *HandleSetResult {
	return &HandleSetResult{}
}

func (r *HandleSetResult) Write(oprot IProtocol) error {
	err := oprot.WriteStructBegin("HandleGetResult")
	if err != nil {
		return err
	}
	err = r.WriteField1(oprot)
	if err != nil {
		return err
	}
	err = r.WriteField2(oprot)
	if err != nil {
		return err
	}
	err = r.WriteField3(oprot)
	if err != nil {
		return err
	}
	err = oprot.WriteStructEnd()
	if err != nil {
		return err
	}
	return nil
}

func (r *HandleSetResult) WriteField1(oprot IProtocol) error {
	err := oprot.WriteFieldBegin("Code", TTYPE_I64, 1)
	if err != nil {
		return err
	}
	err = oprot.WriteI64(r.Code)
	if err != nil {
		return err
	}
	err = oprot.WriteFieldEnd()
	if err != nil {
		return err
	}
	return nil
}

func (r *HandleSetResult) WriteField2(oprot IProtocol) error {
	err := oprot.WriteFieldBegin("Message", TTYPE_STRING, 2)
	if err != nil {
		return err
	}
	err = oprot.WriteString(r.Message)
	if err != nil {
		return err
	}
	err = oprot.WriteFieldEnd()
	if err != nil {
		return err
	}
	return nil
}

func (r *HandleSetResult) WriteField3(oprot IProtocol) error {
	err := oprot.WriteFieldBegin("Data", TTYPE_STRING, 2)
	if err != nil {
		return err
	}
	err = oprot.WriteString(string(r.Data))
	if err != nil {
		return err
	}
	err = oprot.WriteFieldEnd()
	if err != nil {
		return err
	}
	return nil
}

type HandleGetProcessor struct {
	handler EggieKvHandler
}

func (p *HandleGetProcessor) Process(ctx context.Context, seqId int32, iprot, oprot IProtocol) (bool, error) {
	var err, finalErr error
	args := NewHandleGetArgs()
	err = args.Read(iprot)
	if err != nil {
		return false, err
	}

	resp, err := p.handler.HandleGet(ctx, args)
	if err != nil {
		// 响应异常消息
	}

	// 响应成功消息
	// 错误延迟处理，为了尽可能给对端发送完整消息
	if err = oprot.WriteMessageBegin("HandleGet", MESSAGE_TYPE_REPLY, seqId); err != nil && finalErr == nil {
		finalErr = err
	}
	if err = resp.Write(oprot); err != nil && finalErr == nil {
		finalErr = err
	}
	if err = oprot.WriteMessageEnd(); err != nil && finalErr == nil {
		finalErr = err
	}
	if finalErr != nil {
		return false, finalErr
	}

	return true, nil
}

type HandleSetProcessor struct {
	handler EggieKvHandler
}

func (p *HandleSetProcessor) Process(ctx context.Context, seqId int32, iprot, oprot IProtocol) (bool, error) {
	args := NewHandleSetArgs()
	err := args.Read(iprot)
	if err != nil {
		return false, err
	}

	resp, err := p.handler.HandleSet(ctx, args)

	err = oprot.WriteMessageBegin("HandleSet", MESSAGE_TYPE_REPLY, seqId)
	if err != nil {
		return false, err
	}
	err = resp.Write(oprot)
	if err != nil {
		return false, err
	}
	err = oprot.WriteMessageEnd()
	if err != nil {
		return false, err
	}
	return true, nil
}
