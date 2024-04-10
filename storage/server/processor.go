package server

import "fmt"

type IProcessor interface {
	Process(iprot, oprot IProtocol) (bool, error)
}

type KvProcessor struct {
	registerMap map[string]ProcessorFunc
}

func (p *KvProcessor) Process(iprot, oprot IProtocol) (bool, error) {
	name, _, seqId, err := iprot.ReadMessageBegin()
	if err != nil {
		return false, err
	}

	processor, ok := p.registerMap[name]
	if !ok {
		// 当找不到processor时，读完请求，并返回异常响应。
		e := iprot.Skip(TTYPE_STRUCT)
		if e != nil {
			return false, e
		}
		e = iprot.ReadMessageEnd()
		if e != nil {
			return false, e
		}
		e = oprot.WriteMessageBegin(name, MESSAGE_TYPE_EXCEPTION, seqId)
		if e != nil {
			return false, e
		}
		e = oprot.WriteMessageEnd()
		if e != nil {
			return false, e
		}
		e = oprot.Flush()
		if e != nil {
			return false, e
		}
		return false, fmt.Errorf("processor not found")
	}

	return processor.Process(seqId, iprot, oprot)
}

type ProcessorFunc interface {
	Process(seqId int32, iprot, oprot IProtocol) (bool, error)
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
	HandleGet(args *HandleGetArgs) (*HandleGetResult, error)
	HandleSet(args *HandleSetArgs) (*HandleSetResult, error)
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

func (p *HandleGetProcessor) Process(seqId int32, iprot, oprot IProtocol) (bool, error) {
	args := NewHandleGetArgs()
	err := args.Read(iprot)
	if err != nil {
		return false, err
	}

	resp, err := p.handler.HandleGet(args)

	err = oprot.WriteMessageBegin("HandleGet", MESSAGE_TYPE_REPLY, seqId)
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

type HandleSetProcessor struct {
	handler EggieKvHandler
}

func (p *HandleSetProcessor) Process(seqId int32, iprot, oprot IProtocol) (bool, error) {
	args := NewHandleSetArgs()
	err := args.Read(iprot)
	if err != nil {
		return false, err
	}

	resp, err := p.handler.HandleSet(args)

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
