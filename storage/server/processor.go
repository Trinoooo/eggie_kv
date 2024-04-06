package server

type IProcessor interface {
	Process(iprot, oprot IProtocol) (bool, error)
}

type KvProcessor struct {
	registerMap map[string]ProcessorHandleFunc
}

func (p *KvProcessor) Process(iprot, oprot IProtocol) (bool, error) {
	name, _, seqId, err := iprot.ReadMessageBegin()
	if err != nil {
		return false, err
	}

	handleFunc, ok := p.registerMap[name]
	if !ok {
		e := iprot.ReadMessageEnd()
		if e != nil {
			return false, e
		}

		return false, nil
	}

	return handleFunc(seqId, iprot, oprot)
}

type ProcessorHandleFunc func(seqId int32, iprot, oprot IProtocol) (bool, error)

func (p *KvProcessor) Register(name string, handler ProcessorHandleFunc) {
	p.registerMap[name] = handler
}

func NewKvProcessor() *KvProcessor {
	return &KvProcessor{
		registerMap: make(map[string]ProcessorHandleFunc),
	}
}
