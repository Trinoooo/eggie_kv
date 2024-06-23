package server

import (
	"errors"
	"github.com/Trinoooo/eggie_kv/errs"
	"github.com/Trinoooo/eggie_kv/storage/server/protocol"
	"log"
	"strings"
	"sync"
	"syscall"
)

type OpType int64

const (
	OpTypeUnknown OpType = 0
	OpTypeGet     OpType = 1
	OpTypeSet     OpType = 2
)

type KvRequest struct {
	OperationType OpType `json:"operation_type"`
	Key           []byte `json:"key"`
	Value         []byte `json:"value"`
}

type KvResponse struct {
	Data []byte `json:"data"`
}

type KvException struct {
	Code    int64  `json:"code"`
	Message string `json:"message"`
}

type HandlerFunc func(req *KvRequest) (*KvResponse, error)

type Processor struct {
	srv                *ReactorServer
	inputProtocol      protocol.IProtocol
	outputProtocol     protocol.IProtocol
	handlers           map[string]HandlerFunc
	stepState          map[string]bool
	task               *Task
	handlerTriggerOnce sync.Once
}

func NewProcessor(srv *ReactorServer, inputProtocol, outputProtocol protocol.IProtocol) *Processor {
	return &Processor{
		srv:            srv,
		inputProtocol:  inputProtocol,
		outputProtocol: outputProtocol,
		handlers: map[string]HandlerFunc{
			"HandleGet": HandleGet,
			"HandleSet": HandleSet,
		},
		stepState: map[string]bool{
			"START": true,
		},
		task: &Task{
			req:   &KvRequest{},
			ready: make(chan struct{}),
		},
	}
}

func (p *Processor) Process() error {
	if err := p.decodeHandler(); err != nil {
		return err
	}

	if err := p.decodeRequest(); err != nil {
		return err
	}

	if err := p.triggerHandler(); err != nil {
		return err
	}

	if p.task.resp != nil {
		if err := p.encodeResponse(); err != nil {
			return err
		}
	} else if p.task.exception != nil {
		if err := p.encodeException(); err != nil {
			return err
		}
	}

	return p.resetState() // for long connection reuse
}

func (p *Processor) GetInputProtocol() protocol.IProtocol {
	return p.inputProtocol
}

func (p *Processor) GetOutputProtocol() protocol.IProtocol {
	return p.outputProtocol
}

func (p *Processor) decodeHandler() error {
	return p.checkOrSetState("DecodeRequestHandler", []string{"START"}, func() (bool, error) {
		handlerKey, err := p.inputProtocol.ReadString()
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			return false, errs.NewTaskNotFinishErr()
		} else if err != nil {
			return false, err
		}

		handler, exist := p.handlers[handlerKey]
		if !exist {
			return false, errs.NewUnexpectHandler()
		}
		p.task.handler = handler
		return true, nil
	})
}

func (p *Processor) decodeRequest() error {
	if err := p.checkOrSetState("DecodeRequestOpType", []string{"DecodeRequestHandler"}, func() (bool, error) {
		opTypeI64, err := p.inputProtocol.ReadI64()
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			return false, errs.NewTaskNotFinishErr()
		} else if err != nil {
			return false, err
		}
		p.task.req.OperationType = OpType(opTypeI64)
		return true, nil
	}); err != nil {
		return err
	}

	if err := p.checkOrSetState("DecodeRequestKey", []string{"DecodeRequestOpType"}, func() (bool, error) {
		key, err := p.inputProtocol.ReadBytes()
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			return false, errs.NewTaskNotFinishErr()
		} else if err != nil {
			return false, err
		}
		p.task.req.Key = key
		return true, nil
	}); err != nil {
		return err
	}

	if err := p.checkOrSetState("DecodeRequestValue", []string{"DecodeRequestKey"}, func() (bool, error) {
		value, err := p.inputProtocol.ReadBytes()
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			return false, errs.NewTaskNotFinishErr()
		} else if err != nil {
			return false, err
		}
		p.task.req.Value = value
		return true, nil
	}); err != nil {
		return err
	}
	return nil
}

func (p *Processor) triggerHandler() error {
	return p.checkOrSetState("TriggerHandler", []string{"DecodeRequestValue"}, func() (bool, error) {
		select {
		case <-p.task.ready:
			return true, nil
		default:
			p.handlerTriggerOnce.Do(func() {
				p.srv.done.Add(1)
				p.srv.pool.Go(func() {
					defer p.srv.done.Done()
					defer close(p.task.ready)
					p.task.Execute()
				})
			})
			return false, errs.NewTaskNotFinishErr()
		}
	})
}

func (p *Processor) encodeResponse() error {
	return p.checkOrSetState("EncodeResponseData", []string{"TriggerHandler"}, func() (bool, error) {
		err := p.outputProtocol.WriteBytes(p.task.resp.Data)
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			return false, errs.NewTaskNotFinishErr()
		} else if err != nil {
			return false, err
		}
		return true, nil
	})
}

func (p *Processor) encodeException() error {
	if err := p.checkOrSetState("EncodeExceptionCode", []string{"TriggerHandler"}, func() (bool, error) {
		err := p.outputProtocol.WriteI64(p.task.exception.Code)
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			return false, errs.NewTaskNotFinishErr()
		} else if err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return err
	}

	if err := p.checkOrSetState("EncodeExceptionMessage", []string{"EncodeExceptionCode"}, func() (bool, error) {
		err := p.outputProtocol.WriteString(p.task.exception.Message)
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			return false, errs.NewTaskNotFinishErr()
		} else if err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return err
	}

	return nil
}

func (p *Processor) resetState() error {
	p.stepState = map[string]bool{
		"START": true,
	}
	p.task = &Task{
		req:   &KvRequest{},
		ready: make(chan struct{}),
	}
	p.handlerTriggerOnce = sync.Once{}
	return nil
}

func (p *Processor) checkOrSetState(currentStepKey string, previousStepKeys []string, fn func() (bool, error)) error {
	// previous step not finish, we should tell caller to retry.
	var previousStepFinish bool
	for _, previousStepKey := range previousStepKeys {
		if psf := p.stepState[previousStepKey]; psf {
			previousStepFinish = true
			break
		}
	}
	if !previousStepFinish {
		connection := p.inputProtocol.GetConnection()
		log.Printf("previous step %v not finish, retry. remote addr: %v, local addr: %v, fd: %v", strings.Join(previousStepKeys, ","), connection.RemoteAddr(), connection.LocalAddr(), connection.RawFd())
		return errs.NewTaskNotFinishErr()
	}

	// current step has already finished. we should skip to avoid execute `fn` more than once.
	if currentStepFinish := p.stepState[currentStepKey]; currentStepFinish {
		connection := p.inputProtocol.GetConnection()
		log.Printf("current step %v alreay finished, skip. remote addr: %v, local addr: %v, fd: %v", currentStepKey, connection.RemoteAddr(), connection.LocalAddr(), connection.RawFd())
		return nil
	}

	isFinish, err := fn()
	if err != nil {
		return err
	}

	p.stepState[currentStepKey] = isFinish
	return nil
}

type Task struct {
	handler   HandlerFunc
	req       *KvRequest
	resp      *KvResponse
	exception *KvException
	ready     chan struct{}
}

func (t *Task) Execute() {
	resp, err := t.handler(t.req)
	if err != nil {
		log.Printf("handler return error: %v", err)
		var kvErr *errs.KvErr
		if errors.As(err, &kvErr) {
			t.exception = &KvException{
				Code:    kvErr.Code(),
				Message: kvErr.Error(),
			}
		} else {
			t.exception = &KvException{
				Code:    errs.UnknownErrCode,
				Message: err.Error(),
			}
		}
	}
	t.resp = resp
}
