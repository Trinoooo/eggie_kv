package ragdoll

import (
	"github.com/Trinoooo/eggie_kv/errs"
	"github.com/Trinoooo/eggie_kv/utils"
	"github.com/bytedance/gopkg/collection/lscq"
	"unsafe"
)

type Result struct {
	Error *errs.KvErr
	Value []byte
}

type Task struct {
	batch  *Batch
	result chan *Result
}

type Channel struct {
	notifier *utils.UnboundChan
	queue    *lscq.PointerQueue
}

func NewChannel() *Channel {
	return &Channel{
		notifier: utils.NewUnboundChan(),
		queue:    lscq.NewPointer(),
	}
}

func (c *Channel) Produce(batch *Batch) chan *Result {
	result := make(chan *Result)
	c.queue.Enqueue(unsafe.Pointer(&Task{
		batch:  batch,
		result: result,
	}))
	c.notifier.In()
	return result
}

func (c *Channel) Consume() *Task {
	c.notifier.Out()
	data, _ := c.queue.Dequeue()
	return (*Task)(data)
}
