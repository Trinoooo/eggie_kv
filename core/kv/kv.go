package kv

import (
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/Trinoooo/eggie_kv/core/wal"
	"sync"
)

var (
	Kv   *KV
	once sync.Once
)

func InitKv(opt *Option) {
	once.Do(func() {
		var err error
		Kv, err = NewKV(opt)
		if err != nil {
			panic(err)
		}
	})
}

type Option struct {
	Durable bool // 是否持久化存储
}

type KV struct {
	Opt       *Option
	Data      *Data
	Wal       *wal.Log
	BatchPool sync.Pool
	Chan      *Channel
}

func NewKV(opt *Option) (*KV, error) {
	data, err := NewData("")
	if err != nil {
		return nil, err
	}

	wal, err := wal.Open()
	if err != nil {
		return nil, err
	}

	kv := &KV{
		Opt:  opt,
		Data: data,
		Wal:  wal,
		BatchPool: sync.Pool{
			New: NewBatch,
		},
		Chan: NewChannel(),
	}

	go func() {
		for task := kv.Chan.Consume(); ; {
			handleTask(task)
		}
	}()

	return kv, nil
}

func handleTask(task *Task) {
	// TODO
}

func (kv *KV) Get(key string) ([]byte, error) {
	batch := kv.BatchPool.Get().(*Batch)
	batch.Reset()
	batch.AppendOps(
		NewOp(consts.OperatorTypeGet, key, nil),
	)

	result := <-kv.Chan.Produce(batch)
	return result.Value, result.Error
}

func (kv *KV) Set(key string, value []byte) error {
	batch := kv.BatchPool.Get().(*Batch)
	batch.Reset()
	batch.AppendOps(
		NewOp(consts.OperatorTypeSet, key, value),
	)

	result := <-kv.Chan.Produce(batch)
	return result.Error
}
