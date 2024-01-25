package ragdoll

import (
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/Trinoooo/eggie_kv/kv_storage/core/iface"
	"github.com/Trinoooo/eggie_kv/kv_storage/core/ragdoll/wal"
	"github.com/spf13/viper"
	"sync"
)

type KV struct {
	Config    *viper.Viper
	Data      *Data
	Wal       *wal.Log
	BatchPool sync.Pool
	Chan      *Channel
}

func New(config *viper.Viper) (iface.ICore, error) {
	data, err := NewData("")
	if err != nil {
		return nil, err
	}

	wal, err := wal.Open("123", wal.NewOptions())
	if err != nil {
		return nil, err
	}

	kv := &KV{
		Config: config,
		Data:   data,
		Wal:    wal,
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
