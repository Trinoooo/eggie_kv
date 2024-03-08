package data

import (
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/Trinoooo/eggie_kv/errs"
	"github.com/Trinoooo/eggie_kv/storage/core/ragdoll/logs"
	"go.uber.org/zap"
	"sync"
)

// limitedSizeMemPool 有元素大小限制的内存池
type limitedSizeMemPool struct {
	cond     *sync.Cond
	mu       *sync.Mutex
	pool     sync.Pool
	counter  int64
	size     int64
	pch, cch chan any
}

func newLimitedSizeMemPool(size int64, initializer func() any) (*limitedSizeMemPool, error) {
	if size <= 0 {
		e := errs.NewInvalidParamErr()
		logs.Error(e.Error(), zap.String(consts.LogFieldParams, "size"), zap.Int64(consts.LogFieldValue, size))
		return nil, e
	}

	if initializer == nil {
		e := errs.NewInvalidParamErr()
		logs.Error(e.Error(), zap.String(consts.LogFieldParams, "initializer"), zap.Any(consts.LogFieldValue, initializer))
		return nil, e
	}

	lp := &limitedSizeMemPool{
		pool: sync.Pool{
			New: initializer,
		},
		mu:   &sync.Mutex{},
		size: size,
		pch:  make(chan any),
		cch:  make(chan any),
	}
	lp.cond = sync.NewCond(lp.mu)

	go lp.consumer()
	go lp.producer()

	return lp, nil
}

func (lp *limitedSizeMemPool) Get() any {
	return <-lp.pch
}

func (lp *limitedSizeMemPool) Put(element any) {
	lp.cch <- element
}

func (lp *limitedSizeMemPool) consumer() {
	for {
		lp.mu.Lock()
		for lp.counter == 0 {
			lp.cond.Wait()
		}
		lp.mu.Unlock()

		lp.pool.Put(<-lp.cch)

		lp.mu.Lock()
		lp.counter--
		lp.mu.Unlock()

		lp.cond.Signal()
	}
}

func (lp *limitedSizeMemPool) producer() {
	for {
		lp.pch <- lp.pool.Get()
		lp.mu.Lock()
		lp.counter++
		for lp.counter >= lp.size {
			lp.cond.Wait()
		}
		lp.mu.Unlock()
		lp.cond.Signal()
	}
}
