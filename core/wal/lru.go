package wal

import (
	"container/list"
	"github.com/Trinoooo/eggie_kv/utils"
	"sync"
)

type Lru struct {
	mu   sync.Mutex
	list *list.List
	size int
	m    map[int]*list.Element
}

func NewLru(size int) *Lru {
	return &Lru{
		list: list.New(),
		size: size,
		m:    map[int]*list.Element{},
	}
}

type item struct {
	idx   int
	value []byte
}

func (lru *Lru) Read(idx int) []byte {
	var res []byte
	utils.WithLock(&lru.mu, func() {
		v, exist := lru.m[idx]
		if !exist {
			return
		}

		vv := v.Value.(*item)
		res = vv.value
		lru.list.Remove(v)
		lru.list.PushFront(vv)
	})
	return res
}

func (lru *Lru) Write(idx int, data []byte) {
	utils.WithLock(&lru.mu, func() {
		_, exist := lru.m[idx]
		if exist {
			return
		}

		v := &item{
			idx:   idx,
			value: data,
		}
		lru.m[idx] = lru.list.PushFront(v)
		if lru.list.Len() > lru.size {
			last := lru.list.Back()
			v := lru.list.Remove(last).(*item)
			delete(lru.m, v.idx)
		}
	})
}
