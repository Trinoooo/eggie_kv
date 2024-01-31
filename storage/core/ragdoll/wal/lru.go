package wal

import (
	"container/list"
	"sync"
)

type Lru struct {
	mu   sync.Mutex
	list *list.List
	size int
	m    map[interface{}]*list.Element
}

type item struct {
	k, v interface{}
}

func newLru(size int) *Lru {
	return &Lru{
		list: list.New(),
		size: size,
		m:    map[interface{}]*list.Element{},
	}
}

func (lru *Lru) read(key interface{}) interface{} {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	elem, exist := lru.m[key]
	if !exist {
		return nil
	}

	lru.list.MoveToFront(elem)
	return elem.Value.(*item).v
}

func (lru *Lru) write(key, data interface{}) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	elem, exist := lru.m[key]
	if exist {
		lru.list.MoveToFront(elem)
		elem.Value = &item{
			k: key,
			v: data,
		}
		return
	}

	lru.m[key] = lru.list.PushFront(&item{
		k: key,
		v: data,
	})
	if lru.list.Len() > lru.size {
		delete(lru.m, lru.list.Remove(lru.list.Back()).(*item).k)
	}
}
