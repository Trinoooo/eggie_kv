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
	elem *list.Element
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

func (lru *Lru) write(key, data interface{}) interface{} {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	elem, exist := lru.m[key]
	if exist {
		lru.list.MoveToFront(elem)
		elem.Value = &item{
			k:    key,
			v:    data,
			elem: elem,
		}
		return nil
	}

	newItem := &item{
		k: key,
		v: data,
	}
	e := lru.list.PushFront(newItem)
	newItem.elem = e
	lru.m[key] = e
	if lru.list.Len() > lru.size {
		item := lru.list.Remove(lru.list.Back()).(*item)
		delete(lru.m, item.k)
		return item.v
	}
	return nil
}

func (lru *Lru) remove(key interface{}) interface{} {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	elem, exist := lru.m[key]
	if exist {
		delete(lru.m, key)
		lru.list.Remove(elem)
		return elem.Value.(*item).v
	}

	return nil
}
