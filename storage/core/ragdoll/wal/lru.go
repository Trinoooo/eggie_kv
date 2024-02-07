package wal

import (
	"container/list"
	"sync"
)

// todo: 抽成公用方法

type Lru struct {
	mu   sync.Mutex
	list *list.List
	size int
	m    map[interface{}]*list.Element
}

type item struct {
	k, v interface{}
	e    *list.Element
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

// todo：lock性能
func (lru *Lru) write(key, data interface{}) interface{} {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	elem, exist := lru.m[key]
	if exist {
		lru.list.MoveToFront(elem)
		elem.Value = &item{
			k: key,
			v: data,
			e: elem,
		}
		return nil
	}

	newItem := &item{
		k: key,
		v: data,
	}
	e := lru.list.PushFront(newItem)
	newItem.e = e
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
