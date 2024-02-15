package utils

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
	e    *list.Element
}

func NewLRU(size int) *Lru {
	return &Lru{
		list: list.New(),
		size: size,
		m:    map[interface{}]*list.Element{},
	}
}

func (lru *Lru) Read(key interface{}) interface{} {
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
func (lru *Lru) Write(key, data interface{}) interface{} {
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

func (lru *Lru) Remove(key interface{}) interface{} {
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

// Traverse 遍历lru中的存储元素，对每个元素执行do方法
// 如果执行do方法过程中出现错误，Traverse 会根据 skipErr
// 决定是否忽略错误，即当 skipErr 为true时，Traverse 不会返回错误
// 当 skipErr 为false时，Traverse 会返回第一个出现的错误
func (lru *Lru) Traverse(do func(item interface{}) error, skipErr bool) error {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	for _, e := range lru.m {
		err := do(e.Value.(*item).v)
		if !skipErr && err != nil {
			return err
		}
	}

	return nil
}
