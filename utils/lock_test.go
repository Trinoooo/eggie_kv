package utils

import (
	"sync"
	"testing"
)

func TestWrapLock(t *testing.T) {
	var (
		mu        sync.Mutex
		globalVar int64
	)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go WrapLock(&mu, func() {
		defer wg.Done()
		loop(10, func() {
			globalVar++
		})
	})

	wg.Add(1)
	go WrapLock(&mu, func() {
		defer wg.Done()
		loop(10, func() {
			globalVar++
		})
	})

	wg.Add(1)
	go WrapLock(&mu, func() {
		defer wg.Done()
		loop(10, func() {
			globalVar++
		})
	})

	wg.Wait()
	t.Log("global var:", globalVar)
}

func loop(times int, fn func()) {
	for i := 0; i < times; i++ {
		fn()
	}
}
