package utils

import (
	"sync"
	"testing"
	"time"
)

func TestUnboundChan(t *testing.T) {
	uc := NewUnboundChan()
	serialize(t, uc)
	parallel(t, uc)
	tclose(t, uc)
}

func serialize(t *testing.T, uc *UnboundChan) {
	for i := 0; i < 10000; i++ {
		uc.In()
	}
	t.Log("[serialize] finish in")

	for i := 0; i < 10000; i++ {
		uc.Out()
	}
	t.Log("[serialize] finish out")
	go func() {
		time.Sleep(1 * time.Second)
		t.Log("[serialize] arise")
		uc.In()
	}()
	uc.Out()
	t.Log("[serialize] try out")
	t.Log("[serialize] len of uc buffer:", uc.Len())
}

func parallel(t *testing.T, uc *UnboundChan) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			uc.In()
		}
		t.Log("[parallel] finish in")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			uc.Out()
		}
		t.Log("[parallel] finish out")
	}()

	wg.Wait()
	t.Log("[parallel] len of uc buffer:", uc.Len())
	go func() {
		time.Sleep(1 * time.Second)
		t.Log("[parallel] arise")
		uc.In()
	}()
	uc.Out()
	t.Log("[parallel] try out")
}

func tclose(t *testing.T, uc *UnboundChan) {
	uc.In()
	uc.Close()
	t.Log("[tclose] uc close")
	t.Log("[tclose] finish in")
	// uc.In()
	// t.Log("[tclose] try in")

	uc.Out()
	t.Log("[tclose] can out")
	ok := uc.Out()
	if ok {
		t.Log("[tclose] try out")
	}
}
