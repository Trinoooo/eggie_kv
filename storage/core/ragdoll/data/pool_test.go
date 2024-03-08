package data

import (
	"github.com/Trinoooo/eggie_kv/errs"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func defaultInitializer() any {
	return struct{}{}
}

func TestMain(m *testing.M) {
	m.Run()
}

// TestNewLimitedSizeMemPool 创建元素大小限制的内存池成功
func TestNewLimitedSizeMemPool(t *testing.T) {
	_, err := newLimitedSizeMemPool(10, defaultInitializer)
	assert.Nil(t, err)
}

// TestNewLimitedSizeMemPoolFailed 创建元素大小限制的内存池失败
//   - 传入非法size
//   - 传入非法initializer
func TestNewLimitedSizeMemPoolFailed(t *testing.T) {
	_, err := newLimitedSizeMemPool(0, defaultInitializer)
	assert.Equal(t, errs.GetCode(err), errs.InvalidParamErrCode)

	_, err = newLimitedSizeMemPool(-10, defaultInitializer)
	assert.Equal(t, errs.GetCode(err), errs.InvalidParamErrCode)

	_, err = newLimitedSizeMemPool(1, nil)
	assert.Equal(t, errs.GetCode(err), errs.InvalidParamErrCode)
}

// TestLimitedSizeMemPoolGet 测试获取元素，以及达到元素大小限制时获取
func TestLimitedSizeMemPoolGet(t *testing.T) {
	t.Log(time.Now().Unix(), time.Now().UnixNano())
	lsmp, _ := newLimitedSizeMemPool(10, defaultInitializer)
	for i := 0; i < 10; i++ {
		lsmp.Get()
	}

	go func() {
		time.Sleep(1 * time.Second)
		t.Log(time.Now().Unix(), time.Now().UnixNano(), "prev awake...")
		lsmp.Put(struct{}{})
	}()
	lsmp.Get()
	t.Log(time.Now().Unix(), time.Now().UnixNano(), "post got...")
}

func TestLimitedSizeMemPoolPut(t *testing.T) {
	lsmp, _ := newLimitedSizeMemPool(10, defaultInitializer)
	go func() {
		time.Sleep(1 * time.Second)
		t.Log(time.Now().Unix(), time.Now().UnixNano(), "prev awake...")
		lsmp.Get()
	}()
	lsmp.Put(struct{}{})
	t.Log(time.Now().Unix(), time.Now().UnixNano(), "post put...")

	for i := 0; i < 10; i++ {
		lsmp.Get()
	}

	for i := 0; i < 10; i++ {
		lsmp.Put(struct{}{})
	}

	go func() {
		time.Sleep(1 * time.Second)
		t.Log(time.Now().Unix(), time.Now().UnixNano(), "prev awake...")
		lsmp.Get()
	}()
	lsmp.Put(struct{}{})
	t.Log(time.Now().Unix(), time.Now().UnixNano(), "post put...")
}
