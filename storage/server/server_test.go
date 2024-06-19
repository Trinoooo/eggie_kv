package server

import (
	"context"
	"encoding/binary"
	"github.com/cloudwego/netpoll"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"testing"
	"time"
)

const (
	addr = "127.0.0.1:9999"

	fixSize = 8

	concurrency = 18
)

func TestMain(m *testing.M) {

	log.SetFlags(log.Llongfile | log.LstdFlags)
	m.Run()
}

func mockClient(t *testing.T, closeServerCallback func() error) {
	time.Sleep(500 * time.Millisecond) // wait for server start
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, 100)
	inflight := sync.WaitGroup{}
	// 短连接场景下可控制并发度
	for i := 0; i < concurrency; i++ {
		inflight.Add(2)
		go func() {
			defer inflight.Done()
			shortConnection(t, buf)
		}()
		go func() {
			defer inflight.Done()
			longConnection(t, buf)
		}()
	}
	inflight.Wait()
	if err := closeServerCallback(); err != nil {
		t.Error(err)
	}
}

func shortConnection(t *testing.T, buf []byte) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
		return
	}
	_, e := conn.Write(buf)
	if e != nil {
		t.Error(e)
		return
	}
	log.Println("[short] client send request successfully")
	innerBuf := make([]byte, 8)
	_, e = conn.Read(innerBuf)
	if e != nil {
		t.Error(e)
		return
	}
	log.Println("[short] client recv response successfully")
	e = conn.Close()
	if e != nil {
		t.Error(e)
		return
	}
	log.Println("[short] client close connection")
}

func longConnection(t *testing.T, buf []byte) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
		return
	}
	for i := 0; i < 10; i++ {
		_, e := conn.Write(buf)
		if e != nil {
			t.Error(e)
			return
		}
		log.Println("[long] client send request successfully")
		innerBuf := make([]byte, 8)
		_, e = conn.Read(innerBuf)
		if e != nil {
			t.Error(e)
			return
		}
		log.Println("[long] client recv response successfully")
	}
	e := conn.Close()
	if e != nil {
		t.Error(e)
		return
	}
	log.Println("[long] client close connection")
}

func commonHandler(conn net.Conn, t *testing.T) {
	buf := make([]byte, fixSize)
	_, err := conn.Read(buf)
	if err != nil {
		t.Error(err)
	}
	v := binary.BigEndian.Uint64(buf)
	v = bizLogic(v)
	binary.BigEndian.PutUint64(buf, v)
	_, err = conn.Write(buf)
	if err != nil {
		t.Error(err)
	}
}

func bizLogic(v uint64) uint64 {
	// 模拟耗时cpu密集性计算
	for i := 0; i < 1000000; i++ {
		v += uint64(i)
	}
	// 模拟耗时io密集性操作
	time.Sleep(1 * time.Second)
	return v
}

func TestPureGoroutineServer(t *testing.T) {
	return
	server, err := NewPureGoroutineServer(addr, func(ctx context.Context, conn net.Conn) {
		commonHandler(conn, t)
	})
	if err != nil {
		t.Error(err)
		return
	}
	go mockClient(t, server.Close)
	err = server.Serve()
	if err != nil {
		t.Error(err)
	}
}

func TestNetpollEventLoopServer(t *testing.T) {
	return
	server, err := NewNetpollEventLoopServer(addr, func(ctx context.Context, connection netpoll.Connection) error {
		commonHandler(connection, t)
		return nil
	})
	if err != nil {
		t.Error(err)
		return
	}
	go mockClient(t, server.Close)
	err = server.Serve()
	if err != nil {
		t.Error(err)
	}
}

func TestReactorServer(t *testing.T) {
	// 启动性能分析服务器
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	server, err := NewReactorServer(addr, func(ctx context.Context, conn net.Conn) {
		commonHandler(conn, t)
	})
	if err != nil {
		t.Error(err)
		return
	}
	go mockClient(t, server.Close)
	err = server.Serve()
	if err != nil {
		t.Error(err)
	}

}

func TestProactorServer(t *testing.T) {
	//todo
}

func BenchmarkNewNetpollEventLoopServer(b *testing.B) {

}

func BenchmarkNewPureGoroutineServer(b *testing.B) {

}

func BenchmarkNewReactorServer(b *testing.B) {

}
