package server

import (
	"encoding/binary"
	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"log"
	"net"
	_ "net/http/pprof"
	"sync"
	"testing"
	"time"
)

const (
	addr = "127.0.0.1:9999"

	concurrency = 10
)

func TestMain(m *testing.M) {
	log.SetFlags(log.Llongfile | log.LstdFlags)
	m.Run()
}

/*
type KvRequest struct {
	OperationType OpType `json:"operation_type"`
	Key           []byte `json:"key"`
	Value         []byte `json:"value"`
}
*/

func mockClient(t *testing.T, closeServerCallback func() error) {
	time.Sleep(500 * time.Millisecond) // wait for server start
	buf := make([]byte, 8+9+8+8+0+8+8)
	binary.BigEndian.PutUint64(buf, 9)
	copy(buf[8:], "HandleGet")
	binary.BigEndian.PutUint64(buf[17:], 0)
	binary.BigEndian.PutUint64(buf[25:], 0)
	binary.BigEndian.PutUint64(buf[33:], 8)
	binary.BigEndian.PutUint64(buf[41:], 100)
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

	log.Println("[short] client send request successfully", conn.RemoteAddr(), conn.LocalAddr())
	innerBuf := make([]byte, 8)
	_, e = conn.Read(innerBuf)
	if e != nil {
		t.Error(e)
		return
	}
	log.Println("[short] client recv response successfully", conn.RemoteAddr(), conn.LocalAddr())
	e = conn.Close()
	if e != nil {
		t.Error(e)
		return
	}
	log.Println("[short] client close connection", conn.RemoteAddr(), conn.LocalAddr())
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
		log.Println("[long] client send request successfully", conn.RemoteAddr(), conn.LocalAddr())
		innerBuf := make([]byte, 8)
		_, e = conn.Read(innerBuf)
		if e != nil {
			t.Error(e)
			return
		}
		log.Println("[long] client recv response successfully", conn.RemoteAddr(), conn.LocalAddr())
	}
	e := conn.Close()
	if e != nil {
		t.Error(e)
		return
	}
	log.Println("[long] client close connection", conn.RemoteAddr(), conn.LocalAddr())
}

func TestReactorServer(t *testing.T) {
	// 启动性能采集服务器
	/*go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()*/
	mockey.Mock((*push.Pusher).Add).Return(nil).Build()
	mockey.Mock(prometheus.Counter.Inc).Return().Build()

	server, err := NewReactorServer([4]byte{127, 0, 0, 1}, 9999)
	if err != nil {
		t.Error(err)
		return
	}
	go mockClient(t, server.Close)
	err = server.Serve()
	if err != nil {
		t.Error("server shutdown", err)
	}
}
