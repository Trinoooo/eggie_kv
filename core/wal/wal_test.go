package wal

import (
	"github.com/Trinoooo/eggie_kv/consts"
	log "github.com/sirupsen/logrus"
	"testing"
)

func TestMain(m *testing.M) {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
	m.Run()
}

// TestLog_Write
// * 测试写入与从文件中恢复（日志文件夹下存在数据文件）
// * opt采用自定义
func TestLog_Write(t *testing.T) {
	segmentSize := consts.KB
	opts := NewOptions().
		SetDirPerm(0770).
		SetDataPerm(0660).
		SetSegmentCacheSize(5).
		SetSegmentSize(uint64(segmentSize))
	wal, err := Open("../../test_data/wal/", opts)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		data := []byte{1, 3, 5, 2, 4, 6}
		idx, err := wal.Write(data)
		if err != nil {
			t.Fatal(err)
		}
		log.Info("finish append block, log idx:", idx)
	}

	err = wal.Close()
	if err != nil {
		t.Fatal(err)
	}
}

// TestLog_Read
// * 测试读日志
// * opt用默认的
func TestLog_Read(t *testing.T) {
	wal, err := Open("../../test_data/wal/", nil)
	if err != nil {
		t.Fatal(err)
	}

	data, err := wal.Read(10)
	if err != nil {
		t.Fatal(err)
	}
	log.Info(data)

	err = wal.Close()
	if err != nil {
		t.Fatal(err)
	}
}

// TestLog_Sync
// * opt设置不主动刷盘，日志持久化的时机只有单数据文件满以及主动调用wal.Sync
func TestLog_Sync(t *testing.T) {
	opts := NewOptions().
		SetSegmentSize(consts.KB).
		SetNoSync()
	wal, err := Open("../../test_data/wal/", opts)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		idx, err := wal.Write([]byte{1, 3, 5, 2, 4, 6})
		if err != nil {
			t.Fatal(err)
		}
		log.Info("finish append block, log idx:", idx)
	}

	err = wal.Sync()
	if err != nil {
		t.Fatal(err)
	}

	err = wal.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestLog_Truncate(t *testing.T) {
	wal, err := Open("../../test_data/wal/", nil)
	if err != nil {
		t.Fatal(err)
	}

	err = wal.Truncate(40)
	if err != nil {
		t.Fatal(err)
	}

	err = wal.Close()
	if err != nil {
		t.Fatal(err)
	}
}
