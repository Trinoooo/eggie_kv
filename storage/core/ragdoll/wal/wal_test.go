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
	segmentSize := 100 * consts.MB
	opts := NewOptions().
		SetDirPerm(0770).
		SetDataPerm(0660).
		SetSegmentCacheSize(5).
		SetSegmentSize(int64(segmentSize)).
		SetNoSync()
	wal, err := Open("../../../../test_data/wal/", opts)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = wal.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	for i := 0; i < 3e7; i++ {
		_, err := wal.Write(testList[3])
		if err != nil {
			t.Fatal(err)
			return
		}
	}
}

// TestLog_Read
// * 测试读日志
// * opt用默认的
func TestLog_Read(t *testing.T) {
	wal, err := Open("../../../../test_data/wal/", nil)
	if err != nil {
		t.Fatal(err)
	}

	readIdxList := []int64{
		3000000,
		3000000,
		6000000,
		7000000,
		9000000,
		12000000,
		29400000,
	}

	for _, idx := range readIdxList {
		data, err := wal.Read(idx)
		if err != nil {
			t.Fatal(err)
		}

		t.Log(data)
	}

	err = wal.Close()
	if err != nil {
		t.Fatal(err)
	}
}

// TestLog_Sync
// * opt设置不主动刷盘，日志持久化的时机只有单数据文件满以及主动调用wal.Sync
func TestLog_Sync(t *testing.T) {
	segmentSize := 100 * consts.MB
	opts := NewOptions().
		SetSegmentSize(int64(segmentSize))
	wal, err := Open("../../../../test_data/wal/", opts)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		_, err := wal.Write([]byte{1, 3, 5, 2, 4, 6})
		if err != nil {
			t.Fatal(err)
		}
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
	wal, err := Open("../../../../test_data/wal/", nil)
	if err != nil {
		t.Fatal(err)
	}

	err = wal.Truncate(3000000)
	if err != nil {
		t.Fatal(err)
	}

	err = wal.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestLog_TruncateAgain(t *testing.T) {
	wal, err := Open("../../../../test_data/wal/", nil)
	if err != nil {
		t.Fatal(err)
	}

	err = wal.Truncate(29400000)
	if err != nil {
		t.Fatal(err)
	}

	err = wal.Close()
	if err != nil {
		t.Fatal(err)
	}
}

var testList = [][]byte{
	{1, 3, 5, 2, 4, 6},
	{1, 2, 3, 4, 5, 6, 7, 8, 9, 0},
	{1},
	{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
	},
	{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
	},
}

func BenchmarkWal_Write_6byte(b *testing.B) {
	benchmarkInner(b, testList[0])
}

func BenchmarkWal_Write_10byte(b *testing.B) {
	benchmarkInner(b, testList[1])
}

func BenchmarkWal_Write_1byte(b *testing.B) {
	benchmarkInner(b, testList[2])
}

func BenchmarkWal_Write_100byte(b *testing.B) {
	benchmarkInner(b, testList[3])
}

func BenchmarkWal_Write_1000byte(b *testing.B) {
	benchmarkInner(b, testList[4])
}

func benchmarkInner(b *testing.B, data []byte) {
	segmentSize := 100 * consts.MB
	opts := NewOptions().
		SetDirPerm(0770).
		SetDataPerm(0660).
		SetSegmentCacheSize(5).
		SetSegmentSize(int64(segmentSize))
	wal, err := Open("../../../../test_data/wal/", opts)
	if err != nil {
		b.Fatal(err)
	}
	defer wal.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := wal.Write(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}
