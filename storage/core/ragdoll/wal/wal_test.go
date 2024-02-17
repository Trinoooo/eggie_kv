package wal

import (
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/Trinoooo/eggie_kv/errs"
	"math/rand"
	"testing"
	"time"
)

// testData
// 测试写入和读取的日志内容
// 写入内容体积分别为6、10、1、100、1000
var testData = [][]byte{
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

// TestOptions_check_success
// option各项配置设置在合法范围内，check能够通过
func TestOptions_check_success(t *testing.T) {
	opts := NewOptions().
		SetDataPerm(0777).
		SetDirPerm(0777).
		SetSegmentCapacity(10 * consts.MB).
		SetSegmentCacheSize(100).
		SetNoSync().
		SetSyncInterval(time.Second)

	err := opts.check()
	if err != nil {
		t.Error(err)
	}
}

// TestOptions_check_failed
// option各项配置设置在合法范围外，check无法通过
func TestOptions_check_failed(t *testing.T) {
	o1 := NewOptions().SetDataPerm(0)
	err := o1.check()
	if err != nil && errs.GetCode(err) != errs.InvalidParamErrCode {
		t.Error(err)
	}

	o2 := NewOptions().SetDataPerm(01777)
	err = o2.check()
	if err != nil && errs.GetCode(err) != errs.InvalidParamErrCode {
		t.Error(err)
	}

	o3 := NewOptions().SetDirPerm(0)
	err = o3.check()
	if err != nil && errs.GetCode(err) != errs.InvalidParamErrCode {
		t.Error(err)
	}

	o4 := NewOptions().SetDirPerm(02777)
	err = o4.check()
	if err != nil && errs.GetCode(err) != errs.InvalidParamErrCode {
		t.Error(err)
	}

	o5 := NewOptions().SetSegmentCacheSize(-1)
	err = o5.check()
	if err != nil && errs.GetCode(err) != errs.InvalidParamErrCode {
		t.Error(err)
	}

	o6 := NewOptions().SetSegmentCapacity(consts.KB)
	err = o6.check()
	if err != nil && errs.GetCode(err) != errs.InvalidParamErrCode {
		t.Error(err)
	}

	o7 := NewOptions().SetSegmentCapacity(2 * consts.GB)
	err = o7.check()
	if err != nil && errs.GetCode(err) != errs.InvalidParamErrCode {
		t.Error(err)
	}

	o8 := NewOptions().SetSyncInterval(-1)
	err = o8.check()
	if err != nil && errs.GetCode(err) != errs.InvalidParamErrCode {
		t.Error(err)
	}
}

// TestLog_Write_normal
// * 写入3e7条数据，每条数据100字节大小
func TestLog_Write_normal(t *testing.T) {
	segmentSize := 100 * consts.MB
	opts := NewOptions().
		SetDirPerm(0770).
		SetDataPerm(0660).
		SetSegmentCacheSize(5).
		SetSegmentCapacity(int64(segmentSize)).
		SetNoSync()
	wal, err := Open("../../../../test_data/wal/", opts)
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 3e7; i++ {
		_, err := wal.Write(testData[3])
		if err != nil {
			t.Error(err)
		}
	}

	err = wal.Close()
	if err != nil {
		t.Error(err)
	}
}

// TestLog_Write_abnormal
// 写入1e7条数据，每条数据大小不固定
func TestLog_Write_abnormal(t *testing.T) {
	segmentSize := 100 * consts.MB
	opts := NewOptions().
		SetDirPerm(0770).
		SetDataPerm(0660).
		SetSegmentCacheSize(5).
		SetSegmentCapacity(int64(segmentSize)).
		SetNoSync()
	wal, err := Open("../../../../test_data/wal/", opts)
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 1e7; i++ {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		_, err := wal.Write(testData[r.Int()%5])
		if err != nil {
			t.Error(err)
		}
	}

	err = wal.Close()
	if err != nil {
		t.Error(err)
	}
}

// TestLog_Write_failed
// 写入数据不合法，写入失败
func TestLog_Write_failed(t *testing.T) {
	segmentSize := 100 * consts.MB
	opts := NewOptions().
		SetDirPerm(0770).
		SetDataPerm(0660).
		SetSegmentCacheSize(5).
		SetSegmentCapacity(int64(segmentSize)).
		SetNoSync()
	wal, err := Open("../../../../test_data/wal/", opts)
	if err != nil {
		t.Error(err)
	}

	_, err = wal.Write([]byte{})
	if err != nil && errs.GetCode(err) != errs.InvalidParamErrCode {
		t.Error(err)
	}

	err = wal.Close()
	if err != nil {
		t.Error(err)
	}
}

// TestLog_Read 测试读日志
// * 重复读同一个segment
// * lru
// * 读activeSegment
func TestLog_Read(t *testing.T) {
	wal, err := Open("../../../../test_data/wal/", nil)
	if err != nil {
		t.Error(err)
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
		blockIdxToData, err := wal.Read(idx)
		if err != nil {
			t.Error(err)
		}

		t.Log(len(blockIdxToData))
	}

	err = wal.Close()
	if err != nil {
		t.Error(err)
	}
}

// TestLog_Read_failed
// 尝试读取一个有效范围外的日志
func TestLog_Read_failed(t *testing.T) {
	wal, err := Open("../../../../test_data/wal/", nil)
	if err != nil {
		t.Error(err)
	}

	_, err = wal.Read(-1)
	if err != nil && errs.GetCode(err) != errs.NotFoundErrCode {
		t.Error(err)
	}

	err = wal.Close()
	if err != nil {
		t.Error(err)
	}
}

// TestLog_Sync
// * opt设置不主动刷盘，日志持久化的时机只有单数据文件满以及主动调用wal.Sync
func TestLog_Sync(t *testing.T) {
	segmentSize := 100 * consts.MB
	opts := NewOptions().
		SetSegmentCapacity(int64(segmentSize))
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

	l, err := wal.Len()
	if err != nil {
		t.Error(err)
	}
	t.Log(l)

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

	l, err := wal.Len()
	if err != nil {
		t.Error(err)
	}
	t.Log(l)

	err = wal.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkWal_Write_6byte(b *testing.B) {
	benchmarkInner(b, testData[0])
}

func BenchmarkWal_Write_10byte(b *testing.B) {
	benchmarkInner(b, testData[1])
}

func BenchmarkWal_Write_1byte(b *testing.B) {
	benchmarkInner(b, testData[2])
}

func BenchmarkWal_Write_100byte(b *testing.B) {
	benchmarkInner(b, testData[3])
}

func BenchmarkWal_Write_1000byte(b *testing.B) {
	benchmarkInner(b, testData[4])
}

func benchmarkInner(b *testing.B, data []byte) {
	segmentSize := 100 * consts.MB
	opts := NewOptions().
		SetDirPerm(0770).
		SetDataPerm(0660).
		SetSegmentCacheSize(5).
		SetSegmentCapacity(int64(segmentSize))
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
