package wal

import (
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/Trinoooo/eggie_kv/errs"
	"math/rand"
	"testing"
	"time"
)

/*
-rwxr-x---   1 somebody  staff          0 Feb 17 20:47 .lock
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 00000000
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 00794375
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 01588750
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 02383125
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 03177500
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 03971875
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 04766250
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 05560625
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 06355000
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 07149375
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 07943750
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 08738125
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 09532500
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 10326875
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 11121250
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 11915625
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 12710000
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 13504375
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 14298750
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 15093125
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 15887500
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 16681875
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 17476250
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 18270625
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 19065000
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 19859375
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 20653750
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 21448125
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 22242500
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 23036875
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 23831250
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 24625625
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 25420000
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 26214375
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 27008750
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 27803125
-rw-------   1 somebody  staff  104857500 Feb 17 20:47 28597500
-rw-------   1 somebody  staff  104857105 Feb 17 20:47 29391875
-rw-------   1 somebody  staff  104857571 Feb 17 20:47 30095575
-rw-------   1 somebody  staff  104856798 Feb 17 20:47 30505027
-rw-------   1 somebody  staff  104856622 Feb 17 20:47 30917049
-rw-------   1 somebody  staff  104856841 Feb 17 20:47 31327486
-rw-------   1 somebody  staff  104857445 Feb 17 20:47 31736170
-rw-------   1 somebody  staff  104857586 Feb 17 20:47 32147996
-rw-------   1 somebody  staff  104857470 Feb 17 20:48 32558850
-rw-------   1 somebody  staff  104857404 Feb 17 20:48 32970689
-rw-------   1 somebody  staff  104856787 Feb 17 20:48 33382542
-rw-------   1 somebody  staff  104857572 Feb 17 20:48 33792700
-rw-------   1 somebody  staff  104857292 Feb 17 20:48 34201817
-rw-------   1 somebody  staff  104857190 Feb 17 20:48 34612449
-rw-------   1 somebody  staff  104857132 Feb 17 20:48 35023381
-rw-------   1 somebody  staff  104856862 Feb 17 20:48 35433646
-rw-------   1 somebody  staff  104856736 Feb 17 20:48 35844151
-rw-------   1 somebody  staff  104857300 Feb 17 20:48 36255033
-rw-------   1 somebody  staff  104857129 Feb 17 20:48 36665039
-rw-------   1 somebody  staff  104857563 Feb 17 20:48 37076868
-rw-------   1 somebody  staff  104857121 Feb 17 20:49 37488228
-rw-------   1 somebody  staff  104856871 Feb 17 20:49 37898101
-rw-------   1 somebody  staff  104856810 Feb 17 20:49 38307691
-rw-------   1 somebody  staff  104857591 Feb 17 20:49 38716437
-rw-------   1 somebody  staff  104857464 Feb 17 20:49 39127826
-rw-------   1 somebody  staff  104857013 Feb 17 20:49 39539275
-rw-------   1 somebody  staff   12876944 Feb 17 20:49 39949750.active
*/

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
		0,        // 第一个segment第一个
		0,        // 重复读
		700000,   // 第一个segment中间
		794374,   // 第一个segment的最后一个
		794375,   // 第二个segment的第一个
		30095575, // 中间随便一个segment的第一个
		30099999, // 中间随便一个segment中间
		30505026, // 中间随便一个segment的最后一个
		39949750, // activeSegment中的第一个
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

	truncateIdxList := []int64{
		0,        // 第一个segment第一个
		700000,   // 第一个segment中间
		794374,   // 第一个segment的最后一个
		794375,   // 第二个segment的第一个
		30095575, // 中间随便一个segment的第一个
		30099999, // 中间随便一个segment中间
		30505026, // 中间随便一个segment的最后一个
		39949750, // activeSegment中的第一个
	}
	for _, idx := range truncateIdxList {
		err = wal.Truncate(idx)
		if err != nil {
			t.Fatal(err)
		}

		l, err := wal.Len()
		if err != nil {
			t.Error(err)
		}
		t.Log(l)
	}

	err = wal.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestLog_OpenAgain(t *testing.T) {
	wal, err := Open("../../../../test_data/wal/", nil)
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
