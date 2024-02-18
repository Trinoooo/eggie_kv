package wal

import (
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/Trinoooo/eggie_kv/errs"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	// 每次测试之前删除测试数据
	err := os.RemoveAll("../../../../test_data/")
	if err != nil {
		panic(err)
	}

	m.Run()
}

/*
-rwxr-x---   1 x  staff          0 Feb 18 10:22 .lock
-rw-------   1 x  staff  104857500 Feb 18 10:22 00000000
-rw-------   1 x  staff  104857500 Feb 18 10:22 00794375
-rw-------   1 x  staff  104857500 Feb 18 10:22 01588750
-rw-------   1 x  staff  104857500 Feb 18 10:22 02383125
-rw-------   1 x  staff  104857500 Feb 18 10:22 03177500
-rw-------   1 x  staff  104857500 Feb 18 10:22 03971875
-rw-------   1 x  staff  104857500 Feb 18 10:22 04766250
-rw-------   1 x  staff  104857500 Feb 18 10:22 05560625
-rw-------   1 x  staff  104857500 Feb 18 10:22 06355000
-rw-------   1 x  staff  104857500 Feb 18 10:22 07149375
-rw-------   1 x  staff  104857500 Feb 18 10:22 07943750
-rw-------   1 x  staff  104857500 Feb 18 10:22 08738125
-rw-------   1 x  staff  104857500 Feb 18 10:22 09532500
-rw-------   1 x  staff  104857500 Feb 18 10:22 10326875
-rw-------   1 x  staff  104857500 Feb 18 10:22 11121250
-rw-------   1 x  staff  104857500 Feb 18 10:22 11915625
-rw-------   1 x  staff  104857500 Feb 18 10:22 12710000
-rw-------   1 x  staff  104857500 Feb 18 10:22 13504375
-rw-------   1 x  staff  104857500 Feb 18 10:22 14298750
-rw-------   1 x  staff  104857500 Feb 18 10:22 15093125
-rw-------   1 x  staff  104857500 Feb 18 10:22 15887500
-rw-------   1 x  staff  104857500 Feb 18 10:22 16681875
-rw-------   1 x  staff  104857500 Feb 18 10:22 17476250
-rw-------   1 x  staff  104857500 Feb 18 10:22 18270625
-rw-------   1 x  staff  104857500 Feb 18 10:22 19065000
-rw-------   1 x  staff  104857500 Feb 18 10:22 19859375
-rw-------   1 x  staff  104857500 Feb 18 10:22 20653750
-rw-------   1 x  staff  104857500 Feb 18 10:22 21448125
-rw-------   1 x  staff  104857500 Feb 18 10:22 22242500
-rw-------   1 x  staff  104857500 Feb 18 10:23 23036875
-rw-------   1 x  staff  104857500 Feb 18 10:23 23831250
-rw-------   1 x  staff  104857500 Feb 18 10:23 24625625
-rw-------   1 x  staff  104857500 Feb 18 10:23 25420000
-rw-------   1 x  staff  104857500 Feb 18 10:23 26214375
-rw-------   1 x  staff  104857500 Feb 18 10:23 27008750
-rw-------   1 x  staff  104857500 Feb 18 10:23 27803125
-rw-------   1 x  staff  104857500 Feb 18 10:23 28597500
-rw-------   1 x  staff  104856968 Feb 18 10:23 29391875
-rw-------   1 x  staff  104857579 Feb 18 10:23 30095834
-rw-------   1 x  staff  104857459 Feb 18 10:23 30507158
-rw-------   1 x  staff  104857488 Feb 18 10:23 30918393
-rw-------   1 x  staff  104857505 Feb 18 10:23 31330009
-rw-------   1 x  staff  104857254 Feb 18 10:23 31740631
-rw-------   1 x  staff  104856595 Feb 18 10:23 32151358
-rw-------   1 x  staff  104857382 Feb 18 10:23 32561774
-rw-------   1 x  staff  104857557 Feb 18 10:23 32972964
-rw-------   1 x  staff  104857074 Feb 18 10:23 33383874
-rw-------   1 x  staff  104856823 Feb 18 10:23 33794185
-rw-------   1 x  staff  104857338 Feb 18 10:23 34204088
-rw-------   1 x  staff  104856770 Feb 18 10:23 34613752
-rw-------   1 x  staff  104857471 Feb 18 10:23 35025051
-rw-------   1 x  staff  104857383 Feb 18 10:23 35435693
-rw-------   1 x  staff  104857247 Feb 18 10:23 35847313
-rw-------   1 x  staff  104857541 Feb 18 10:23 36259152
-rw-------   1 x  staff  104856799 Feb 18 10:23 36668661
-rw-------   1 x  staff  104857535 Feb 18 10:23 37077621
-rw-------   1 x  staff  104857377 Feb 18 10:23 37487306
-rw-------   1 x  staff  104856693 Feb 18 10:23 37900141
-rw-------   1 x  staff  104857470 Feb 18 10:23 38308559
-rw-------   1 x  staff  104857100 Feb 18 10:23 38718098
-rw-------   1 x  staff  104857188 Feb 18 10:23 39128970
-rw-------   1 x  staff  104857592 Feb 18 10:23 39538923
-rw-------   1 x  staff   12893193 Feb 18 10:23 39949649.active
*/

// testData
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

// TestOptions_CheckSuccess option各项配置设置在合法范围内，check能够通过
func TestOptions_CheckSuccess(t *testing.T) {
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

// TestOptions_CheckFailed option各项配置设置在合法范围外，check无法通过
func TestOptions_CheckFailed(t *testing.T) {
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

// TestLog_WriteNormal 写入等长记录
func TestLog_WriteNormal(t *testing.T) {
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

// TestLog_WriteAbnormal 写如变长记录
func TestLog_WriteAbnormal(t *testing.T) {
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
	seed := 2001 // 保证结果可复现
	r := rand.New(rand.NewSource(int64(seed)))
	for i := 0; i < 1e7; i++ {
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

// TestLog_WriteFailed 写入数据不合法，写入失败
func TestLog_WriteFailed(t *testing.T) {
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
		23831250, // 定长记录中的随便一个segment的第一个
		24000000, // 定长记录中的随便一个segment的中间
		24625624, // 定长记录中的随便一个segment的最后一个
		30507158, // 变长记录中的随便一个segment的第一个
		30900000, // 变长记录中的随便一个segment的中间
		30918392, // 变长记录中的随便一个segment的最后一个
		39949649, // activeSegment中的第一个
		39950000, // activeSegment的中间
		39999999, // activeSegment中的最后一个
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

// TestLog_ReadFailed 尝试读取一个有效范围外的日志
func TestLog_ReadFailed(t *testing.T) {
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

// TestLog_Truncate 截断日志
func TestLog_Truncate(t *testing.T) {
	wal, err := Open("../../../../test_data/wal/", nil)
	if err != nil {
		t.Error(err)
	}

	truncateIdxList := []int64{
		0,        // 第一个segment第一个
		700000,   // 第一个segment中间
		794374,   // 第一个segment的最后一个
		794375,   // 第二个segment的第一个
		23831250, // 定长记录中的随便一个segment的第一个
		24000000, // 定长记录中的随便一个segment的中间
		24625624, // 定长记录中的随便一个segment的最后一个
		30507158, // 变长记录中的随便一个segment的第一个
		30900000, // 变长记录中的随便一个segment的中间
		30918392, // 变长记录中的随便一个segment的最后一个
		39949649, // activeSegment中的第一个
		39950000, // activeSegment的中间
		39999999, // activeSegment中的最后一个
	}
	for _, idx := range truncateIdxList {
		err = wal.Truncate(idx)
		if err != nil {
			t.Error(err)
		}

		l, err := wal.Len()
		if err != nil {
			t.Error(err)
		}
		t.Log(l)
	}

	err = wal.Close()
	if err != nil {
		t.Error(err)
	}
}

// TestLog_OpenOnlyEmptySegment wal目录下只有一个空segment
func TestLog_OpenOnlyEmptySegment(t *testing.T) {
	wal, err := Open("../../../../test_data/wal/", nil)
	if err != nil {
		t.Error(err)
	}

	l, err := wal.Len()
	if err != nil {
		t.Error(err)
	}
	t.Log(l)

	err = wal.Close()
	if err != nil {
		t.Error(err)
	}
}

// TestLog_OpenWithDir wal目录下有预期外的目录
func TestLog_OpenWithDir(t *testing.T) {
	err := os.Mkdir("../../../../test_data/wal/dir", 0770)
	if err != nil {
		t.Error(err)
	}

	wal, err := Open("../../../../test_data/wal/", nil)
	if err != nil {
		t.Error(err)
	}

	err = wal.Close()
	if err != nil {
		t.Error(err)
	}
}

// TestLog_OpenWithDuplicateActiveSegment wal目录下有多个.active后缀的文件
func TestLog_OpenWithDuplicateActiveSegment(t *testing.T) {
	_, err := os.Create("../../../../test_data/wal/39949649.active")
	if err != nil {
		t.Log(err)
	}

	wal, err := Open("../../../../test_data/wal/", nil)
	if err != nil {
		t.Error(err)
	}

	err = wal.Close()
	if err != nil {
		t.Error(err)
	}

	err = os.Remove("../../../../test_data/wal/39949649")
	if err != nil {
		t.Error(err)
	}

	err = os.Remove("../../../../test_data/wal/39950001.active")
	if err != nil {
		t.Error(err)
	}
}

/*// TestLog_Corrupt 测试文件内容损坏
func TestLog_Corrupt(t *testing.T) {
	corrupt, err := utils.CheckAndCreateFile("../../../../test_data/wal/00000000.active", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660)
	if err != nil {
		t.Error(err)
	}
	zeroI64 := []byte{0, 10, 0, 0, 0, 0, 0, 0}
	// 读入数据比header尺寸小
	_, err = corrupt.Write(zeroI64)
	if err != nil {
		t.Error(err)
	}

	_, err = Open("../../../../test_data/wal/", nil)
	if err != nil {
		t.Log(err)
	}

	// 读到的raw长度小于读到的length
	var tmp []byte
	for i := 0; i <= 4; i++ {
		tmp = append(tmp, zeroI64...)
	}
	_, err = corrupt.Write(tmp)
	if err != nil {
		t.Error(err)
	}

	_, err = Open("../../../../test_data/wal/", nil)
	if err != nil {
		t.Log(err)
	}

	// checksum不匹配
	var tt []byte
	for i := 0; i <= 20; i++ {
		tt = append(tt, zeroI64...)
	}
	_, err = corrupt.Write(zeroI64)
	if err != nil {
		t.Error(err)
	}

	_, err = Open("../../../../test_data/wal/", nil)
	if err != nil {
		t.Log(err)
	}
}*/

// TestLog_Sync 同步磁盘
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

// TestLog_WriteNormalToFull 写入等长记录到写满
func TestLog_WriteNormalToFull(t *testing.T) {
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

	for {
		_, err := wal.Write(testData[3])
		if err != nil {
			t.Log(err)
			break
		}
	}

	length, err := wal.Len()
	if err != nil {
		t.Error(err)
	}
	t.Log(length)

	err = wal.Close()
	if err != nil {
		t.Error(err)
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
