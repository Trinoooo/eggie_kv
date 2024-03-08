package data

import (
	"crypto/md5"
	"encoding/binary"
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/Trinoooo/eggie_kv/errs"
	"github.com/Trinoooo/eggie_kv/storage/core/ragdoll/logs"
	"go.uber.org/zap"
)

// fixedLengthRecord 定长记录
// 内存结构 & 文件内存储结构：
// ---------------------------------------------
// | 	idx - 8字节		| 	next - 8字节			|
// ---------------------------------------------
// | 	length - 1字节	| 	reserved - 15字节	|
// ---------------------------------------------
// | 	signature - 16字节						|
// ---------------------------------------------
// | 	data - 224字节							|
// ---------------------------------------------
type fixedLengthRecord struct {
	idx    int64 // idx 全局索引
	next   int64 // next 片内下一条记录索引
	length int8  // length 记录有效长度
	// reserved  [15]byte  // reserved 保留字段，向后扩展
	signature [16]byte // signature 记录签名
	data      []byte   // data 记录数据内容
}

// newFixedLengthRecord 初始化空定长记录
func newFixedLengthRecord() *fixedLengthRecord {
	return &fixedLengthRecord{}
}

func (flr *fixedLengthRecord) fill(idx, next int64, length int8, data []byte) {
	flr.idx = idx
	flr.next = next
	flr.length = length
	flr.data = data
	flr.sign()
}

func (flr *fixedLengthRecord) marshal() []byte {
	buf := make([]byte, lengthOfRecord)
	binary.PutVarint(buf, flr.idx)
	binary.PutVarint(buf[offsetToNext:], flr.next)
	buf[offsetToLength] = byte(flr.length)
	for i := 0; i < lengthOfSignature; i++ {
		buf[offsetToSignature+i] = flr.signature[i]
	}
	copy(buf[offsetToData:], flr.data)
	return buf
}

func (flr *fixedLengthRecord) unmarshal(data []byte) error {
	if lodata := len(data); lodata != lengthOfRecord {
		e := errs.NewInvalidParamErr()
		logs.Error(
			e.Error(),
			zap.String(consts.LogFieldParams, "len(data)"),
			zap.Int(consts.LogFieldValue, lodata),
		)
		return e
	}

	flr.idx, _ = binary.Varint(data)
	flr.next, _ = binary.Varint(data[offsetToNext:])
	flr.length = int8(data[offsetToLength])
	arr := [16]byte{}
	for i := 0; i < lengthOfSignature; i++ {
		arr[i] = data[offsetToSignature+i]
	}
	flr.signature = arr
	flr.data = data[offsetToData : flr.length+offsetToData]
	return nil
}

func (flr *fixedLengthRecord) sign() {
	dataToSign := make([]byte, 240)
	binary.PutVarint(dataToSign, flr.idx)
	binary.PutVarint(dataToSign[offsetToNext:], flr.next)
	dataToSign[offsetToLength] = byte(flr.length)
	copy(dataToSign[offsetToSignature:], flr.data)
	flr.signature = md5.Sum(dataToSign)
}
