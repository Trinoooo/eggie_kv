package wal

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/Trinoooo/eggie_kv/storage/core/ragdoll/logs"
	"github.com/Trinoooo/eggie_kv/utils"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	// 字段长度，单位字节
	headerLengthSize  = 8
	headerBlockIdSize = 8
	headerSummarySize = 16
	headerSize        = 32

	// 字段偏移量，单位字节
	headerLengthOffset  = 0
	headerBlockIdOffset = 8
	headerSummaryOffset = 16
	headerDataOffset    = 32
)

const suffix = ".active" // suffix 活跃segment文件的后缀标识

// getBaseFormat 获取segment文件名中blockIdx部分宽度
func getBaseFormat() string {
	return utils.GetValueOnEnv("%010d", "%08d").(string)
}

type position struct {
	start int64 // start 表示起始偏移量
	end   int64 // end 表示结束偏移量
}

// TODO: 补充corrupt处理
type segment struct {
	fd            *os.File // fd segment 文件描述符
	path          string   // path segment 文件路径
	firstBlockIdx *int64   // firstBlockIdx segment 中的起始 blockIdx
	lastBlockIdx  *int64   // lastBlockIdx segment 中最后的 blockIdx
	// blockBuffer segment 中存储的数据内容
	// segment 存储的最大容量取决于外部，存储结构如下：
	// | block #1 | block #2 | 0000 |
	// 当文件剩余容量不足以再写下一个完整 block 时
	// 文件末尾剩余内容不再使用，保证文件开头是一个完整的 block
	blockBuffer []byte
	// blockPosition 指示 blockBuffer 中每个block的位置
	blockPosition      []*position
	blockBufferSyncIdx int64 // blockBufferSyncIdx 下一个要刷盘的 blockBuffer 偏移量

	maxSize   int64 // maxSize segment 文件最大体积
	hasSuffix bool  // hasSuffix segment 文件路径中是否包含.active后缀
}

func newSegment(path string, maxSize int64) *segment {
	seg := &segment{}
	seg.path = path
	seg.maxSize = maxSize
	return seg
}

// open 打开segment
// 需要外部保证线程安全
func (seg *segment) open(perm os.FileMode) error {
	// 如果是新segment文件，则创建一个新文件
	// 否则以追加模式进行操作
	// 指定操作包含读和写
	fd, err := utils.CheckAndCreateFile(seg.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, perm)
	if err != nil {
		e := consts.NewOpenFileErr()
		logs.Error(e.Error())
		return e
	}

	seg.fd = fd
	all, err := io.ReadAll(seg.fd)
	if err != nil {
		e := consts.NewReadFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	bps, bbf, err := loadBinary(all)
	if err != nil {
		return err
	}

	seg.blockBuffer = bbf
	seg.blockPosition = bps
	lengthOfBlockPosition := int64(len(seg.blockPosition))
	seg.blockBufferSyncIdx = seg.blockPosition[lengthOfBlockPosition-1].end + 1
	startBlockIdx, hasSuffix, err := baseToBlockId(filepath.Base(seg.path))
	if err != nil {
		return err
	}
	seg.firstBlockIdx = &startBlockIdx
	if lengthOfBlockPosition > 0 {
		seg.lastBlockIdx = utils.Int64Ptr(*seg.firstBlockIdx + lengthOfBlockPosition - 1)
	}
	seg.hasSuffix = hasSuffix
	return nil
}

// getFirstBlockIdx 获取 segment 中第一个block的blockIdx
// 需要外部保证线程安全
func (seg *segment) getFirstBlockIdx() int64 {
	// note：使用指针类型 (*int) 和使用一个特殊的非法值都可以表示一个 int 值是否被赋过值的状态
	// chatgpt更倾向于前者
	// “根据你的描述和需求，我更倾向于推荐使用指针类型 *int 的方式来表示一个 int 值是否被赋过值。
	// 使用指针类型可以明确地表达该值是否被赋过值，而且它不需要引入额外的字段或特殊的非法值。
	// 只需要通过判断指针是否为 nil 即可得知是否有给其赋过值。
	// 这种方式也比较常见，易于理解和维护。它符合 Go 语言中的惯用方式，能够在代码中清晰地表示该字段的状态。”
	if seg.firstBlockIdx != nil {
		return *seg.firstBlockIdx
	}

	blockId, _, _ := baseToBlockId(filepath.Base(seg.path))
	return blockId
}

// getLastBlockIdx 获取 segment 中最后一个block的blockIdx
// 需要外部保证线程安全
func (seg *segment) getLastBlockIdx() int64 {
	if seg.lastBlockIdx == nil {
		return 0
	}
	return *seg.lastBlockIdx
}

// close 关闭 segment 文件
// 需要外部保证线程安全
func (seg *segment) close() error {
	err := seg.sync()
	if err != nil {
		return err
	}

	err = seg.fd.Close()
	if err != nil {
		e := consts.NewCloseFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	// note：避免内存泄漏
	seg.blockBuffer = nil
	seg.blockPosition = nil
	return nil
}

// write 写日志到数据文件中
// 需要外部保证线程安全
func (seg *segment) write(data []byte) error {
	lengthOfBlock := int64(len(data) + headerSize)
	lengthOfBuffer := int64(len(seg.blockBuffer))
	if lengthOfBlock+lengthOfBuffer > seg.maxSize {
		return consts.NewSegmentFullErr()
	}
	nextBlockIdx := seg.getLastBlockIdx() + 1
	if nextBlockIdx >= getMaxBlockCapacityInWAL() {
		return consts.NewReachBlockIdxLimitErr()
	}
	seg.blockPosition = append(seg.blockPosition, &position{
		start: lengthOfBuffer,
		end:   lengthOfBuffer + lengthOfBlock,
	})
	seg.blockBuffer = append(seg.blockBuffer, buildBinary(nextBlockIdx, data)...)
	seg.lastBlockIdx = &nextBlockIdx
	return nil
}

// sync 持久化数据到磁盘
// 需要外部保证线程安全
func (seg *segment) sync() error {
	var written int64
	lenToWrite := int64(len(seg.blockBuffer)) - seg.blockBufferSyncIdx
	// fast-through
	if lenToWrite == 0 {
		return nil
	}

	dir, base := filepath.Dir(seg.path), filepath.Base(seg.path)
	tempFile, err := os.CreateTemp(dir, base)
	if err != nil {
		e := consts.NewCreateTempFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	_, err = io.Copy(tempFile, seg.fd)
	if err != nil {
		e := consts.NewCopyFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	for {
		if written == lenToWrite {
			break
		}
		n, err := tempFile.Write(seg.blockBuffer[seg.blockBufferSyncIdx:])
		if err != nil {
			e := consts.NewWriteFileErr().WithErr(err)
			logs.Error(e.Error())
			return e
		}

		written += int64(n)
	}

	seg.blockBufferSyncIdx = int64(len(seg.blockBuffer))
	err = tempFile.Sync()
	if err != nil {
		e := consts.NewSyncFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	err = seg.fd.Close()
	if err != nil {
		e := consts.NewCloseFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	err = os.Remove(seg.path)
	if err != nil {
		e := consts.NewRemoveFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	err = os.Rename(filepath.Join(dir, tempFile.Name()), seg.path)
	if err != nil {
		e := consts.NewRenameFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	seg.fd = tempFile
	return nil
}

// read 读取
// 需要外部保证线程安全
func (seg *segment) read(idx int64) ([]byte, error) {
	lengthOfBlockPosition := len(seg.blockPosition)
	targetIdx := sort.Search(lengthOfBlockPosition, func(i int) bool {
		return int64(i) >= idx
	})

	if targetIdx < lengthOfBlockPosition && int64(targetIdx) == idx {
		// bugfix: 读到的内容没有去掉header
		pos := seg.blockPosition[targetIdx]
		return seg.blockBuffer[pos.start+headerSize : pos.end], nil
	}

	e := consts.NewNotFoundErr()
	logs.Error(e.Error())
	return nil, e
}

// truncate 截断 segment 文件中的指定范围数据
// 如果执行成功会截断 segment 文件中 [firstBlockIdx, idx] 范围数据
// 如果idx超过 segment 文件容纳的block数量，该文件会被截断成空文件
// 需要外部保证线程安全
func (seg *segment) truncate(idx int64) (bool, error) {
	var empty bool
	firstBlockIdxAfterTruncate := (idx + 1) % seg.getFirstBlockIdx()
	if idx < seg.getFirstBlockIdx() || firstBlockIdxAfterTruncate > seg.getLastBlockIdx() {
		seg.firstBlockIdx = utils.Int64Ptr(0)
		seg.blockPosition = make([]*position, 0, consts.KB)
		seg.blockBuffer = make([]byte, 0, seg.maxSize)
		seg.blockBufferSyncIdx = 0
		empty = true
	} else {
		seg.firstBlockIdx = utils.Int64Ptr(firstBlockIdxAfterTruncate)
		seg.blockPosition = seg.blockPosition[firstBlockIdxAfterTruncate:]
		seg.blockBuffer = seg.blockBuffer[seg.blockPosition[0].start:] // 前面的判断保证这里取seg.blockPosition[0]不会有问题
		seg.blockBufferSyncIdx = int64(len(seg.blockBuffer))
	}

	// 清空文件内容
	err := seg.fd.Truncate(0)
	if err != nil {
		e := consts.NewTruncateFileErr().WithErr(err)
		logs.Error(e.Error())
		return false, e
	}

	// 将截断后的内容刷盘到文件中
	err = seg.sync()
	if err != nil {
		return false, err
	}

	// 由于segment文件由起始blockId命名，因此需要重命名
	oldPath := seg.path
	seg.path = filepath.Join(filepath.Dir(seg.path), blockIdToBase(seg.getFirstBlockIdx(), seg.hasSuffix))
	err = os.Rename(oldPath, seg.path)
	if err != nil {
		e := consts.NewRenameFileErr().WithErr(err)
		logs.Error(e.Error())
		return false, e
	}

	return empty, nil
}

// rename 重命名 segment 文件
// 需要外部保证线程安全
func (seg *segment) rename() error {
	oldPath := seg.path
	if seg.hasSuffix {
		seg.path, _ = strings.CutSuffix(seg.path, suffix)
	} else {
		seg.path += suffix
	}

	err := os.Rename(oldPath, seg.path)
	if err != nil {
		e := consts.NewRenameFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	return nil
}

// blockIdToBase 起始blockIdx转文件名
// setSuffix 设置为true时会在文件名末尾追加.active后缀
func blockIdToBase(blockId int64, setSuffix bool) string {
	base := fmt.Sprintf(getBaseFormat(), blockId)
	if setSuffix {
		base += suffix
	}
	return base
}

// baseToBlockId 文件名转起始blockIdx
// 额外返回文件名中是否包含.active后缀
func baseToBlockId(base string) (int64, bool, error) {
	blockIdStr, hasSuffix := strings.CutSuffix(base, suffix)
	firstBlockIdOfSegment, err := strconv.ParseInt(blockIdStr, 10, 64)
	if err != nil {
		e := consts.NewParseIntErr().WithErr(err)
		logs.Error(e.Error())
		return 0, false, e
	}
	return firstBlockIdOfSegment, hasSuffix, nil
}

// buildBinary 日志数据 -> 格式化二进制数据
// 存储在文件中的block结构：
// | length 8字节 | blockid 8字节 | checksum 16字节 | payload x字节 |
func buildBinary(blockId int64, data []byte) []byte {
	length := int64(len(data))
	// prof: 避免buf重分配
	buf := make([]byte, headerSize, headerSize+length)
	binary.PutVarint(buf[:headerBlockIdOffset], length)
	binary.PutVarint(buf[headerBlockIdOffset:headerSummaryOffset], blockId)
	var dataAndHeader []byte
	dataAndHeader = append(dataAndHeader, data...)
	dataAndHeader = append(dataAndHeader, buf[:headerSummaryOffset]...)
	checksum := md5.Sum(dataAndHeader)
	for i := 0; i < len(checksum); i++ {
		buf[headerSummaryOffset+i] = checksum[i]
	}
	buf = append(buf, data...)
	return buf
}

// loadBinary 从文件装载格式化二进制数据
func loadBinary(raw []byte) ([]*position, []byte, error) {
	var start int64 = 0
	fileSize := int64(len(raw))
	// prof: 粗拍一个cap，避免小数据段导致的频繁重分配问题
	bps := make([]*position, 0, consts.KB)
	bbf := make([]byte, 0, fileSize)
	for {
		if start == fileSize {
			break
		}

		block, offset, _, err := parseBinary(raw[start:])
		if err != nil {
			return nil, nil, err
		}

		bps = append(bps, &position{
			start: start,
			end:   start + offset,
		})
		start += offset
		bbf = append(bbf, block...)
	}

	return bps, bbf, nil
}

// parseBinary 解析单个格式化二进制数据 -> 日志数据
func parseBinary(raw []byte) ([]byte, int64, int64, error) {
	rawSize := int64(len(raw))
	// note: 先校验headerSize是不是比raw的长度大，校验通过后
	// 再检查blockSize是不是比raw的长度大，最后校验读取到的checksum
	// 是否和计算出的checksum一致（验证数据完整性）
	if rawSize < headerSize {
		e := consts.NewCorruptErr()
		logs.Error(e.Error())
		return nil, 0, 0, e
	}
	length, _ := binary.Varint(raw[:headerBlockIdOffset])
	blockId, _ := binary.Varint(raw[headerBlockIdOffset:headerSummaryOffset])
	checksum := raw[headerSummaryOffset:headerDataOffset]
	blockSize := headerDataOffset + length
	if rawSize < blockSize {
		e := consts.NewCorruptErr()
		logs.Error(e.Error())
		return nil, 0, 0, e
	}
	var dataAndLength []byte
	dataAndLength = append(dataAndLength, raw[headerDataOffset:blockSize]...)
	dataAndLength = append(dataAndLength, raw[:headerSummaryOffset]...)
	current := md5.Sum(dataAndLength)
	for i := 0; i < len(checksum); i++ {
		if checksum[i] != current[i] {
			e := consts.NewCorruptErr()
			logs.Error(e.Error())
			return nil, 0, 0, e
		}
	}
	return raw[:blockSize], blockSize, blockId, nil
}
