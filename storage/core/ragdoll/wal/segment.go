package wal

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/Trinoooo/eggie_kv/errs"
	"github.com/Trinoooo/eggie_kv/storage/core/ragdoll/logs"
	"github.com/Trinoooo/eggie_kv/utils"
	"go.uber.org/zap"
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
	fd   *os.File // fd segment 文件描述符
	path string   // path segment 文件路径
	// startBlockIdx segment 中的起始 blockIdx，和 firstBlockIdx 的区别是前者指代 segment 文件的起始边界
	// 边界存在不意味着第一条记录存在，后者指代 segment 文件中第一个block的blockIdx
	startBlockIdx *int64
	firstBlockIdx int64 // firstBlockIdx segment 中的起始 blockIdx
	lastBlockIdx  int64 // lastBlockIdx segment 中最后的 blockIdx
	// bbuf segment 中存储的数据内容
	// segment 存储的最大容量取决于外部，存储结构如下：
	// | block #1 | block #2 | 0000 |
	// 当文件剩余容量不足以再写下一个完整 block 时
	// 文件末尾剩余内容不再使用，保证文件开头是一个完整的 block
	bbuf        []byte
	bpos        []*position // bpos 指示 bbuf 中每个block的位置
	bbufSyncIdx int64       // bbufSyncIdx 下一个要刷盘的 bbuf 偏移量

	maxSize   int64 // maxSize segment 文件最大体积
	hasSuffix bool  // hasSuffix segment 文件路径中是否包含.active后缀
}

func newSegment(path string, maxSize int64) *segment {
	seg := &segment{}
	seg.path = path
	seg.maxSize = maxSize
	seg.firstBlockIdx = -1
	seg.lastBlockIdx = -1
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
		e := errs.NewOpenFileErr()
		logs.Error(e.Error())
		return e
	}

	seg.fd = fd
	all, err := io.ReadAll(seg.fd)
	if err != nil {
		e := errs.NewReadFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	bps, bbf, err := loadBinary(all)
	if err != nil {
		return err
	}

	seg.bbuf = bbf
	seg.bpos = bps
	lengthOfBpos := int64(len(seg.bpos))
	seg.bbufSyncIdx = int64(len(seg.bbuf))
	startBlockIdx, hasSuffix, err := baseToBlockId(filepath.Base(seg.path))
	if err != nil {
		return err
	}
	seg.startBlockIdx = &startBlockIdx
	if lengthOfBpos > 0 {
		seg.firstBlockIdx = startBlockIdx
		seg.lastBlockIdx = seg.firstBlockIdx + lengthOfBpos - 1
	}
	seg.hasSuffix = hasSuffix
	return nil
}

func (seg *segment) getStartBlockIdx() int64 {
	if seg.startBlockIdx != nil {
		return *seg.startBlockIdx
	}
	blockId, _, _ := baseToBlockId(filepath.Base(seg.path))
	return blockId
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
		e := errs.NewCloseFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	// note：避免内存泄漏
	seg.bbuf = nil
	seg.bpos = nil
	return nil
}

// write 写日志到数据文件中
// 需要外部保证线程安全
func (seg *segment) write(data []byte) error {
	lengthOfBlock := int64(len(data) + headerSize)
	lengthOfBbuf := int64(len(seg.bbuf))
	if lengthOfBlock+lengthOfBbuf > seg.maxSize {
		// note: 这里不打日志是因为可能是稳态错误，在外层判断再打日志
		return errs.NewSegmentFullErr()
	}
	nextBlockIdx := seg.lastBlockIdx + 1
	if nextBlockIdx >= getMaxBlockCapacityInWAL() {
		// note: 这里不打日志是因为可能是稳态错误，在外层判断再打日志
		return errs.NewReachBlockIdxLimitErr()
	}
	seg.bpos = append(seg.bpos, &position{
		start: lengthOfBbuf,
		end:   lengthOfBbuf + lengthOfBlock,
	})
	seg.bbuf = append(seg.bbuf, buildBinary(nextBlockIdx, data)...)
	seg.lastBlockIdx = nextBlockIdx
	return nil
}

// sync 持久化数据到磁盘
// 需要外部保证线程安全
func (seg *segment) sync() error {
	var written int64
	lenToWrite := int64(len(seg.bbuf)) - seg.bbufSyncIdx
	// fast-through
	if lenToWrite == 0 {
		return nil
	}

	dir, base := filepath.Dir(seg.path), filepath.Base(seg.path)
	tempFile, err := os.CreateTemp(dir, base)
	if err != nil {
		e := errs.NewCreateTempFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	// bugfix: 不调整文件偏移量在 io.Copy 时会有问题
	_, err = seg.fd.Seek(0, io.SeekStart)
	if err != nil {
		e := errs.NewSeekFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	_, err = io.Copy(tempFile, seg.fd)
	if err != nil {
		e := errs.NewCopyFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	for {
		if written == lenToWrite {
			break
		}
		n, err := tempFile.Write(seg.bbuf[seg.bbufSyncIdx+written:])
		if err != nil {
			e := errs.NewWriteFileErr().WithErr(err)
			logs.Error(e.Error())
			return e
		}

		written += int64(n)
	}

	seg.bbufSyncIdx = int64(len(seg.bbuf))
	err = tempFile.Sync()
	if err != nil {
		e := errs.NewSyncFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	err = seg.fd.Close()
	if err != nil {
		e := errs.NewCloseFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	err = os.Remove(seg.path)
	if err != nil {
		e := errs.NewRemoveFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	err = os.Rename(tempFile.Name(), seg.path)
	if err != nil {
		e := errs.NewRenameFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	seg.fd = tempFile
	return nil
}

// read 读取
// 需要外部保证线程安全
func (seg *segment) read(idx int64) ([]byte, error) {
	lengthOfBpos := len(seg.bpos)
	offset := idx - seg.firstBlockIdx
	targetIdx := sort.Search(lengthOfBpos, func(i int) bool {
		return int64(i) >= offset
	})

	if targetIdx < lengthOfBpos && int64(targetIdx) == offset {
		// bugfix: 读到的内容没有去掉header
		pos := seg.bpos[targetIdx]
		return seg.bbuf[pos.start+headerSize : pos.end], nil
	}

	e := errs.NewNotFoundErr()
	logs.Error(e.Error(), zap.String(consts.LogFieldParams, "idx,offset"), zap.Any(consts.LogFieldValue, []interface{}{idx, offset}))
	return nil, e
}

// truncate 截断 segment 文件中的指定范围数据
// 如果执行成功会截断 segment 文件中 [firstBlockIdx, idx] 范围数据
// 如果idx超过 segment 文件容纳的block数量，该文件会被截断成空文件
// 需要外部保证线程安全
func (seg *segment) truncate(idx int64) (bool, error) {
	var empty bool
	if idx < seg.firstBlockIdx || idx > seg.lastBlockIdx {
		seg.bpos = make([]*position, 0, consts.KB)
		seg.bbuf = make([]byte, 0, seg.maxSize)
		seg.bbufSyncIdx = 0
		empty = true
	} else {
		firstBlockIdxAfterTruncate := (idx + 1) - seg.getStartBlockIdx()
		seg.firstBlockIdx = idx
		seg.bpos = seg.bpos[firstBlockIdxAfterTruncate:]
		firstByteAfterTruncate := seg.bpos[0].start
		seg.bbuf = seg.bbuf[firstByteAfterTruncate:] // 前面的判断保证这里取seg.bpos[0]不会有问题
		// 下面会完全截断，所以从0开始同步buf内容
		seg.bbufSyncIdx = 0
		// 由于segment文件由起始blockId命名，因此需要重命名
		oldPath := seg.path
		seg.path = filepath.Join(filepath.Dir(seg.path), blockIdToBase(seg.firstBlockIdx, seg.hasSuffix))
		err := os.Rename(oldPath, seg.path)
		if err != nil {
			e := errs.NewRenameFileErr().WithErr(err)
			logs.Error(e.Error())
			return false, e
		}
	}

	// 清空文件内容
	err := seg.fd.Truncate(0)
	if err != nil {
		e := errs.NewTruncateFileErr().WithErr(err)
		logs.Error(e.Error())
		return false, e
	}

	err = seg.sync()
	if err != nil {
		return false, err
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
		e := errs.NewRenameFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	return nil
}

func (seg *segment) size() (int64, error) {
	stat, err := os.Stat(seg.path)
	if err != nil {
		e := errs.NewFileStatErr().WithErr(err)
		logs.Error(e.Error())
		return 0, e
	}

	return stat.Size(), nil
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
		e := errs.NewParseIntErr().WithErr(err)
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
		e := errs.NewCorruptErr()
		logs.Error(e.Error())
		return nil, 0, 0, e
	}
	length, _ := binary.Varint(raw[:headerBlockIdOffset])
	blockId, _ := binary.Varint(raw[headerBlockIdOffset:headerSummaryOffset])
	checksum := raw[headerSummaryOffset:headerDataOffset]
	blockSize := headerDataOffset + length
	if rawSize < blockSize {
		e := errs.NewCorruptErr()
		logs.Error(e.Error())
		return nil, 0, 0, e
	}
	var dataAndLength []byte
	dataAndLength = append(dataAndLength, raw[headerDataOffset:blockSize]...)
	dataAndLength = append(dataAndLength, raw[:headerSummaryOffset]...)
	current := md5.Sum(dataAndLength)
	for i := 0; i < len(checksum); i++ {
		if checksum[i] != current[i] {
			e := errs.NewCorruptErr()
			logs.Error(e.Error())
			return nil, 0, 0, e
		}
	}
	return raw[:blockSize], blockSize, blockId, nil
}
