package wal

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/Trinoooo/eggie_kv/utils"
	"io"
	"os"
	"path/filepath"
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

func getBaseFormat() string {
	return utils.GetValueOnEnv("%010d", "%08d").(string)
}

type bpos struct {
	start int64
	end   int64
}

// TODO: 补充corrupt处理
type segment struct {
	fd           *os.File // fd segment文件描述符
	path         string   // path segment文件路径
	startBlockId *int64   // startBlockId segment起始blockId
	// bbuf segment中存储的数据内容
	// segment存储的最大容量取决于外部，存储结构如下：
	// | block #1 | block #2 | 0000 |
	// 当文件剩余容量不足以再写下一个完整block时
	// 文件末尾剩余内容不再使用，保证文件开头是一个完整的block
	bbuf []byte
	// bpos 指示 bbuf 中每个block的位置
	// start 表示起始偏移量
	// end 表示结束偏移量
	bpos           []*bpos
	nextByteToSync int64 // nextByteToSync 下一个要同步的字节偏移量
	maxSize        int64 // maxSize segment文件最大体积
	hasSuffix      bool  // hasSuffix segment文件路径中是否包含.active后缀
	nextBlockId    int64 // nextBlockId 下一个写入block的idx，口径是segment内的相对偏移量
}

func newSegment(path string, maxSize int64) *segment {
	seg := &segment{}
	seg.path = path
	seg.maxSize = maxSize
	return seg
}

func (seg *segment) getStartBlockId() int64 {
	// todo: startBlockId 用指针还是非法值
	if seg.startBlockId != nil {
		return *seg.startBlockId
	}

	blockId, _, _ := baseToBlockId(filepath.Base(seg.path))
	return blockId
}

func (seg *segment) open(perm os.FileMode) error {
	// 如果是新segment文件，则创建一个新文件
	// 否则以追加模式进行操作
	// 指定操作包含读和写
	fd, err := utils.CheckAndCreateFile(seg.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, perm)
	if err != nil {
		return consts.OpenFileErr
	}

	seg.fd = fd
	all, err := io.ReadAll(seg.fd)
	if err != nil {
		return err
	}

	bps, bbf, lastBlockId, err := loadBinary(all)
	if err != nil {
		return err
	}

	seg.bbuf = bbf
	seg.bpos = bps
	seg.nextByteToSync = int64(len(seg.bbuf))
	seg.nextBlockId = lastBlockId + 1
	sbid, hasSuffix, err := baseToBlockId(filepath.Base(seg.path))
	if err != nil {
		return err
	}
	seg.startBlockId = &sbid
	seg.hasSuffix = hasSuffix
	return nil
}

func (seg *segment) close() error {
	err := seg.sync()
	if err != nil {
		return err
	}

	err = seg.fd.Close()
	if err != nil {
		return err
	}

	// note：避免内存泄漏
	seg.bbuf = nil
	seg.bpos = nil
	return nil
}

// write 写日志到数据文件中。
func (seg *segment) write(data []byte) error {
	// todo: 优化命名
	lb := int64(len(data) + headerSize)
	lbbf := int64(len(seg.bbuf))
	if lb+lbbf > seg.maxSize {
		return consts.SegmentFullErr
	}

	seg.bpos = append(seg.bpos, &bpos{
		start: lbbf,
		end:   lbbf + lb,
	})
	seg.nextBlockId++
	seg.bbuf = append(seg.bbuf, buildBinary(seg.nextBlockId, data)...)
	return nil
}

// sync 持久化数据到磁盘
// todo: 文件完整性，问下gpt
func (seg *segment) sync() error {
	var written int64
	lenToWrite := int64(len(seg.bbuf)) - seg.nextByteToSync

	for {
		if written == lenToWrite {
			break
		}
		n, err := seg.fd.Write(seg.bbuf[seg.nextByteToSync:])
		if err != nil {
			return err
		}

		written += int64(n)
	}

	// fast-through
	if written == 0 {
		return nil
	}

	seg.nextByteToSync = int64(len(seg.bbuf))
	err := seg.fd.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (seg *segment) read(idx int64) ([]byte, error) {
	// todo： 可以二分优化
	for i, pos := range seg.bpos {
		if *seg.startBlockId+int64(i) == idx {
			// bugfix: 读到的内容没有去掉header
			return seg.bbuf[pos.start+headerSize : pos.end], nil
		}
	}

	return nil, consts.NotFoundErr
}

func (seg *segment) truncate(idx int64) (int64, error) {
	// note：外部使用时可能会传超过当前segment容量的idx
	if idx > seg.nextBlockId+seg.getStartBlockId()-1 {
		seg.bbuf = nil
		seg.bpos = nil
		seg.nextByteToSync = 0
		err := os.Remove(seg.path)
		if err != nil {
			return 0, err
		}

		return 0, nil
	}

	posOffset := idx - *seg.startBlockId
	*seg.startBlockId = idx
	firstPosAfterTruncate := seg.bpos[posOffset]
	seg.bpos = seg.bpos[posOffset:]
	seg.bbuf = seg.bbuf[firstPosAfterTruncate.start:]

	// fd.Truncate 内部调用Ftruncate
	// 保留截断后的文件大小
	lbuf := int64(len(seg.bbuf))
	err := seg.fd.Truncate(0)
	if err != nil {
		return 0, err
	}
	seg.nextByteToSync = 0
	oldPath := seg.path
	seg.path = filepath.Join(filepath.Dir(seg.path), blockIdToBase(idx, seg.hasSuffix))
	err = os.Rename(oldPath, seg.path)
	if err != nil {
		return 0, err
	}
	return lbuf, nil
}

func (seg *segment) rename() error {
	oldPath := seg.path
	if seg.hasSuffix {
		seg.path, _ = strings.CutSuffix(seg.path, suffix)
	} else {
		seg.path += suffix
	}

	err := os.Rename(oldPath, seg.path)
	if err != nil {
		return err
	}

	return nil
}

func blockIdToBase(blockId int64, setSuffix bool) string {
	base := fmt.Sprintf(getBaseFormat(), blockId)
	if setSuffix {
		base += suffix
	}
	return base
}

func baseToBlockId(base string) (int64, bool, error) {
	blockIdStr, hasSuffix := strings.CutSuffix(base, suffix)
	var firstBlockIdOfSegment int64
	// todo：strconv是不是可以 str -> int
	_, err := fmt.Sscanf(blockIdStr, getBaseFormat(), &firstBlockIdOfSegment)
	if err != nil {
		return 0, false, err
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
func loadBinary(raw []byte) ([]*bpos, []byte, int64, error) {
	var start int64 = 0
	// prof: 粗拍一个cap，避免小数据段导致的频繁重分配问题
	bps := make([]*bpos, 0, consts.KB)
	// todo：prof stat可以拿到文件大小
	bbf := make([]byte, 0, consts.MB)
	var lastBlockId int64
	for {
		if int64(len(raw)) == start {
			break
		}

		block, offset, blockId, err := parseBinary(raw[start:])
		if err != nil {
			return nil, nil, 0, err
		}

		bps = append(bps, &bpos{
			start: start,
			end:   start + offset,
		})
		start += offset
		bbf = append(bbf, block...)
		lastBlockId = blockId
	}

	return bps, bbf, lastBlockId, nil
}

// parseBinary 解析单个格式化二进制数据 -> 日志数据
func parseBinary(raw []byte) ([]byte, int64, int64, error) {
	// length + chechsum
	if len(raw) < headerSize {
		return nil, 0, 0, consts.CorruptErr
	}
	length, _ := binary.Varint(raw[:headerBlockIdOffset])
	blockId, _ := binary.Varint(raw[headerBlockIdOffset:headerSummaryOffset])
	checksum := raw[headerSummaryOffset:headerDataOffset]
	// todo: 校验end < raw的长度
	end := headerDataOffset + length
	var dataAndLength []byte
	dataAndLength = append(dataAndLength, raw[headerDataOffset:end]...)
	dataAndLength = append(dataAndLength, raw[:headerSummaryOffset]...)
	current := md5.Sum(dataAndLength)
	for i := 0; i < len(checksum); i++ {
		if checksum[i] != current[i] {
			return nil, 0, 0, consts.CorruptErr
		}
	}
	return raw[:end], end, blockId, nil
}
