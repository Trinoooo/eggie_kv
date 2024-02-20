package wal

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/Trinoooo/eggie_kv/errs"
	"github.com/Trinoooo/eggie_kv/storage/core/ragdoll/logs"
	"github.com/Trinoooo/eggie_kv/utils"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// segment 日志数据的文件
// 所有字段外部都不应该外部直接访问
// 出于性能考虑所有方法均不保证并发安全
type segment struct {
	fd   *os.File // fd 数据文件描述符
	path string   // path 数据文件路径
	// startBlockIdx 数据文件的起始 block 索引，和 firstBlockIdx 的区别是前者指代数据文件的起始边界
	// 边界存在不意味着第一条记录存在，后者指代数据文件中第一个 block 的索引
	startBlockIdx *int64
	firstBlockIdx int64 // firstBlockIdx 数据文件维护的起始 block 索引
	lastBlockIdx  int64 // lastBlockIdx 数据文件维护的最后 block 索引
	// bbuf 数据文件活跃状态下在内存中维护日志数据，存储结构如下：
	// | block #1 | block #2 | block #3 | block #4 ｜ 0000 |
	// 当文件剩余容量不足以再写下一个完整 block 时文件末尾剩余内容不再使用，
	// 保证文件开头是一个完整的 block
	bbuf        []byte
	bpos        []*position // bpos 指示 bbuf 中每个block的位置
	bbufSyncIdx int64       // bbufSyncIdx 下一个要同步的 bbuf 数据下标
	maxSize     int64       // maxSize 数据文件最大体积
	hasSuffix   bool        // hasSuffix 数据文件路径中是否包含.active后缀，用于标识最后活跃的数据文件
	opened      bool        // opened 数据文件是否已经打开
}

// position 日志位置信息
type position struct {
	start int64 // start 在bbuf中的起始偏移量
	end   int64 // end 在bbuf中结束偏移量
}

// newSegment 初始化数据文件
func newSegment(path string, maxSize int64) (*segment, error) {
	seg := &segment{}
	seg.path = path
	seg.maxSize = maxSize
	seg.firstBlockIdx = -1
	seg.lastBlockIdx = -1
	startBlockIdx, hasSuffix, err := baseToBlockIdx(filepath.Base(seg.path))
	if err != nil {
		return nil, err
	}
	seg.startBlockIdx = &startBlockIdx
	seg.hasSuffix = hasSuffix
	return seg, nil
}

// open 打开数据文件
func (seg *segment) open(perm os.FileMode) error {
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

	bps, bbf, err := loadBlocks(all)
	if err != nil {
		return err
	}

	seg.bbuf = bbf
	seg.bpos = bps
	seg.bbufSyncIdx = int64(len(seg.bbuf))
	if lengthOfBpos := int64(len(seg.bpos)); lengthOfBpos > 0 {
		seg.firstBlockIdx = seg.getStartBlockIdx()
		seg.lastBlockIdx = seg.firstBlockIdx + lengthOfBpos - 1
	}
	seg.opened = true
	return nil
}

// isOpened 判断数据文件是否处于打开状态
func (seg *segment) isOpened() bool {
	return seg.opened
}

// getStartBlockIdx 获取 segment 文件的起始 blockIdx
func (seg *segment) getStartBlockIdx() int64 {
	if seg.startBlockIdx != nil {
		return *seg.startBlockIdx
	}
	blockId, _, _ := baseToBlockIdx(filepath.Base(seg.path))
	return blockId
}

// close 关闭 segment 文件
func (seg *segment) close() error {
	err := seg.checkState()
	if err != nil {
		return err
	}

	err = seg.sync()
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
	seg.fd = nil
	seg.bbuf = nil
	seg.bpos = nil
	seg.opened = false
	return nil
}

// write 写日志到数据文件中
func (seg *segment) write(data []byte) error {
	err := seg.checkState()
	if err != nil {
		return err
	}

	lengthOfBlock := int64(len(data) + headerSize)
	lengthOfBbuf := int64(len(seg.bbuf))
	nextBlockIdx := seg.lastBlockIdx + 1
	// bugfix：新建的/空的 segment 写入时 firstBlockIdx 和 lastBlockIdx 都没有初始化成该文件的起始 blockIdx
	if seg.firstBlockIdx == -1 {
		seg.firstBlockIdx = seg.getStartBlockIdx()
		nextBlockIdx = seg.firstBlockIdx
	}
	if lengthOfBlock+lengthOfBbuf > seg.maxSize {
		// note: 这里不打日志是因为可能是稳态错误，在外层判断再打日志
		return errs.NewSegmentFullErr()
	}
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
func (seg *segment) sync() error {
	err := seg.checkState()
	if err != nil {
		return err
	}
	lenToWrite := int64(len(seg.bbuf)) - seg.bbufSyncIdx
	// fast-through
	if lenToWrite == 0 {
		return nil
	}

	err = seg.copyOnWrite(true)
	if err != nil {
		return err
	}

	return nil
}

// read 查询 segment 文件中的指定范围数据
// 如果执行成功会截断 segment 文件中 [firstBlockIdx, idx] 范围数据
func (seg *segment) read(idx int64) (map[int64][]byte, error) {
	err := seg.checkState()
	if err != nil {
		return nil, err
	}

	blockIdxToData := make(map[int64][]byte)
	border := idx - seg.getStartBlockIdx()
	if idx < seg.firstBlockIdx || idx > seg.lastBlockIdx {
		border = int64(len(seg.bpos)) - 1
	}

	for i := int64(0); i <= border; i++ {
		data, _, blockIdx, err := parseBinary(seg.bbuf[seg.bpos[i].start:])
		if err != nil {
			return nil, err
		}
		blockIdxToData[blockIdx] = data
	}

	return blockIdxToData, nil
}

// truncate 截断 segment 文件中的指定范围数据
// 如果执行成功会截断 segment 文件中 [firstBlockIdx, idx] 范围数据
// 如果idx超过 segment 文件容纳的block数量，该文件会被截断成空文件
func (seg *segment) truncate(idx int64) (err error) {
	if idx < seg.firstBlockIdx || idx >= seg.lastBlockIdx {
		seg.bpos = make([]*position, 0, consts.KB)
		seg.bbuf = make([]byte, 0, seg.maxSize)
		seg.firstBlockIdx = -1
		seg.lastBlockIdx = -1
	} else {
		seg.firstBlockIdx = idx + 1
		seg.bpos = seg.bpos[seg.firstBlockIdx-seg.getStartBlockIdx():]
		seg.bbuf = seg.bbuf[seg.bpos[0].start:] // 前面的判断保证这里取seg.bpos[0]不会有问题
		// note: 这里 rename 可能导致不一致问题
		// 即原文件内容没有被截断，但是文件名被修改成截断后的
		oldPath := seg.path
		defer func() {
			if err != nil {
				return
			}

			err = os.Remove(oldPath)
			if err != nil {
				e := errs.NewRemoveFileErr().WithErr(err)
				logs.Error(e.Error())
				err = e
			}
		}()
		seg.path = filepath.Join(filepath.Dir(seg.path), blockIdxToBase(seg.firstBlockIdx, seg.hasSuffix))
	}
	seg.bbufSyncIdx = 0

	err = seg.copyOnWrite(false)
	if err != nil {
		return err
	}

	return nil
}

// rename 重命名 segment 文件
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

// remove 移除该数据文件
// 内部会在移除之前关闭文件
func (seg *segment) remove() error {
	err := seg.close()
	if err != nil {
		return err
	}

	err = os.Remove(seg.path)
	if err != nil {
		e := errs.NewRemoveFileErr()
		logs.Error(e.Error())
		return e
	}
	return nil
}

// size 返回数据文件中的 block 数
func (seg *segment) size() int64 {
	return int64(len(seg.bpos))
}

// checkState 检查数据文件状态
func (seg *segment) checkState() error {
	if !seg.opened {
		e := errs.NewFileClosedErr()
		logs.Error(e.Error())
		return e
	}
	return nil
}

// copyOnWrite 写时复制
func (seg *segment) copyOnWrite(copy bool) error {
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

	if copy {
		_, err = io.Copy(tempFile, seg.fd)
		if err != nil {
			e := errs.NewCopyFileErr().WithErr(err)
			logs.Error(e.Error())
			return e
		}
	}

	err = seg.fd.Close()
	if err != nil {
		e := errs.NewCloseFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	_, err = tempFile.Write(seg.bbuf[seg.bbufSyncIdx:])
	if err != nil {
		e := errs.NewWriteFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	err = tempFile.Sync()
	if err != nil {
		e := errs.NewSyncFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}

	err = os.Rename(tempFile.Name(), seg.path)
	if err != nil {
		e := errs.NewRenameFileErr().WithErr(err)
		logs.Error(e.Error())
		return e
	}
	seg.bbufSyncIdx = int64(len(seg.bbuf))
	seg.fd = tempFile
	seg.startBlockIdx = nil
	return nil
}

// blockIdxToBase 起始blockIdx转文件名
// setSuffix 设置为true时会在文件名末尾追加.active后缀
func blockIdxToBase(blockId int64, setSuffix bool) string {
	base := fmt.Sprintf(getBaseFormat(), blockId)
	if setSuffix {
		base += suffix
	}
	return base
}

// baseToBlockIdx 文件名转起始blockIdx
// 额外返回文件名中是否包含.active后缀
func baseToBlockIdx(base string) (int64, bool, error) {
	blockIdStr, hasSuffix := strings.CutSuffix(base, suffix)
	firstBlockIdOfSegment, err := strconv.ParseInt(blockIdStr, 10, 64)
	if err != nil {
		e := errs.NewParseIntErr().WithErr(err)
		logs.Error(e.Error())
		return 0, false, e
	}
	return firstBlockIdOfSegment, hasSuffix, nil
}

// buildBinary 日志数据格式化成二进制数据
// 存储在数据文件中的 block 结构：| length 8字节 | blockid 8字节 | checksum 16字节 | payload x字节 |
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

// loadBlocks 从数据文件中装载并解析二进制数据
func loadBlocks(raw []byte) ([]*position, []byte, error) {
	var start int64
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

// parseBinary 二进制数据解析成日志数据
// 返回日志内容、日志大小、blockid
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
