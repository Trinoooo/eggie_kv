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
)

type bpos struct {
	start int64
	end   int64
}

// TODO: 补充corrupt处理
type segment struct {
	fd           *os.File // fd segment文件描述符
	path         string   // path segment文件路径
	startBlockId int64    // startBlockId segment起始blockId
	// bbuf segment中存储的数据内容
	// segment存储的最大容量取决于外部，存储结构如下：
	// | block #1 | block #2 | 0000 |
	// 当文件剩余容量不足以再写下一个完整block时
	// 文件末尾剩余内容不再使用，保证文件开头是一个完整的block
	bbuf []byte
	// bpos 指示bbuf中每个block的位置
	// start 表示起始偏移量
	// end 表示结束偏移量
	bpos           []*bpos
	nextByteToSync int64 // nextByteToSync 下一个要同步的字节偏移量
}

func newSegment(path string) *segment {
	seg := &segment{}
	seg.path = path
	return seg
}

func (seg *segment) getStartBlockId() int64 {
	blockId, _ := baseToBlockId(filepath.Base(seg.path))
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

	bps, bbf, err := loadBinary(all)
	if err != nil {
		return err
	}

	seg.bbuf = bbf
	seg.bpos = bps
	seg.nextByteToSync = int64(len(seg.bbuf))
	seg.startBlockId, err = baseToBlockId(filepath.Base(seg.path))
	if err != nil {
		return err
	}
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

	seg.bbuf = nil
	seg.bpos = nil
	return nil
}

// write 写日志到数据文件中。
// segment数据文件不知道自己的存储上限，由外层使用者控制
func (seg *segment) write(data []byte) error {
	b := buildBinary(data)
	lbbf := int64(len(seg.bbuf))
	seg.bpos = append(seg.bpos, &bpos{
		start: lbbf,
		end:   lbbf + int64(len(b)),
	})

	seg.bbuf = append(seg.bbuf, b...)
	return nil
}

// sync 持久化数据到磁盘
// todo: 文件完整性
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
	seg.nextByteToSync = int64(len(seg.bbuf))

	err := seg.fd.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (seg *segment) size() int64 {
	return int64(len(seg.bbuf))
}

func (seg *segment) read(idx int64) ([]byte, error) {
	for inner, pos := range seg.bpos {
		if seg.startBlockId+int64(inner) == idx {
			return seg.bbuf[pos.start:pos.end], nil
		}
	}

	return nil, consts.NotFoundErr
}

func (seg *segment) truncate(idx int64) error {
	posOffset := idx - seg.startBlockId
	firstPosAfterTruncate := seg.bpos[posOffset]
	seg.bpos = seg.bpos[posOffset:]
	seg.bbuf = seg.bbuf[firstPosAfterTruncate.start:]
	// fd.Truncate 内部调用Ftruncate
	// 是相对当前文件offset改变文件大小
	_, err := seg.fd.Seek(firstPosAfterTruncate.start, 0)
	if err != nil {
		return err
	}

	// 保留截断后的文件大小
	lbuf := int64(len(seg.bbuf))
	err = seg.fd.Truncate(lbuf)
	if err != nil {
		return err
	}
	seg.nextByteToSync = lbuf
	oldPath := seg.path
	seg.path = filepath.Join(filepath.Dir(seg.path), blockIdToBase(idx))
	err = os.Rename(oldPath, seg.path)
	if err != nil {
		return err
	}
	return nil
}

func blockIdToBase(blockId int64) string {
	return fmt.Sprintf("%010d", blockId)
}

func baseToBlockId(base string) (int64, error) {
	var firstBlockIdOfSegment int64
	_, err := fmt.Sscanf(base, "%010d", &firstBlockIdOfSegment)
	if err != nil {
		return 0, err
	}
	return firstBlockIdOfSegment, nil
}

// buildBinary 日志数据 -> 格式化二进制数据
// 存储在文件中的block结构：
// |	length 8字节		|	checksum 16字节	|
// |				payload x字节			|
func buildBinary(data []byte) []byte {
	length := int64(len(data))
	buf := make([]byte, 24)
	binary.PutVarint(buf[:8], length)
	checksum := md5.Sum(append(data, buf[:8]...))
	for i := 0; i < len(checksum); i++ {
		buf[8+i] = checksum[i]
	}
	buf = append(buf, data...)
	return buf
}

// loadBinary 从文件装载格式化二进制数据
func loadBinary(raw []byte) ([]*bpos, []byte, error) {
	var start int64 = 0
	var bps []*bpos
	var bbf []byte
	for {
		if int64(len(raw)) == start {
			break
		}

		block, offset, err := parseBinary(raw[start:])
		if err != nil {
			return nil, nil, err
		}

		bps = append(bps, &bpos{
			start: start,
			end:   start + offset,
		})
		start += offset
		bbf = append(bbf, block...)
	}

	return bps, bbf, nil
}

// parseBinary 解析单个格式化二进制数据 -> 日志数据
func parseBinary(raw []byte) ([]byte, int64, error) {
	// length + chechsum
	if len(raw) < 24 {
		return nil, 0, consts.CorruptErr
	}

	length, _ := binary.Varint(raw[:8])
	checksum := raw[8:24]
	end := 24 + length
	data := make([]byte, end-24)
	copy(data, raw[24:end])
	cc := md5.Sum(append(data, raw[:8]...))
	for i := 0; i < len(checksum); i++ {
		if checksum[i] != cc[i] {
			return nil, 0, consts.CorruptErr
		}
	}
	return raw[:end], end, nil
}
