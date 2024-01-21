package wal

import (
	"bufio"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/Trinoooo/eggie_kv/utils"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
)

type bpos struct {
	start int64
	end   int64
}

type segment struct {
	fd           *os.File
	bbuf         []byte
	bpos         []*bpos
	path         string
	startBlockId int64
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
	fd, err := utils.CheckAndCreateFile(seg.path, os.O_RDWR|os.O_CREATE, perm)
	if err != nil {
		log.Fatalln(err)
		return consts.OpenFileErr
	}

	seg.fd = fd
	all, err := io.ReadAll(seg.fd)
	if err != nil {
		return err
	}

	_, err = seg.fd.Seek(0, 0)
	if err != nil {
		return err
	}

	bps, bbf, err := loadBinary(all)
	if err != nil {
		return err
	}

	seg.bbuf = bbf
	seg.bpos = bps
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
	writer := bufio.NewWriter(seg.fd)
	for _, pos := range seg.bpos {
		_, err := writer.Write(seg.bbuf[pos.start:pos.end])
		if err != nil {
			return err
		}
	}
	err := writer.Flush()
	if err != nil {
		return err
	}

	err = seg.fd.Sync()
	if err != nil {
		return err
	}

	_, err = seg.fd.Seek(0, 0)
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
	seg.bpos = seg.bpos[idx-seg.startBlockId:]
	seg.bbuf = seg.bbuf[seg.bpos[0].start:]
	err := seg.fd.Truncate(0)
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

// 存储在文件中的block结构：
// |	length 8字节	|	checksum 16字节	|
// |			payload x字节			|
// buildBinary 日志数据 -> 格式化二进制数据
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
	bps := []*bpos{}
	bbf := []byte{}
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
	data := raw[24:end]
	cc := md5.Sum(append(data, raw[:8]...))
	for i := 0; i < len(checksum); i++ {
		if checksum[i] != cc[i] {
			return nil, 0, consts.CorruptErr
		}
	}
	return raw[:end], end, nil
}
