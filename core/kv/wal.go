package kv

import (
	"bufio"
	"crypto/md5"
	"encoding/binary"
	"github.com/Trinoooo/eggie_kv/consts"
	"github.com/Trinoooo/eggie_kv/utils"
	log "github.com/sirupsen/logrus"
	"io"
	"math"
	"os"
	"sync"
	"syscall"
	"time"
)

const (
	WalMask               = 0x010814
	SyncFrequency         = 1 * time.Second
	MaxWalFileSize        = 100 * consts.MB
	MaxWalFileSizeForTest = consts.KB

	BlockHeaderSize                = 40
	BlockHeaderWithoutCheckSumSize = 24
)

type Block struct {
	id        int64
	checkSum  [16]byte
	size      uint64
	operation consts.OperatorType
	payload   []byte
}

func NewBlock(id int64, operation consts.OperatorType, payload []byte) *Block {
	b := &Block{
		id:        id,
		size:      uint64(len(payload)),
		operation: operation,
		payload:   payload,
	}
	b.checkSum = b.summary()
	return b
}

func NewEmptyBlock() *Block {
	return NewBlock(0, consts.OperatorTypeUnknown, nil)
}

func (b *Block) UnMarshal(rawBytes []byte) (int64, error) {
	if rawByteLen := len(rawBytes); rawByteLen < BlockHeaderSize {
		log.Error("file integrity has been compromised")
		return 0, consts.FileIntegrityErr
	}

	offsetId, lengthId := 0, 8
	offsetCheckSum := 8
	offsetSize, lengthSize := 24, 8
	offsetOp, lengthOp := 32, 8
	offsetPayload, lengthPayload := 40, int(binary.BigEndian.Uint64(rawBytes[offsetSize:offsetSize+lengthSize]))
	b.id = int64(binary.BigEndian.Uint64(rawBytes[offsetId : offsetId+lengthId]))
	b.size = binary.BigEndian.Uint64(rawBytes[offsetSize : offsetSize+lengthSize])
	b.operation = consts.OperatorType(binary.BigEndian.Uint64(rawBytes[offsetOp : offsetOp+lengthOp]))
	b.payload = rawBytes[offsetPayload : offsetPayload+lengthPayload]

	sum := b.summary()
	for i := 0; i < len(sum); i++ {
		if rawBytes[offsetCheckSum+i] != sum[i] {
			log.Error("file integrity has been compromised")
			return 0, consts.FileIntegrityErr
		}
	}
	b.checkSum = sum

	return int64(offsetPayload + lengthPayload), nil
}

func (b *Block) summary() [16]byte {
	buffer := make([]byte, BlockHeaderWithoutCheckSumSize+len(b.payload))
	binary.BigEndian.PutUint64(buffer[:8], uint64(b.id))
	binary.BigEndian.PutUint64(buffer[8:16], b.size)
	binary.BigEndian.PutUint64(buffer[16:], uint64(b.operation))
	return md5.Sum(append(buffer, b.payload...))
}

func (b *Block) Marshal() []byte {
	buffer := make([]byte, BlockHeaderSize)
	binary.BigEndian.PutUint64(buffer[:8], uint64(b.id))
	for i := 0; i < len(b.checkSum); i++ {
		buffer[8+i] = b.checkSum[i]
	}
	binary.BigEndian.PutUint64(buffer[24:32], b.size)
	binary.BigEndian.PutUint64(buffer[32:], uint64(b.operation))
	buffer = append(buffer, b.payload...)
	return buffer
}

// WriteAheadLog 先行日志
// 1. 顺序写日志而不是随机写数据，提升读写磁盘效率
// 2. 保证事务原子性
type WriteAheadLog struct {
	Kv          *KV
	Blocks      []*Block
	NextBlockId int64
	mu          sync.Mutex
	ticker      *time.Ticker
}

func NewWriteAheadLog() (*WriteAheadLog, error) {
	wal := &WriteAheadLog{}

	if err := wal.init(); err != nil {
		return nil, err
	}

	go wal.checkPoint()

	return wal, nil
}

// init 读取wal文件中所有block，找到id最大的完整block
func (wal *WriteAheadLog) init() error {
	fd, err := utils.CheckAndCreateFile(consts.BaseDir+"/wal_cycle_log", syscall.O_APPEND|syscall.O_CREAT|syscall.O_RDWR, 0660)
	defer utils.HandlePanic(func() {
		if err = fd.Close(); err != nil {
			log.Fatalln(err)
		}
	})
	if err != nil {
		log.Errorln(err)
		return consts.OpenFileErr
	}

	bytes, err := io.ReadAll(fd)
	if err != nil {
		log.Errorln(err)
		return consts.ReadFileErr
	}

	// 初始化block
	var maxBlockId int64
	for {
		if len(bytes) == 0 {
			break
		}

		block := NewEmptyBlock()
		offset, err := block.UnMarshal(bytes)
		if err != nil {
			return err
		}

		maxBlockId = int64(math.Max(float64(maxBlockId), float64(block.id)))
		wal.Blocks = append(wal.Blocks, block)
		bytes = bytes[offset:]
	}
	wal.NextBlockId = maxBlockId + 1

	// 根据wal修复数据记录
	return wal.replay()
}

// checkPoint
// TODO: 保证文件完整
// 1. 进程崩溃
// 2. 电源切断
/*
type Block struct {
	id        int64 				// 8B
	checkSum  [16]byte 				// 16B
	size      uint64 				// 8B
	operation consts.OperatorType 	// 8B
	payload   []byte
}
*/
func (wal *WriteAheadLog) checkPoint() {
	defer utils.HandlePanic(func() {
		if err := wal.destroy(); err != nil {
			log.Errorln(err)
		}
	})

	wal.ticker = time.NewTicker(SyncFrequency)
	for range wal.ticker.C {
		utils.WithLock(&wal.mu, func() {
			os.Stat()
			// 循环写入cycleLog，保证文件开头是block的起点
			fileInfo, err := tmpFd.Stat()
			if err != nil {
				log.Fatalln(err)
			}
			fileSize := fileInfo.Size()
			writer := bufio.NewWriter(tmpFd)
			var writtenByteSize int64
			for i := 0; i < len(wal.Blocks); i++ {
				rawBlock := wal.Blocks[i].Marshal()
				writtenByteSize += int64(len(rawBlock))
				if fileSize+writtenByteSize > wal.getMaxWalFileSize() {
					_, err := tmpFd.Seek(0, 0)
					writtenByteSize = 0
					fileSize = 0
					if err != nil {
						log.Fatalln(err)
					}
				}
				_, err := writer.Write(rawBlock)
				if err != nil {
					log.Fatalln(err)
				}
			}
			wal.Blocks = wal.Blocks[:0]
		})
	}
}

func (wal *WriteAheadLog) getMaxWalFileSize() int {
	if utils.IsTest() {
		return MaxWalFileSizeForTest
	}
	return MaxWalFileSize
}

func (wal *WriteAheadLog) destroy() error {
	if wal.ticker != nil {
		wal.ticker.Stop()
	}
	return nil
}

func (wal *WriteAheadLog) Append(operation consts.OperatorType, payload []byte) {
	utils.WithLock(&wal.mu, func() {
		wal.Blocks = append(wal.Blocks, NewBlock(wal.NextBlockId, operation, payload))
		wal.NextBlockId = (wal.NextBlockId + 1) % WalMask
	})
}

func (wal *WriteAheadLog) replay() error {
	// todo
	return nil
}
