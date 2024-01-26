package ragdoll

import (
	"crypto/md5"
	"encoding/binary"
	"github.com/Trinoooo/eggie_kv/utils"
	"os"
	"syscall"
)

type Record struct {
	CheckSum [16]byte
	Size     uint64
	Key      string
	Value    []byte
}

func NewRecord(key string, value []byte) *Record {
	r := &Record{
		Size:  uint64(len(append([]byte(key), value...))),
		Key:   key,
		Value: value,
	}
	r.CheckSum = r.signature()
	return r
}

func (r *Record) signature() [16]byte {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, r.Size)
	return md5.Sum(append(append(buffer, []byte(r.Key)...), r.Value...))
}

func (r *Record) checkIntegrity() bool {
	return r.CheckSum == r.signature()
}

// Data 磁盘中的数据文件
type Data struct {
	Fd  *os.File
	Mem map[string]*Record
}

func NewData(path string) (*Data, error) {
	fd, err := utils.CheckAndCreateFile(path, syscall.O_APPEND|syscall.O_CREAT|syscall.O_RDWR, 0660)
	if err != nil {
		return nil, err
	}

	return &Data{
		Fd:  fd,
		Mem: make(map[string]*Record),
	}, nil
}
