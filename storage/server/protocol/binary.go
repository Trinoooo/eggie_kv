package protocol

import (
	"encoding/binary"
	"github.com/Trinoooo/eggie_kv/storage/server/connections"
)

type BinaryProtocol struct {
	transport connections.IConnection
	buf       [64]byte // inline array. for memory allocate optimization
}

func NewBinaryProtocol(transport connections.IConnection) *BinaryProtocol {
	return &BinaryProtocol{
		transport: transport,
		buf:       [64]byte{},
	}
}

func (cbp *BinaryProtocol) GetConnection() connections.IConnection {
	return cbp.transport
}

func (cbp *BinaryProtocol) ReadStructBegin() error {
	return nil
}

func (cbp *BinaryProtocol) ReadStructEnd() error {
	return nil
}

func (cbp *BinaryProtocol) ReadI64() (int64, error) {
	i64Bytes := cbp.buf[:8]
	_, err := cbp.transport.Read(i64Bytes)
	if err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(i64Bytes)), nil
}

func (cbp *BinaryProtocol) ReadString() (string, error) {
	bytes, err := cbp.ReadBytes()
	return string(bytes), err
}

func (cbp *BinaryProtocol) ReadBytes() ([]byte, error) {
	length, err := cbp.ReadI64()
	if err != nil {
		return nil, err
	}

	var buf []byte
	if length <= 64 {
		buf = cbp.buf[:length]
	} else {
		buf = make([]byte, 0, length)
	}

	_, err = cbp.transport.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (cbp *BinaryProtocol) WriteStructBegin() error {
	return nil
}

func (cbp *BinaryProtocol) WriteStructEnd() error {
	return nil
}

func (cbp *BinaryProtocol) WriteI64(v int64) error {
	i64Bytes := cbp.buf[:8]
	binary.BigEndian.PutUint64(i64Bytes, uint64(v))
	_, err := cbp.transport.Write(i64Bytes)
	return err
}

func (cbp *BinaryProtocol) WriteString(v string) error {
	return cbp.WriteBytes([]byte(v))
}

func (cbp *BinaryProtocol) WriteBytes(v []byte) error {
	length := int64(len(v))
	err := cbp.WriteI64(length)
	if err != nil {
		return err
	}

	_, err = cbp.transport.Write(v)
	return err
}
