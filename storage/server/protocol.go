package server

import (
	"encoding/binary"
	"fmt"
	"math"
)

/*
	协议实现主要参考 https://anthony-dong.github.io/2022/03/20/1fbc1901406195cf47c58e7436468f2e/

	thrift 官方文档似乎没有对协议的详细介绍
	因此实现上一方面读源码，一方面参考第三方资料
	另外市面上有一本《Programmer's Guide to Apache Thrift》
	只是国内还没有翻译版本，且英文原书贵到离谱
*/

const (
	VERSION_1    = 0x80010000
	VERSION_MASK = 0xffff0000
)

type MessageType int

const (
	MESSAGE_TYPE_INALID MessageType = iota
	MESSAGE_TYPE_CALL
	MESSAGE_TYPE_REPLY
	MESSAGE_TYPE_EXCEPTION
	MESSAGE_TYPE_ONEWAY
)

type FieldType byte

const (
	FIELD_TYPE_STOP   = 0
	FIELD_TYPE_VOID   = 1
	FIELD_TYPE_BOOL   = 2
	FIELD_TYPE_BYTE   = 3
	FIELD_TYPE_I08    = 3
	FIELD_TYPE_DOUBLE = 4
	FIELD_TYPE_I16    = 6
	FIELD_TYPE_I32    = 8
	FIELD_TYPE_I64    = 10
	FIELD_TYPE_STRING = 11
	FIELD_TYPE_UTF7   = 11
	FIELD_TYPE_STRUCT = 12
	FIELD_TYPE_MAP    = 13
	FIELD_TYPE_SET    = 14
	FIELD_TYPE_LIST   = 15
	FIELD_TYPE_UUID   = 16
)

var _ IProtocol = &BinaryProtocol{}

type IProtocol interface {
	WriteMessageBegin(name string, mt MessageType, seq int32) error
	WriteMessageEnd() error
	WriteStructBegin(name string) error
	WriteStructEnd() error
	WriteFieldBegin(name string, ftype FieldType, fieldId int16) error
	WriteFieldEnd() error
	WriteFieldStop() error
	WriteMapBegin(ktype, vtype FieldType, size int32) error
	WriteMapEnd() error
	WriteListBegin(etype FieldType, size int32) error
	WriteListEnd() error
	WriteSetBegin(etype FieldType, size int32) error
	WriteSetEnd() error
	WriteBool(v bool) error
	WriteByte(v byte) error
	WriteI16(v int16) error
	WriteI32(v int32) error
	WriteI64(v int64) error
	WriteDouble(v float64) error
	WriteString(v string) error

	ReadMessageBegin() (name string, mt MessageType, seqId int32, err error)
	ReadMessageEnd() error
	ReadStructBegin() (name string, err error)
	ReadStructEnd() error
	ReadFieldBegin() (name string, ftype FieldType, fieldId int16, err error)
	ReadFieldEnd() error
	ReadMapBegin() (ktype FieldType, vtype FieldType, size int32, err error)
	ReadMapEnd() error
	ReadListBegin() (etype FieldType, size int32, err error)
	ReadListEnd() error
	ReadSetBegin() (etype FieldType, size int32, err error)
	ReadSetEnd() error
	ReadBool() (bool, error)
	ReadByte() (byte, error)
	ReadI16() (int16, error)
	ReadI32() (int32, error)
	ReadI64() (int64, error)
	ReadDouble() (float64, error)
	ReadString() (string, error)
}

type BinaryProtocol struct {
	trans  ITransport
	buffer [64]byte
}

/*
Binary protocol Message, strict encoding, 12+ bytes:
+--------+--------+--------+--------+--------+--------+--------+--------+--------+...+--------+--------+--------+--------+--------+
|1vvvvvvv|vvvvvvvv|unused  |00000mmm| name length                       | name                | seq id                            |
+--------+--------+--------+--------+--------+--------+--------+--------+--------+...+--------+--------+--------+--------+--------+
*/

func (b *BinaryProtocol) WriteMessageBegin(name string, mt MessageType, seqId int32) error {
	// 不区分strict，一律按照新版本来
	err := b.WriteI32(int32(0x80020000 | mt))
	if err != nil {
		return err
	}

	err = b.WriteString(name)
	if err != nil {
		return err
	}

	err = b.WriteI32(seqId)
	if err != nil {
		return err
	}

	return nil
}

func (b *BinaryProtocol) WriteMessageEnd() error {
	return nil
}

func (b *BinaryProtocol) WriteStructBegin(name string) error {
	return nil
}

func (b *BinaryProtocol) WriteStructEnd() error {
	return nil
}

/*
Binary protocol field header and field value:
+--------+--------+--------+--------+...+--------+
|tttttttt| field id        | field value         |
+--------+--------+--------+--------+...+--------+

Binary protocol stop field:
+--------+
|00000000|
+--------+
*/

func (b *BinaryProtocol) WriteFieldBegin(name string, ftype FieldType, fieldId int16) error {
	err := b.WriteByte(byte(ftype))
	if err != nil {
		return err
	}

	err = b.WriteI16(fieldId)
	if err != nil {
		return err
	}

	return nil
}

func (b *BinaryProtocol) WriteFieldEnd() error {
	return nil
}

func (b *BinaryProtocol) WriteFieldStop() error {
	return b.WriteByte(FIELD_TYPE_STOP)
}

/*
Binary protocol map (6+ bytes) and key value pairs:
+--------+--------+--------+--------+--------+--------+--------+...+--------+
|kkkkkkkk|vvvvvvvv| size                              | key value pairs     |
+--------+--------+--------+--------+--------+--------+--------+...+--------+
*/

func (b *BinaryProtocol) WriteMapBegin(ktype, vtype FieldType, size int32) error {
	err := b.WriteByte(byte(ktype))
	if err != nil {
		return err
	}

	err = b.WriteByte(byte(vtype))
	if err != nil {
		return err
	}

	err = b.WriteI32(size)
	if err != nil {
		return err
	}

	return nil
}

func (b *BinaryProtocol) WriteMapEnd() error {
	return nil
}

/*
Binary protocol list (5+ bytes) and elements:
+--------+--------+--------+--------+--------+--------+...+--------+
|tttttttt| size                              | elements            |
+--------+--------+--------+--------+--------+--------+...+--------+
*/

func (b *BinaryProtocol) WriteListBegin(etype FieldType, size int32) error {
	err := b.WriteByte(byte(etype))
	if err != nil {
		return err
	}

	err = b.WriteI32(size)
	if err != nil {
		return err
	}

	return nil
}

func (b *BinaryProtocol) WriteListEnd() error {
	return nil
}

/*
Binary protocol set (5+ bytes) and elements:
+--------+--------+--------+--------+--------+--------+...+--------+
|tttttttt| size                              | elements            |
+--------+--------+--------+--------+--------+--------+...+--------+
*/

func (b *BinaryProtocol) WriteSetBegin(etype FieldType, size int32) error {
	err := b.WriteByte(byte(etype))
	if err != nil {
		return err
	}

	err = b.WriteI32(size)
	if err != nil {
		return err
	}

	return nil
}

func (b *BinaryProtocol) WriteSetEnd() error {
	return nil
}

func (b *BinaryProtocol) WriteBool(v bool) error {
	t := b.buffer[0:1]
	if v {
		t[0] = 1
	} else {
		t[0] = 0
	}
	return b.WriteBytes(t)
}

func (b *BinaryProtocol) WriteByte(v byte) error {
	t := b.buffer[0:1]
	t[0] = v
	return b.WriteBytes(t)
}

func (b *BinaryProtocol) WriteI16(v int16) error {
	t := b.buffer[0:2]
	binary.BigEndian.PutUint16(t, uint16(v))
	return b.WriteBytes(t)
}

func (b *BinaryProtocol) WriteI32(v int32) error {
	t := b.buffer[0:4]
	binary.BigEndian.PutUint32(t, uint32(v))
	return b.WriteBytes(t)
}

func (b *BinaryProtocol) WriteI64(v int64) error {
	t := b.buffer[0:8]
	binary.BigEndian.PutUint64(t, uint64(v))
	return b.WriteBytes(t)
}

func (b *BinaryProtocol) WriteDouble(v float64) error {
	t := b.buffer[0:8]
	binary.BigEndian.PutUint64(t, math.Float64bits(v))
	return b.WriteBytes(t)
}

/*
Binary protocol, binary data, 4+ bytes:
+--------+--------+--------+--------+--------+...+--------+
| byte length                       | bytes                |
+--------+--------+--------+--------+--------+...+--------+
*/

func (b *BinaryProtocol) WriteString(v string) error {
	lov := len(v)
	err := b.WriteI32(int32(lov))
	if err != nil {
		return err
	}

	return b.WriteBytes([]byte(v))
}

func (b *BinaryProtocol) WriteBytes(v []byte) error {
	lov := len(v)
	written := 0
	for written < lov {
		n, err := b.trans.Write(v)
		if err != nil {
			return err
		}

		written += n
	}
	return nil
}

/*
Binary protocol Message, strict encoding, 12+ bytes:
+--------+--------+--------+--------+--------+--------+--------+--------+--------+...+--------+--------+--------+--------+--------+
|1vvvvvvv|vvvvvvvv|unused  |00000mmm| name length                       | name                | seq id                            |
+--------+--------+--------+--------+--------+--------+--------+--------+--------+...+--------+--------+--------+--------+--------+
*/

func (b *BinaryProtocol) ReadMessageBegin() (name string, mt MessageType, seqId int32, err error) {
	i32, err := b.ReadI32()
	if err != nil {
		return "", MESSAGE_TYPE_INALID, 0, err
	}

	if version := uint32(i32) & VERSION_MASK; version != VERSION_1 {
		return "", MESSAGE_TYPE_INALID, 0, fmt.Errorf("Bad version in ReadMessageBegin")
	}

	name, err = b.ReadString()
	if err != nil {
		return "", MESSAGE_TYPE_INALID, 0, err
	}

	seqId, err = b.ReadI32()
	if err != nil {
		return "", MESSAGE_TYPE_INALID, 0, err
	}

	return name, MessageType(i32 & 0xff), seqId, nil
}

func (b *BinaryProtocol) ReadMessageEnd() error {
	return nil
}

func (b *BinaryProtocol) ReadStructBegin() (name string, err error) {
	return "", nil
}

func (b *BinaryProtocol) ReadStructEnd() error {
	return nil
}

/*
Binary protocol field header and field value:
+--------+--------+--------+--------+...+--------+
|tttttttt| field id        | field value         |
+--------+--------+--------+--------+...+--------+

Binary protocol stop field:
+--------+
|00000000|
+--------+
*/

func (b *BinaryProtocol) ReadFieldBegin() (name string, ftype FieldType, fieldId int16, err error) {
	ft, err := b.ReadByte()
	if err != nil {
		return "", FIELD_TYPE_STOP, 0, err
	}

	fieldId, err = b.ReadI16()
	if err != nil {
		return "", FIELD_TYPE_STOP, 0, err
	}

	return name, FieldType(ft), fieldId, nil
}

func (b *BinaryProtocol) ReadFieldEnd() error {
	return nil
}

/*
Binary protocol map (6+ bytes) and key value pairs:
+--------+--------+--------+--------+--------+--------+--------+...+--------+
|kkkkkkkk|vvvvvvvv| size                              | key value pairs     |
+--------+--------+--------+--------+--------+--------+--------+...+--------+
*/

func (b *BinaryProtocol) ReadMapBegin() (ktype FieldType, vtype FieldType, size int32, err error) {
	kt, err := b.ReadByte()
	if err != nil {
		return FIELD_TYPE_STOP, FIELD_TYPE_STOP, 0, err
	}

	vt, err := b.ReadByte()
	if err != nil {
		return FIELD_TYPE_STOP, FIELD_TYPE_STOP, 0, err
	}

	size, err = b.ReadI32()
	if err != nil {
		return FIELD_TYPE_STOP, FIELD_TYPE_STOP, 0, err
	}

	return FieldType(kt), FieldType(vt), size, nil
}

func (b *BinaryProtocol) ReadMapEnd() error {
	return nil
}

/*
Binary protocol list (5+ bytes) and elements:
+--------+--------+--------+--------+--------+--------+...+--------+
|tttttttt| size                              | elements            |
+--------+--------+--------+--------+--------+--------+...+--------+
*/

func (b *BinaryProtocol) ReadListBegin() (etype FieldType, size int32, err error) {
	et, err := b.ReadByte()
	if err != nil {
		return FIELD_TYPE_STOP, 0, err
	}

	size, err = b.ReadI32()
	if err != nil {
		return FIELD_TYPE_STOP, 0, err
	}

	return FieldType(et), size, nil
}

func (b *BinaryProtocol) ReadListEnd() error {
	return nil
}

/*
Binary protocol set (5+ bytes) and elements:
+--------+--------+--------+--------+--------+--------+...+--------+
|tttttttt| size                              | elements            |
+--------+--------+--------+--------+--------+--------+...+--------+
*/

func (b *BinaryProtocol) ReadSetBegin() (etype FieldType, size int32, err error) {
	et, err := b.ReadByte()
	if err != nil {
		return FIELD_TYPE_STOP, 0, err
	}

	size, err = b.ReadI32()
	if err != nil {
		return FIELD_TYPE_STOP, 0, err
	}

	return FieldType(et), size, nil
}

func (b *BinaryProtocol) ReadSetEnd() error {
	return nil
}

func (b *BinaryProtocol) ReadBool() (v bool, err error) {
	bte, err := b.ReadByte()
	if err != nil {
		return false, err
	}

	if bte == 1 {
		v = true
	}
	return v, nil
}

func (b *BinaryProtocol) ReadByte() (byte, error) {
	t := b.buffer[0:1]
	err := b.ReadBytes(t)
	return t[0], err
}

func (b *BinaryProtocol) ReadI16() (int16, error) {
	t := b.buffer[0:2]
	err := b.ReadBytes(t)
	return int16(binary.BigEndian.Uint16(t)), err
}

func (b *BinaryProtocol) ReadI32() (int32, error) {
	t := b.buffer[0:4]
	err := b.ReadBytes(t)
	return int32(binary.BigEndian.Uint32(t)), err
}

func (b *BinaryProtocol) ReadI64() (int64, error) {
	t := b.buffer[0:8]
	err := b.ReadBytes(t)
	return int64(binary.BigEndian.Uint64(t)), err
}

func (b *BinaryProtocol) ReadDouble() (float64, error) {
	t := b.buffer[0:8]
	err := b.ReadBytes(t)
	return math.Float64frombits(binary.BigEndian.Uint64(t)), err
}

/*
Binary protocol, binary data, 4+ bytes:
+--------+--------+--------+--------+--------+...+--------+
| byte length                       | bytes                |
+--------+--------+--------+--------+--------+...+--------+
*/

func (b *BinaryProtocol) ReadString() (string, error) {
	length, err := b.ReadI32()
	if err != nil {
		return "", err
	}

	var buf []byte
	if length > int32(len(b.buffer)) {
		buf = make([]byte, length)
	} else {
		buf = b.buffer[0:length]
	}

	err = b.ReadBytes(buf)
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func (b *BinaryProtocol) ReadBytes(v []byte) error {
	read := 0
	lov := len(v)
	for read < lov {
		n, err := b.trans.Read(v)
		if err != nil {
			return err
		}

		read += n
	}
	return nil
}

type BinaryProtocolFactory struct {
}

func NewBinaryProtocolFactory() *BinaryProtocolFactory {
	return &BinaryProtocolFactory{}
}

func (b *BinaryProtocolFactory) Build(trans ITransport) IProtocol {
	return &BinaryProtocol{
		trans: trans,
	}
}

type IProtocolFactory interface {
	Build(trans ITransport) IProtocol
}
