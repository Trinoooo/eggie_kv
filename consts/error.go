package consts

import (
	"fmt"
	"strings"
)

// todo: code归类

type KvErr struct {
	msg    string
	code   int64
	err    error
	fields map[FieldName]interface{}
}

type FieldName string

const (
	Params = "params"
	Value  = "value"
)

// Error 输出格式：
// [错误码] 错误类型描述 ( => 包含错误详细描述 ) ( 字段=字段值 )...
// 解释：(xxx) 表示可选内容；...表示可以有多个
func (ke *KvErr) Error() string {
	details := fmt.Sprintf(" [%d] %s", ke.code, ke.msg)
	if ke.err != nil {
		details += fmt.Sprintf(" => %s", ke.err)
	}

	var fields []string
	for k, v := range ke.fields {
		fields = append(fields, fmt.Sprintf("%v=%v", k, v))
	}
	return strings.Join(fields, " ") + details
}

func (ke *KvErr) Code() int64 {
	return ke.code
}

func (ke *KvErr) WithErr(err error) *KvErr {
	ke.err = err
	return ke
}

func (ke *KvErr) WithField(fields map[FieldName]interface{}) *KvErr {
	if ke.fields == nil {
		ke.fields = make(map[FieldName]interface{})
	}

	for k, v := range fields {
		ke.fields[k] = v
	}
	return ke
}

func NewUnknownErr() *KvErr {
	return &KvErr{msg: "unknown error", code: 0}
}

func NewInvalidParamErr() *KvErr {
	return &KvErr{msg: "invalid params", code: 100001}
}

func NewJsonMarshalErr() *KvErr {
	return &KvErr{msg: "json marshal failed", code: 100002}
}

func NewJsonUnmarshalErr() *KvErr {
	return &KvErr{msg: "json unmarshal failed", code: 100003}
}

func NewUnsupportedOperatorTypeErr() *KvErr {
	return &KvErr{msg: "unsupported operator type", code: 100004}
}

func NewOpenFileErr() *KvErr {
	return &KvErr{msg: "open file failed", code: 100005}
}

func NewDirNotExistErr() *KvErr {
	return &KvErr{msg: "directory not exist", code: 100006}
}

func NewFileNoPermissionErr() *KvErr {
	return &KvErr{msg: "file no permission", code: 100007}
}

func NewFileStatErr() *KvErr {
	return &KvErr{msg: "file stat failed", code: 100008}
}

func NewMkdirErr() *KvErr {
	return &KvErr{msg: "mkdir failed", code: 100009}
}

func NewReadFileErr() *KvErr {
	return &KvErr{msg: "read file failed", code: 100010}
}

func NewWriteFileErr() *KvErr {
	return &KvErr{msg: "write file failed", code: 100011}
}

func NewSyncFileErr() *KvErr {
	return &KvErr{msg: "sync file failed", code: 100012}
}

func NewCloseFileErr() *KvErr {
	return &KvErr{msg: "close file failed", code: 100013}
}

func NewFileIntegrityErr() *KvErr {
	return &KvErr{msg: "file integrity has been compromised", code: 100014}
}

func NewExecCmdErr() *KvErr {
	return &KvErr{msg: "exec shell command failed", code: 100015}
}

func NewGetWdErr() *KvErr {
	return &KvErr{msg: "get work dir failed", code: 100016}
}

func NewWalkDirErr() *KvErr {
	return &KvErr{msg: "walk dir failed", code: 100017}
}

func NewFileClosedErr() *KvErr {
	return &KvErr{msg: "file already closed", code: 100018}
}

func NewSeekFileErr() *KvErr {
	return &KvErr{msg: "seek file failed", code: 100019}
}

func NewSegmentFullErr() *KvErr {
	return &KvErr{msg: "segment file full", code: 100020}
}

func NewNotFoundErr() *KvErr {
	return &KvErr{msg: "not found", code: 100021}
}

func NewCorruptErr() *KvErr {
	return &KvErr{msg: "file content corrupt", code: 100022}
}

func NewWalFullErr() *KvErr {
	return &KvErr{msg: "wal logs full", code: 100023}
}

func NewBackgroundErr() *KvErr {
	return &KvErr{msg: "background goroutine failed", code: 100024}
}

func NewReachBlockIdxLimitErr() *KvErr {
	return &KvErr{msg: "reach block idx limit", code: 100025}
}

func NewGetErr() *KvErr {
	return &KvErr{msg: "error occur when get value", code: 100026}
}

func NewSetErr() *KvErr {
	return &KvErr{msg: "error occur when set value", code: 100027}
}

func NewBuildCoreErr() *KvErr {
	return &KvErr{msg: "build core failed", code: 200001}
}

func NewParseIntErr() *KvErr {
	return &KvErr{msg: "strcov parse int failed", code: 100028}
}

func NewRenameFileErr() *KvErr {
	return &KvErr{msg: "rename file failed", code: 100029}
}

func NewTruncateFileErr() *KvErr {
	return &KvErr{msg: "truncate file failed", code: 100030}
}

func NewRemoveFileErr() *KvErr {
	return &KvErr{msg: "remove file failed", code: 100031}
}

func NewFlockFileErr() *KvErr {
	return &KvErr{msg: "flock file failed", code: 100032}
}

func NewCreateTempFileErr() *KvErr {
	return &KvErr{msg: "create temp file failed", code: 100033}
}

func NewCopyFileErr() *KvErr {
	return &KvErr{msg: "copy file failed", code: 100034}
}
