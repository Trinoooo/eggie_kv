package errs

import (
	"errors"
	"fmt"
)

// todo: code归类

type KvErr struct {
	msg  string
	code int64
	err  error
}

// Error 输出格式：
// [错误码] 错误类型描述 ( => 包含错误详细描述 )
// 解释：(xxx) 表示可选内容
func (ke *KvErr) Error() string {
	details := fmt.Sprintf("[%d] %s", ke.code, ke.msg)
	if ke.err != nil {
		details += fmt.Sprintf(" => %s", ke.err)
	}

	return details
}

func (ke *KvErr) Code() int64 {
	return ke.code
}

func (ke *KvErr) WithErr(err error) *KvErr {
	ke.err = err
	return ke
}

func GetCode(err error) int64 {
	var ke *KvErr
	if errors.As(err, &ke) {
		return ke.code
	}
	return UnknownErrCode
}

const (
	UnknownErrCode                 = 0
	InvalidParamErrCode            = 100001
	JsonMarshalErrCode             = 100002
	JsonUnmarshalErrCode           = 100003
	UnsupportedOperatorTypeErrCode = 100004
	OpenFileErrCode                = 100005
	DirNotExistErrCode             = 100006
	FileNoPermissionErrCode        = 100007
	FileStatErrCode                = 100008
	MkdirErrCode                   = 100009
	ReadFileErrCode                = 100010
	WriteFileErrCode               = 100011
	SyncFileErrCode                = 100012
	CloseFileErrCode               = 100013
	FileIntegrityErrCode           = 100014
	ExecCmdErrCode                 = 100015
	GetWdErrCode                   = 100016
	WalkDirErrCode                 = 100017
	FileClosedErrCode              = 100018
	SeekFileErrCode                = 100019
	SegmentFullErrCode             = 100020
	NotFoundErrCode                = 100021
	CorruptErrCode                 = 100022
	WalFullErrCode                 = 100023
	BackgroundErrCode              = 100024
	ReachBlockIdxLimitErrCode      = 100025
	GetErrCode                     = 100026
	SetErrCode                     = 100027
	BuildCoreErrCode               = 200001
	ParseIntErrCode                = 100028
	RenameFileErrCode              = 100029
	TruncateFileErrCode            = 100030
	RemoveFileErrCode              = 100031
	FlockFileErrCode               = 100032
	CreateTempFileErrCode          = 100033
	CopyFileErrCode                = 100034
	CoreNotFoundErrCode            = 100035
	ReadSocketErrCode              = 100036
	WriteSocketErrCode             = 100037
)

func NewUnknownErr() *KvErr {
	return &KvErr{msg: "unknown error", code: UnknownErrCode}
}

func NewInvalidParamErr() *KvErr {
	return &KvErr{msg: "invalid params", code: InvalidParamErrCode}
}

func NewJsonMarshalErr() *KvErr {
	return &KvErr{msg: "json marshal failed", code: JsonMarshalErrCode}
}

func NewJsonUnmarshalErr() *KvErr {
	return &KvErr{msg: "json unmarshal failed", code: JsonUnmarshalErrCode}
}

func NewUnsupportedOperatorTypeErr() *KvErr {
	return &KvErr{msg: "unsupported operator type", code: UnsupportedOperatorTypeErrCode}
}

func NewOpenFileErr() *KvErr {
	return &KvErr{msg: "open file failed", code: OpenFileErrCode}
}

func NewDirNotExistErr() *KvErr {
	return &KvErr{msg: "directory not exist", code: DirNotExistErrCode}
}

func NewFileNoPermissionErr() *KvErr {
	return &KvErr{msg: "file no permission", code: FileNoPermissionErrCode}
}

func NewFileStatErr() *KvErr {
	return &KvErr{msg: "file stat failed", code: FileStatErrCode}
}

func NewMkdirErr() *KvErr {
	return &KvErr{msg: "mkdir failed", code: MkdirErrCode}
}

func NewReadFileErr() *KvErr {
	return &KvErr{msg: "read file failed", code: ReadFileErrCode}
}

func NewWriteFileErr() *KvErr {
	return &KvErr{msg: "write file failed", code: WriteFileErrCode}
}

func NewSyncFileErr() *KvErr {
	return &KvErr{msg: "sync file failed", code: SyncFileErrCode}
}

func NewCloseFileErr() *KvErr {
	return &KvErr{msg: "close file failed", code: CloseFileErrCode}
}

func NewFileIntegrityErr() *KvErr {
	return &KvErr{msg: "file integrity has been compromised", code: FileIntegrityErrCode}
}

func NewExecCmdErr() *KvErr {
	return &KvErr{msg: "exec shell command failed", code: ExecCmdErrCode}
}

func NewGetWdErr() *KvErr {
	return &KvErr{msg: "get work dir failed", code: GetWdErrCode}
}

func NewWalkDirErr() *KvErr {
	return &KvErr{msg: "walk dir failed", code: WalkDirErrCode}
}

func NewFileClosedErr() *KvErr {
	return &KvErr{msg: "file already closed", code: FileClosedErrCode}
}

func NewSeekFileErr() *KvErr {
	return &KvErr{msg: "seek file failed", code: SeekFileErrCode}
}

func NewSegmentFullErr() *KvErr {
	return &KvErr{msg: "segment file full", code: SegmentFullErrCode}
}

func NewNotFoundErr() *KvErr {
	return &KvErr{msg: "not found", code: NotFoundErrCode}
}

func NewCorruptErr() *KvErr {
	return &KvErr{msg: "file content corrupt", code: CorruptErrCode}
}

func NewWalFullErr() *KvErr {
	return &KvErr{msg: "wal logs full", code: WalFullErrCode}
}

func NewBackgroundErr() *KvErr {
	return &KvErr{msg: "background goroutine failed", code: BackgroundErrCode}
}

func NewReachBlockIdxLimitErr() *KvErr {
	return &KvErr{msg: "reach block idx limit", code: ReachBlockIdxLimitErrCode}
}

func NewGetErr() *KvErr {
	return &KvErr{msg: "error occur when get value", code: GetErrCode}
}

func NewSetErr() *KvErr {
	return &KvErr{msg: "error occur when set value", code: SetErrCode}
}

func NewBuildCoreErr() *KvErr {
	return &KvErr{msg: "build core failed", code: BuildCoreErrCode}
}

func NewParseIntErr() *KvErr {
	return &KvErr{msg: "strcov parse int failed", code: ParseIntErrCode}
}

func NewRenameFileErr() *KvErr {
	return &KvErr{msg: "rename file failed", code: RenameFileErrCode}
}

func NewTruncateFileErr() *KvErr {
	return &KvErr{msg: "truncate file failed", code: TruncateFileErrCode}
}

func NewRemoveFileErr() *KvErr {
	return &KvErr{msg: "remove file failed", code: RemoveFileErrCode}
}

func NewFlockFileErr() *KvErr {
	return &KvErr{msg: "flock file failed", code: FlockFileErrCode}
}

func NewCreateTempFileErr() *KvErr {
	return &KvErr{msg: "create temp file failed", code: CreateTempFileErrCode}
}

func NewCopyFileErr() *KvErr {
	return &KvErr{msg: "copy file failed", code: CopyFileErrCode}
}

func NewCoreNotFoundErr() *KvErr {
	return &KvErr{msg: "core not found", code: CoreNotFoundErrCode}
}

func NewReadSocketErr() *KvErr {
	return &KvErr{msg: "read socket failed", code: ReadSocketErrCode}
}

func NewWriteSocketErr() *KvErr {
	return &KvErr{msg: "write socket failed", code: WriteSocketErrCode}
}
