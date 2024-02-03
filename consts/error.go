package consts

type KvErr struct {
	msg  string
	code int64
}

func (ke *KvErr) Error() string {
	return ke.msg
}

func (ke *KvErr) Code() int64 {
	return ke.code
}

var (
	InvalidParamErr            = &KvErr{msg: "invalid params", code: 100001}
	UnexpectErr                = &KvErr{msg: "unexpect error", code: 100002}
	JsonMarshalErr             = &KvErr{msg: "json marshal failed", code: 100003}
	JsonUnmarshalErr           = &KvErr{msg: "json unmarshal failed", code: 100004}
	UnsupportedOperatorTypeErr = &KvErr{msg: "unsupported operator type", code: 100005}
	OpenFileErr                = &KvErr{msg: "open file failed", code: 100006}
	DirNotExistErr             = &KvErr{msg: "directory not exist", code: 100007}
	FileNoPermissionErr        = &KvErr{msg: "file no permission", code: 100008}
	FileStatErr                = &KvErr{msg: "file stat failed", code: 100009}
	MkdirErr                   = &KvErr{msg: "mkdir failed", code: 100010}
	ReadFileErr                = &KvErr{msg: "read file failed", code: 100011}
	WriteFileErr               = &KvErr{msg: "write file failed", code: 100012}
	SyncFileErr                = &KvErr{msg: "sync file failed", code: 100013}
	CloseFileErr               = &KvErr{msg: "close file failed", code: 100014}
	FileIntegrityErr           = &KvErr{msg: "file integrity has been compromised", code: 100015}
	ExecCmdErr                 = &KvErr{msg: "exec shell command failed", code: 100016}
	GetWdErr                   = &KvErr{msg: "get work dir failed", code: 100017}
	WalkDirErr                 = &KvErr{msg: "walk dir failed", code: 100018}
	FileClosedErr              = &KvErr{msg: "file already closed", code: 100019}
	SeekFileErr                = &KvErr{msg: "seek file failed", code: 100020}
	SegmentFullErr             = &KvErr{msg: "segment file full", code: 100021}
	NotFoundErr                = &KvErr{msg: "not found", code: 100022}
	CorruptErr                 = &KvErr{msg: "file content corrupt", code: 100023}
	WalFullErr                 = &KvErr{msg: "wal log full", code: 100024}
	BackgroundErr              = &KvErr{msg: "background failed", code: 100025}

	GetErr = &KvErr{msg: "error occur when get value", code: 100020}
	SetErr = &KvErr{msg: "error occur when set value", code: 100021}

	BuildCoreErr = &KvErr{msg: "build core failed", code: 200001}
)
