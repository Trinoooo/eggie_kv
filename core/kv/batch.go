package kv

type OpType int64

const (
	OpTypeUnset OpType = iota
	OpTypeGet
	OpTypeSet
)

type Op struct {
	Type  OpType
	Key   string
	Value []byte
}

func NewOp(t OpType, key string, value []byte) *Op {
	return &Op{
		Type:  t,
		Key:   key,
		Value: value,
	}
}

type Batch struct {
	Ops []*Op
}

func NewBatch() any {
	return &Batch{
		Ops: make([]*Op, 0),
	}
}

func (b *Batch) AppendOps(ops ...*Op) {
	b.Ops = append(b.Ops, ops...)
}

func (b *Batch) Reset() {
	b.Ops = b.Ops[:0]
}
