package ragdoll

import "github.com/Trinoooo/eggie_kv/consts"

type Op struct {
	Type  consts.OperatorType
	Key   string
	Value []byte
}

func NewOp(t consts.OperatorType, key string, value []byte) *Op {
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
