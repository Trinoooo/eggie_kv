package kv

import (
	"github.com/Trinoooo/eggie_kv/consts"
	"math"
	"testing"
	"time"
)

func TestBlock(t *testing.T) {
	block := NewBlock(10, consts.OperatorTypeUnknown, []byte{1, 2, 3, 99, 98, 97})
	raw := block.Marshal()
	formatOutput(t, raw)

	emptyBlock := NewEmptyBlock()
	_, err := emptyBlock.UnMarshal(raw)
	if err != nil {
		t.Error(err)
		return
	}

	formatOutput(t, emptyBlock.Marshal())
}

func formatOutput(t *testing.T, raw []byte) {
	for {
		l := len(raw)
		if l == 0 {
			break
		}
		upperLimit := int(math.Min(float64(l), 8))
		t.Logf("% #x", raw[:upperLimit])
		raw = raw[upperLimit:]
	}
}

func TestWriteAheadLog(t *testing.T) {
	wal, err := NewWriteAheadLog()
	if err != nil {
		t.Fatal(err)
	}

	wal.Append(consts.OperatorTypeSet, []byte{1, 3, 5, 2, 4, 6})
	time.Sleep(2 * time.Second)
	t.Log("#1 finish append block")
	wal.Append(consts.OperatorTypeSet, []byte{1, 3, 5, 2, 4, 6})
	time.Sleep(1 * time.Second)
	t.Log("#2 finish append block")
	wal.Append(consts.OperatorTypeSet, []byte{1, 3, 5, 2, 4, 6})
	time.Sleep(1 * time.Second)
	t.Log("#3 finish append block")
}
