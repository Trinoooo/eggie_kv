package wal

import (
	"testing"
)

func TestWriteAheadLog(t *testing.T) {
	/*opts := NewOptions().
		SetDirPerm(0770).
		SetDataPerm(0660).
		SetNoSync()
	wal, err := Open("./test_data/wal/", opts)
	if err != nil {
		t.Fatal(err)
	}*/

	/*wal.write(consts.OperatorTypeSet, []byte{1, 3, 5, 2, 4, 6})
	time.Sleep(2 * time.Second)
	t.Log("#1 finish append block")
	wal.write(consts.OperatorTypeSet, []byte{1, 3, 5, 2, 4, 6})
	time.Sleep(1 * time.Second)
	t.Log("#2 finish append block")
	wal.write(consts.OperatorTypeSet, []byte{1, 3, 5, 2, 4, 6})
	time.Sleep(1 * time.Second)
	t.Log("#3 finish append block")*/
}
