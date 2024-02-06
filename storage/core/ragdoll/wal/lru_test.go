package wal

import "testing"

func TestLru(t *testing.T) {
	lru := newLru(3)

	t.Log("read idx #1:", lru.read(1))
	lru.write(1, []byte{1})
	t.Log("read idx #1:", lru.read(1))
	lru.write(1, []byte{1})
	t.Log("read idx #1:", lru.read(1))
	lru.write(2, []byte{2})
	t.Log("read idx #2:", lru.read(2))
	lru.write(3, []byte{3})
	t.Log("read idx #3:", lru.read(3))
	eliminate := lru.write(4, []byte{4})
	t.Log("read idx #1:", lru.read(1))
	if eliminate == nil {
		t.Error("expect eliminate not null")
		t.Log(eliminate)
	}

	t.Log(lru.remove(2))
	t.Log(lru.remove(3))
	t.Log(lru.remove(4))
}
