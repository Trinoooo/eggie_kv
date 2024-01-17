package wal

import "testing"

func TestLru(t *testing.T) {
	lru := NewLru(3)

	t.Log("read idx #1:", lru.Read(1))
	lru.Write(1, []byte{1})
	t.Log("read idx #1:", lru.Read(1))
	lru.Write(1, []byte{1})
	t.Log("read idx #1:", lru.Read(1))
	lru.Write(2, []byte{2})
	t.Log("read idx #2:", lru.Read(2))
	lru.Write(3, []byte{3})
	t.Log("read idx #3:", lru.Read(3))
	lru.Write(4, []byte{4})
	t.Log("read idx #1:", lru.Read(1))
}
