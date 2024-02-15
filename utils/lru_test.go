package utils

import "testing"

func TestLru(t *testing.T) {
	lru := NewLRU(3)

	t.Log("Read idx #1:", lru.Read(1))
	lru.Write(1, []byte{1})
	t.Log("Read idx #1:", lru.Read(1))
	lru.Write(1, []byte{1})
	t.Log("Read idx #1:", lru.Read(1))
	lru.Write(2, []byte{2})
	t.Log("Read idx #2:", lru.Read(2))
	lru.Write(3, []byte{3})
	t.Log("Read idx #3:", lru.Read(3))
	eliminate := lru.Write(4, []byte{4})
	t.Log("Read idx #1:", lru.Read(1))
	if eliminate == nil {
		t.Error("expect eliminate not null")
		t.Log(eliminate)
	}

	t.Log(lru.Remove(2))
	t.Log(lru.Remove(3))
	t.Log(lru.Remove(4))
}
