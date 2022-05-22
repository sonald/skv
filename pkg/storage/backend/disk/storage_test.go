package disk

import (
	"testing"
)

func TestPut(t *testing.T) {
	s := NewDiskStorage()
	s.Put("key1", "value1")
	v, err := s.Get("key1")
	if err != nil {
		t.Errorf("get key1 failed: %s\n", err.Error())
		return
	}

	if v != "key1" {
		t.Failed()
	}
}
