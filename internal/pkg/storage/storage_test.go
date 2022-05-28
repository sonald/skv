package storage

import (
	"bytes"
	"testing"
)

func TestInternalKey(t *testing.T) {
	t.Run("encode", func(t *testing.T) {
		k := KeyFromUser([]byte("hello"), 1234567, TagValue)
		buf := k.Encode()

		k2 := DecodeInternalKey(buf)

		if bytes.Compare(k.key, k2.key) != 0 || k.seqAndTag != k2.seqAndTag || k.Tag() != k2.Tag() {
			t.Fatal("encode failed")
		}

		if string(k.Key()) != "hello" {
			t.Fatal("key invalid")
		}

		if k.Sequence() != k2.Sequence() || k.Sequence() != 1234567 {
			t.Fatal("sequence error")
		}

		if k.Tag() != TagValue {
			t.Fatal("tag error")
		}

		k3, _ := ReadInternalKey(bytes.NewReader(buf))
		if bytes.Compare(k.Key(), k3.Key()) != 0 {
			t.Fatal("key invalid")
		}

		if k.Sequence() != k3.Sequence() || k.Sequence() != 1234567 {
			t.Fatal("sequence error")
		}

		if k3.Tag() != TagValue {
			t.Fatal("tag error")
		}

	})
}
