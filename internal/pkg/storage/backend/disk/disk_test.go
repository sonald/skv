package disk

import (
	"bytes"
	"fmt"
	"github.com/sonald/skv/internal/pkg/storage"
	"testing"
)

func TestPut(t *testing.T) {
	s := NewDiskStorage(storage.Options{
		Args: map[string]interface{}{
			SegmentNameOpt:          fmt.Sprintf("%s/segment", t.TempDir()),
			storage.SegmentOpenMode: storage.SegmentOpenModeWR,
		}})

	defer s.Close()

	t.Run("put", func(t *testing.T) {
		key := storage.KeyFromUser([]byte("key1"), 0, storage.TagValue)
		err := s.Put(key, []byte("value1"))
		if err != nil {
			t.Errorf("put failed: %s\n", err.Error())
		}

	})

}

func TestDiskStorage_Get(t *testing.T) {
	s := NewDiskStorage(storage.Options{
		Args: map[string]interface{}{
			SegmentNameOpt:          fmt.Sprintf("%s/segment", t.TempDir()),
			storage.SegmentOpenMode: storage.SegmentOpenModeRW,
		}})

	defer s.Close()

	var sequence uint64 = 1
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("key%02d", i)
		ikey := storage.KeyFromUser([]byte(key), sequence, storage.TagValue)
		sequence++

		s.Put(ikey, []byte(fmt.Sprintf("value%d", i)))
	}

	t.Run("get", func(t *testing.T) {
		ikey := storage.KeyFromUser([]byte("key101"), sequence, storage.TagValue)
		sequence++

		err := s.Put(ikey, []byte("value1"))
		if err != nil {
			t.Errorf("put key1 failed: %s\n", err.Error())
			return
		}

		v, err := s.Get(ikey)
		if err != nil {
			t.Errorf("get %s failed: %s\n", string(ikey.Key()), err.Error())
			return
		}

		if string(v) != "value1" {
			t.Failed()
		}
	})

	t.Run("get0", func(t *testing.T) {
		key := storage.KeyFromUser([]byte("key200"), 0, storage.TagValue)
		var val []byte
		err := s.Put(key, val)
		if err != nil {
			t.Errorf("put failed: %s\n", err.Error())
		}

		//NOTE: val2 is not nil, but a empty []byte{}
		val2, err := s.Get(key)
		if err != nil {
			t.Fatalf("get: %v\n", err)
		}

		if bytes.Compare(val, val2) != 0 {
			t.Fatalf("invalid value: [%v, %T %v]", val2, val2, val2 == nil)
		}

	})

	t.Run("Nget", func(t *testing.T) {
		for i := 0; i < 40; i++ {
			key := fmt.Sprintf("key%02d", i)
			expected := fmt.Sprintf("value%d", i)
			ikey := storage.KeyFromUser([]byte(key), sequence, storage.TagValue)
			sequence++

			v, err := s.Get(ikey)
			if err != nil {
				t.Errorf("get key(%s) failed: %s\n", string(ikey.Key()), err.Error())
				return
			}

			if string(v) != expected {
				t.Error("get recent key's value failed")
			}
		}
	})

	t.Run("2put2get", func(t *testing.T) {
		data := [][2]string{
			{"key203", "value203"},
			{"key204", "value204"},
		}
		for _, d := range data {

			ikey := storage.KeyFromUser([]byte(d[0]), sequence, storage.TagValue)
			sequence++

			err := s.Put(ikey, []byte(d[1]))
			if err != nil {
				t.Errorf("put key failed: %s\n", err.Error())
				return
			}
		}

		ikey := storage.KeyFromUser([]byte(data[1][0]), sequence, storage.TagValue)
		v, err := s.Get(ikey)
		if err != nil {
			t.Fatalf("get key failed: %s\n", err.Error())
		}

		if string(v) != data[1][1] {
			t.Errorf("get key4's value failed")
		}

		ikey = storage.KeyFromUser([]byte(data[0][0]), sequence, storage.TagValue)
		v, err = s.Get(ikey)
		if err != nil {
			t.Errorf("get key failed: %s\n", err.Error())
		}

		if string(v) != data[0][1] {
			t.Errorf("get key3's value failed")
		}
	})
}

func TestDiskStorage_Index(t *testing.T) {
	var segment = fmt.Sprintf("%s/segment", t.TempDir())

	var sequence uint64 = 1
	t.Run("prepare", func(t *testing.T) {
		s := NewDiskStorage(storage.Options{
			Args: map[string]interface{}{
				SegmentNameOpt:          segment,
				storage.SegmentOpenMode: storage.SegmentOpenModeRW,
			}})

		defer s.Close()

		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key%d", i)
			val := fmt.Sprintf("value%d", i)
			ikey := storage.KeyFromUser([]byte(key), sequence, storage.TagValue)
			sequence++

			err := s.Put(ikey, []byte(val))
			if err != nil {
				t.Errorf("put key failed: %s\n", err.Error())
				return
			}
		}
	})

	t.Run("indexed", func(t *testing.T) {
		s := NewDiskStorage(storage.Options{
			Args: map[string]interface{}{
				SegmentNameOpt:          segment,
				storage.SegmentOpenMode: storage.SegmentOpenModeRO,
			}})

		defer s.Close()

		data := [][]string{
			{"key1", "value1"},
			{"key7", "value7"},
			{"key4", "value4"},
			{"key2", "value2"},
		}

		for _, p := range data {
			ikey := storage.KeyFromUser([]byte(p[0]), sequence, storage.TagValue)

			v, err := s.Get(ikey)
			if err != nil {
				t.Errorf("get(%s) failed: %s\n", string(ikey.Key()), err.Error())
				return
			}

			if string(v) != p[1] {
				t.Error("get recent key's value failed")
			}
		}
	})
}

func TestDiskStorage_Scan(t *testing.T) {
	var segment = fmt.Sprintf("%s/segment", t.TempDir())

	t.Run("batchput", func(t *testing.T) {
		s := NewDiskStorage(storage.Options{
			Args: map[string]interface{}{
				SegmentNameOpt:          segment,
				storage.SegmentOpenMode: storage.SegmentOpenModeRW,
			}})

		defer s.Close()

		var sequence uint64 = 1
		for i := 0; i < 40; i++ {
			key := fmt.Sprintf("key%02d", i)

			var tag uint8 = storage.TagValue
			if i == 10 {
				tag = storage.TagTombstone
			}
			ikey := storage.KeyFromUser([]byte(key), sequence, tag)
			sequence++

			s.Put(ikey, []byte(fmt.Sprintf("value%d", i)))
		}
	})
}
