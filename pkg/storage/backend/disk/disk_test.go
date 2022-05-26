package disk

import (
	"fmt"
	"github.com/sonald/skv/pkg/storage"
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
		err := s.Put("key1", "value1")
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

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%02d", i)
		s.Put(key, fmt.Sprintf("value%d", i))
	}

	t.Run("get", func(t *testing.T) {
		err := s.Put("key1", "value1")
		if err != nil {
			t.Errorf("put key1 failed: %s\n", err.Error())
			return
		}

		v, err := s.Get("key1")
		if err != nil {
			t.Errorf("get key1 failed: %s\n", err.Error())
			return
		}

		if v != "value1" {
			t.Failed()
		}
	})

	t.Run("Nget", func(t *testing.T) {
		for i := 0; i < 40; i++ {
			key := fmt.Sprintf("key%02d", i)
			expected := fmt.Sprintf("value%d", i)

			v, err := s.Get(key)
			if err != nil {
				t.Errorf("get key failed: %s\n", err.Error())
				return
			}

			if v != expected {
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
			err := s.Put(d[0], d[1])
			if err != nil {
				t.Errorf("put key failed: %s\n", err.Error())
				return
			}
		}

		v, err := s.Get(data[1][0])
		if err != nil {
			t.Fatalf("get key failed: %s\n", err.Error())
		}

		if v != data[1][1] {
			t.Errorf("get key4's value failed")
		}

		v, err = s.Get(data[0][0])
		if err != nil {
			t.Errorf("get key failed: %s\n", err.Error())
		}

		if v != data[0][1] {
			t.Errorf("get key3's value failed")
		}
	})
}

func TestDiskStorage_Index(t *testing.T) {
	var segment = fmt.Sprintf("%s/segment", t.TempDir())

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
			err := s.Put(key, val)
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
			v, err := s.Get(p[0])
			if err != nil {
				t.Errorf("get key failed: %s\n", err.Error())
				return
			}

			if v != p[1] {
				t.Error("get recent key's value failed")
			}
		}
	})
}
