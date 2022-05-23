package disk

import (
	"fmt"
	"github.com/sonald/skv/pkg/storage"
	"math/rand"
	"testing"
)

func TestPut(t *testing.T) {
	s := NewDiskStorage(storage.Options{
		Args: map[string]interface{}{
			SegmentNameOpt:          fmt.Sprintf("%s/segment", t.TempDir()),
			storage.SegmentOpenMode: storage.SegmentOpenModeWR,
		}})

	defer s.Close()

	r := rand.New(rand.NewSource(0xdeadbeef))

	t.Run("put", func(t *testing.T) {
		err := s.Put("key1", "value1")
		if err != nil {
			t.Errorf("put failed: %s\n", err.Error())
		}
	})

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%02d", r.Intn(40))
		s.Put(key, fmt.Sprintf("value%d", r.Int31()))
	}

}

func TestDiskStorage_Get(t *testing.T) {
	s := NewDiskStorage(storage.Options{
		Args: map[string]interface{}{
			SegmentNameOpt:          fmt.Sprintf("%s/segment", t.TempDir()),
			storage.SegmentOpenMode: storage.SegmentOpenModeRW,
		}})

	defer s.Close()

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

		if v != "key1" {
			t.Failed()
		}
	})

	t.Run("Nput1get", func(t *testing.T) {
		r := rand.New(rand.NewSource(0xdeadbeef))
		var lastVal string
		var key = "key2"
		var count = 40 // exceed threshold
		for i := 0; i < count; i++ {
			lastVal = fmt.Sprintf("value%04d", r.Int31())
			err := s.Put(key, lastVal)
			if err != nil {
				t.Errorf("put key failed: %s\n", err.Error())
				return
			}
		}

		v, err := s.Get(key)
		if err != nil {
			t.Errorf("get key failed: %s\n", err.Error())
			return
		}

		if v != lastVal {
			t.Error("get recent key's value failed")
		}
	})

	t.Run("NputMget", func(t *testing.T) {
		r := rand.New(rand.NewSource(0xdeadbeef))
		var lastVal string
		var key = "key2"
		var count = 40 // exceed threshold
		for i := 0; i < count; i++ {
			lastVal = fmt.Sprintf("value%04d", r.Int31())
			err := s.Put(key, lastVal)
			if err != nil {
				t.Errorf("put key failed: %s\n", err.Error())
				return
			}

			v, err := s.Get(key)
			if err != nil {
				t.Errorf("get key failed: %s\n", err.Error())
				return
			}

			if v != lastVal {
				t.Error("get recent key's value failed")
			}
			//t.Logf("lastVal: %s\n", lastVal)
		}
	})

	t.Run("2put2get", func(t *testing.T) {
		data := [][2]string{
			{"key3", "value3"},
			{"key4", "value4"},
		}
		for _, d := range data {
			err := s.Put(d[0], d[1])
			if err != nil {
				t.Errorf("put key failed: %s\n", err.Error())
				return
			}
		}

		v, err := s.Get("key4")
		if err != nil {
			t.Errorf("get key failed: %s\n", err.Error())
		}

		if v != "value4" {
			t.Errorf("get key4's value failed")
		}

		v, err = s.Get("key3")
		if err != nil {
			t.Errorf("get key failed: %s\n", err.Error())
		}

		if v != "value3" {
			t.Errorf("get key3's value failed")
		}
	})
}
