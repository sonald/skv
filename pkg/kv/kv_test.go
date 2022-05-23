package kv

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestKV_RandomPut(t *testing.T) {
	db := NewKV()
	defer db.Close()

	r := rand.New(rand.NewSource(0xdeadbeef))

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%02d", r.Intn(40))
		db.Put(key, fmt.Sprintf("value%d", r.Int31()))
	}
}

func TestKV_Get(t *testing.T) {
	db := NewKV()
	defer db.Close()

	r := rand.New(rand.NewSource(0xdeadbeef))

	db.Put("key99", "value99")

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%02d", r.Intn(40))
		db.Put(key, fmt.Sprintf("value%d", r.Int31()))
	}

	t.Run("get", func(t *testing.T) {
		err := db.Put("key1", "value1")
		if err != nil {
			t.Errorf("put key1 failed: %s\n", err.Error())
			return
		}

		v, err := db.Get("key1")
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
			err := db.Put(key, lastVal)
			if err != nil {
				t.Errorf("put key failed: %s\n", err.Error())
				return
			}
		}

		v, err := db.Get(key)
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
			err := db.Put(key, lastVal)
			if err != nil {
				t.Errorf("put key failed: %s\n", err.Error())
				return
			}

			v, err := db.Get(key)
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
			err := db.Put(d[0], d[1])
			if err != nil {
				t.Errorf("put key failed: %s\n", err.Error())
				return
			}
		}

		v, err := db.Get("key4")
		if err != nil {
			t.Errorf("get key failed: %s\n", err.Error())
		}

		if v != "value4" {
			t.Errorf("get key4's value failed")
		}

		v, err = db.Get("key3")
		if err != nil {
			t.Errorf("get key failed: %s\n", err.Error())
		}

		if v != "value3" {
			t.Errorf("get key3's value failed")
		}
	})

	t.Run("oldest", func(t *testing.T) {
		v, err := db.Get("key99")
		if err != nil {
			t.Errorf("get key failed: %s\n", err.Error())
		}

		if v != "value99" {
			t.Errorf("get key99's value failed")
		}
	})
}
