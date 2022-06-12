package kv

import (
	"bytes"
	"fmt"
	"github.com/sonald/skv/internal/pkg/storage"
	"log"
	"math/rand"
	"testing"
)

func TestKV_RandomPut(t *testing.T) {
	var skopts = []KVOption{
		WithRoot(t.TempDir()),
		WithDumpPolicy(DumpByCount),
		WithDumpCountThreshold(10),
		WithDumpSizeThreshold(1000),
	}
	db := NewKV(skopts...)
	defer db.Close()

	r := rand.New(rand.NewSource(0xdeadbeef))

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%02d", r.Intn(40))
		db.Put(key, []byte(fmt.Sprintf("value%d", r.Int31())))
	}
}

func TestKV_Get(t *testing.T) {
	var skopts = []KVOption{
		WithRoot(t.TempDir()),
		WithDumpPolicy(DumpByCount),
		WithDumpCountThreshold(10),
		WithDumpSizeThreshold(1000),
	}
	db := NewKV(skopts...)
	defer db.Close()

	r := rand.New(rand.NewSource(0xdeadbeef))

	key99val := []byte(fmt.Sprintf("value%4d", r.Intn(1000)))
	db.Put("key99", key99val)

	t.Run("batchput", func(t *testing.T) {
		for i := 0; i < 30; i++ {
			key := fmt.Sprintf("key%02d", r.Intn(40))
			db.Put(key, []byte(fmt.Sprintf("value%d", r.Int31())))

			if i == 12 {
				if v, err := db.Get("key99"); err != nil {
					t.Fatalf("get key99 failed: %s\n", err.Error())
				} else {
					if bytes.Compare(v, key99val) != 0 {
						t.Fatalf("get wrong value of key99\n")
					}
				}
			}
		}
	})

	t.Run("get", func(t *testing.T) {
		target := []byte("value1")
		err := db.Put("key1", target)
		if err != nil {
			t.Errorf("put key1 failed: %s\n", err.Error())
			return
		}

		v, err := db.Get("key1")
		if err != nil {
			t.Errorf("get key1 failed: %s\n", err.Error())
			return
		}

		if bytes.Compare(v, target) != 0 {
			t.Fatalf("wrong value")
		}
	})

	t.Run("Nput1get", func(t *testing.T) {
		r := rand.New(rand.NewSource(0xdeadbeef))
		var lastVal []byte
		var key = "key2"
		var count = 40 // exceed threshold
		for i := 0; i < count; i++ {
			lastVal = []byte(fmt.Sprintf("value%04d", r.Int31()))
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

		if bytes.Compare(v, lastVal) != 0 {
			t.Error("get recent key's value failed")
		}
	})

	t.Run("NputMget", func(t *testing.T) {
		r := rand.New(rand.NewSource(0xdeadbeef))
		var lastVal []byte
		var key = "key2"
		var count = 40 // exceed threshold
		for i := 0; i < count; i++ {
			lastVal = []byte(fmt.Sprintf("value%04d", r.Int31()))
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

			if bytes.Compare(v, lastVal) != 0 {
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
			err := db.Put(d[0], []byte(d[1]))
			if err != nil {
				t.Errorf("put key failed: %s\n", err.Error())
				return
			}
		}

		v, err := db.Get("key4")
		if err != nil {
			t.Errorf("get key failed: %s\n", err.Error())
		}

		if string(v) != "value4" {
			t.Errorf("get key4's value failed")
		}

		v, err = db.Get("key3")
		if err != nil {
			t.Errorf("get key failed: %s\n", err.Error())
		}

		if string(v) != "value3" {
			t.Errorf("get key3's value failed")
		}
	})

	t.Run("oldest", func(t *testing.T) {
		v, err := db.Get("key99")
		if err != nil {
			t.Errorf("get key failed: %s\n", err.Error())
		}

		if bytes.Compare(v, key99val) != 0 {
			t.Errorf("get key99's value failed")
		}
	})
}

func TestKV_BloomFilter(t *testing.T) {
	var root = t.TempDir()

	t.Run("batchput", func(t *testing.T) {
		var skopts = []KVOption{
			WithRoot(root),
			WithDumpPolicy(DumpByCount),
			WithDumpCountThreshold(10),
			WithDumpSizeThreshold(1000),
		}
		db := NewKV(skopts...)

		defer db.Close()

		for i := 0; i < 40; i++ {
			key := fmt.Sprintf("key%02d", i)
			db.Put(key, []byte(fmt.Sprintf("value%d", i)))
		}
	})

	t.Run("batchget", func(t *testing.T) {
		var skopts = []KVOption{
			WithRoot(root),
			WithDumpPolicy(DumpByCount),
			WithDumpCountThreshold(10),
			WithDumpSizeThreshold(1000),
		}
		db := NewKV(skopts...)
		defer db.Close()

		for i := 0; i < 40; i++ {
			key := fmt.Sprintf("key%02d", i)
			v, err := db.Get(key)
			if err != nil {
				t.Fatalf("get(%s) failed\n", key)
			}

			if string(v) != fmt.Sprintf("value%d", i) {
				t.Fatalf("get(%s) wrong value\n", key)
			}
		}
	})

}

func TestKV_Del(t *testing.T) {
	var skopts = []KVOption{
		WithRoot(t.TempDir()),
		WithDumpPolicy(DumpByCount),
		WithDumpCountThreshold(10),
		WithDumpSizeThreshold(1000),
	}
	db := NewKV(skopts...)
	defer db.Close()

	r := rand.New(rand.NewSource(0xdeadbeef))

	key99val := []byte(fmt.Sprintf("value%04d", r.Intn(1000)))
	db.Put("key99", key99val)

	t.Run("batchput", func(t *testing.T) {
		for i := 0; i < 30; i++ {
			key := fmt.Sprintf("key%02d", r.Intn(40))
			db.Put(key, []byte(fmt.Sprintf("value%d", r.Int31())))

			if i == 12 {
				if v, err := db.Get("key99"); err != nil {
					t.Fatalf("get key99 failed: %s\n", err.Error())
				} else {
					if !bytes.Equal(v, key99val) {
						t.Fatalf("get wrong value of key99\n")
					}
				}
			}
		}
	})

	t.Run("del", func(t *testing.T) {
		key := "key1"
		err := db.Put(key, []byte("value1"))
		if err != nil {
			t.Fatalf("put key1 failed: %s\n", err.Error())
		}

		err = db.Del(key)
		if err != nil {
			t.Fatalf("del key1 failed: %s\n", err.Error())
		}

		//db.Scan(func(k string, v []byte) bool {
		//	log.Printf(" %s\t\t- %s\n", k, string(v))
		//	return true
		//})

		_, err = db.Get(key)
		if err != storage.ErrKeyDeleted {
			t.Fatalf("get key1 failed: %s\n", err.Error())
		}
	})

	t.Run("del2", func(t *testing.T) {
		target := "key_del2"
		var val = []byte("value")
		err := db.Put(target, val)
		if err != nil {
			t.Fatalf("put: %v\n", err)
		}

		err = db.Del(target)
		if err != nil {
			t.Fatalf("del failed")
		}

		val2, err := db.Get(target)
		if err == nil {
			t.Fatalf("key gets deleted")
		}

		err = db.Del(target)
		if err != nil {
			t.Fatalf("del failed")
		}

		//db.Scan(func(k string, v []byte) bool {
		//	log.Printf(" %s\t\t- %s\n", k, string(v))
		//	return true
		//})

		val2, err = db.Get(target)
		if err != storage.ErrKeyDeleted {
			t.Fatalf("key should not be found: val %s, err %v", string(val2), err)
		}
	})

	t.Run("oldest", func(t *testing.T) {
		v, err := db.Get("key99")
		if err != nil {
			t.Errorf("get key failed: %s\n", err.Error())
		}

		if bytes.Compare(v, key99val) != 0 {
			t.Errorf("get key99's value failed")
		}
	})
}

func TestKV_DirectDel(t *testing.T) {
	var skopts = []KVOption{
		WithRoot(t.TempDir()),
		WithDumpPolicy(DumpByCount),
		WithDumpCountThreshold(10),
		WithDumpSizeThreshold(1000),
	}

	r := rand.New(rand.NewSource(0xdeadbeef))
	key99val := []byte(fmt.Sprintf("value%04d", r.Intn(1000)))

	t.Run("batchput", func(t *testing.T) {
		db := NewKV(skopts...)
		defer db.Close()

		db.Put("key99", key99val)
		for i := 0; i < 30; i++ {
			key := fmt.Sprintf("key%02d", r.Intn(40))
			db.Put(key, []byte(fmt.Sprintf("value%d", r.Int31())))
		}
	})

	t.Run("del without put", func(t *testing.T) {
		db := NewKV(skopts...)
		defer db.Close()

		err := db.Del("key99")
		if err != nil {
			t.Fatalf("del failed: %v", err)
		}

		val, err := db.Get("key99")
		if err != nil && err != storage.ErrKeyDeleted {
			t.Fatalf("get failed with wrong err: %v", err)
		}

		if bytes.Equal(val, key99val) {
			t.Fatalf("should not get deleted value\n")
		}
	})

	t.Run("get deleted", func(t *testing.T) {
		db := NewKV(skopts...)
		defer db.Close()

		db.Scan(func(k string, v []byte) bool {
			log.Printf("\t%s - %s", k, string(v))
			return true
		})

		val, err := db.Get("key99")
		if err != nil {
			t.Fatalf("get failed: %v", err)
		}

		if bytes.Equal(val, key99val) {
			t.Fatalf("should not get deleted value\n")
		}
	})
}

func TestKV_MultiDel(t *testing.T) {
	var skopts = []KVOption{
		WithRoot(t.TempDir()),
		WithDumpPolicy(DumpByCount),
		WithDumpCountThreshold(10),
		WithDumpSizeThreshold(1000),
	}

	r := rand.New(rand.NewSource(0xdeadbeef))
	key99val := []byte(fmt.Sprintf("value%04d", r.Intn(1000)))

	t.Run("batchput", func(t *testing.T) {
		db := NewKV(skopts...)
		defer db.Close()

		db.Put("key99", []byte("foobar"))
		db.Del("key99")
		db.Put("key99", []byte("world"))
		db.Put("key99", []byte("hello"))
		db.Put("key99", key99val)

		for i := 0; i < 30; i++ {
			key := fmt.Sprintf("key%02d", r.Intn(40))
			db.Put(key, []byte(fmt.Sprintf("value%d", r.Int31())))
		}
	})

	t.Run("get deleted", func(t *testing.T) {
		db := NewKV(skopts...)
		defer db.Close()

		val, err := db.Get("key99")
		if err != nil {
			t.Fatalf("get failed: %v", err)
		}

		if !bytes.Equal(val, key99val) {
			t.Fatalf("wrong key99 value: %s\n", string(val))
		}

		db.Scan(func(k string, v []byte) bool {
			log.Printf("\t%s - %s", k, string(v))
			return true
		})
		log.Printf("val: %v", val)
	})
}
