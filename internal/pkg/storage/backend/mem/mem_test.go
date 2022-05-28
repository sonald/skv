package mem

import (
	"bytes"
	"fmt"
	"github.com/sonald/skv/internal/pkg/storage"
	"log"
	"testing"
)

func TestMemStorage(t *testing.T) {
	ms := storage.GetStorage("mem", storage.Options{})
	defer ms.Close()

	t.Run("put", func(t *testing.T) {
		ikey := storage.KeyFromUser([]byte("key1"), 0, storage.TagValue)
		val := []byte("value1")
		err := ms.Put(ikey, val)
		if err != nil {
			t.Fatalf("put: %v\n", err)
		}

		val2, err := ms.Get(ikey)
		if err != nil {
			t.Fatalf("get: %v\n", err)
		}

		if bytes.Compare(val, val2) != 0 {
			t.Fatalf("invalid value")
		}
	})

	t.Run("put0", func(t *testing.T) {
		ikey := storage.KeyFromUser([]byte("key1"), 0, storage.TagValue)
		var val []byte
		err := ms.Put(ikey, val)
		if err != nil {
			t.Fatalf("put: %v\n", err)
		}

		val2, err := ms.Get(ikey)
		if err != nil {
			t.Fatalf("get: %v\n", err)
		}

		if bytes.Compare(val, val2) != 0 {
			t.Fatalf("invalid value")
		}
	})

	t.Run("del", func(t *testing.T) {
		ikey := storage.KeyFromUser([]byte("key2"), 0, storage.TagValue)
		var val = []byte("value")
		err := ms.Put(ikey, val)
		if err != nil {
			t.Fatalf("put: %v\n", err)
		}

		// use the same seq, we get empty slice
		// seq should be greater than last one
		ikey2 := storage.KeyFromUser(ikey.Key(), 1, storage.TagTombstone)
		err = ms.Del(ikey2)
		if err != nil {
			t.Fatalf("del failed")
		}

		val2, err := ms.Get(ikey2)
		if err == nil {
			t.Fatalf("key gets deleted")
		}

		if bytes.Compare(val, val2) == 0 {
			t.Fatalf("invalid value: %v", val2)
		}

		ikey3 := storage.KeyFromUser(ikey.Key(), 1, storage.TagTombstone)
		val2, err = ms.Get(ikey3)
		if err != storage.ErrKeyDeleted {
			t.Fatalf("key should not be found: val %s, err %v", string(val2), err)
		}
	})

	t.Run("del2", func(t *testing.T) {
		target := []byte("key_del2")
		ikey := storage.KeyFromUser(target, 0, storage.TagValue)
		var val = []byte("value")
		err := ms.Put(ikey, val)
		if err != nil {
			t.Fatalf("put: %v\n", err)
		}

		// use the same seq, we get empty slice
		// seq should be greater than last one
		ikey2 := storage.KeyFromUser(ikey.Key(), 1, storage.TagTombstone)
		err = ms.Del(ikey2)
		if err != nil {
			t.Fatalf("del failed")
		}

		val2, err := ms.Get(ikey2)
		if err == nil {
			t.Fatalf("key gets deleted")
		}

		ikey2 = storage.KeyFromUser(ikey.Key(), 2, storage.TagTombstone)
		err = ms.Del(ikey2)
		if err != nil {
			t.Fatalf("del failed")
		}

		ms.Scan(func(k *storage.InternalKey, v []byte) bool {
			log.Printf(" %s\t\t- %s\n", k.ToString(), string(v))
			return true
		})

		ikey3 := storage.KeyFromUser(ikey.Key(), 2, storage.TagTombstone)
		val2, err = ms.Get(ikey3)
		if err != storage.ErrKeyDeleted {
			t.Fatalf("key should not be found: val %s, err %v", string(val2), err)
		}
	})

	t.Run("directdel", func(t *testing.T) {
		target := []byte("key_directdel")
		ikey2 := storage.KeyFromUser(target, 1, storage.TagTombstone)
		err := ms.Del(ikey2)
		if err != nil {
			t.Fatalf("del failed: %v", err)
		}
	})

	t.Run("scan", func(t *testing.T) {
		var seq uint64 = 0
		for i := 0; i < 10; i++ {
			ikey := storage.KeyFromUser([]byte(fmt.Sprintf("key3%d", i)), seq, storage.TagValue)
			val := []byte(fmt.Sprintf("value3%d", i))
			seq++

			err := ms.Put(ikey, val)
			if err != nil {
				t.Fatalf("put: %v\n", err)
			}
		}

		ikey := storage.KeyFromUser([]byte("key32"), seq, storage.TagTombstone)
		seq++
		ms.Del(ikey)

		var count = 0
		ms.Scan(func(k *storage.InternalKey, v []byte) bool {
			if bytes.Compare(k.Key(), []byte("key32")) == 0 {
				count++
				if count == 2 && k.Tag() != storage.TagValue {
					t.Fatalf("first key's tag should be TagValue")
				}
				if count == 1 && k.Tag() != storage.TagTombstone {
					t.Fatalf("second key's tag should be TagTombstone: %s", k.ToString())
				}
			}
			log.Printf("#%d: %s\t\t- %s\n", count, k.ToString(), string(v))

			return true
		})

		if count != 2 {
			t.Fatalf("del failed")
		}

		ikey = storage.KeyFromUser([]byte("key32"), seq, storage.TagTombstone)
		_, err := ms.Get(ikey)
		if err != storage.ErrKeyDeleted {
			t.Fatalf("should be deleted")
		}
	})

	// put, del, put ops
	t.Run("PDP", func(t *testing.T) {
		var seq uint64 = 0
		for i := 0; i < 4; i++ {
			ikey := storage.KeyFromUser([]byte(fmt.Sprintf("key4%d", i)), seq, storage.TagValue)
			val := []byte(fmt.Sprintf("value4%d", i))
			seq++

			err := ms.Put(ikey, val)
			if err != nil {
				t.Fatalf("put: %v\n", err)
			}
		}

		target := []byte("key42")
		ikey := storage.KeyFromUser(target, seq, storage.TagTombstone)
		seq++
		ms.Del(ikey)

		ikey = storage.KeyFromUser(target, seq, storage.TagValue)
		val := []byte(fmt.Sprintf("value4%d", 9))
		seq++

		err := ms.Put(ikey, val)
		if err != nil {
			t.Fatalf("put: %v\n", err)
		}

		var count = 0
		ms.Scan(func(k *storage.InternalKey, v []byte) bool {
			if bytes.Compare(k.Key(), []byte("key42")) == 0 {
				count++
				if count == 1 && k.Tag() != storage.TagValue {
					t.Fatalf("first key's tag should be TagValue")
				}
				if count == 2 && k.Tag() != storage.TagTombstone {
					t.Fatalf("second key's tag should be TagTombstone: %s", k.ToString())
				}
			}
			log.Printf("#%d: %s\t\t- %s\n", count, k.ToString(), string(v))

			return true
		})

		ikey = storage.KeyFromUser(target, seq, storage.TagValue)
		val, err = ms.Get(ikey)
		if err != nil {
			t.Fatalf("should be found")
		}
		if bytes.Compare(val, []byte("value49")) != 0 {
			t.Fatalf("wrong value %s", string(val))
		}
	})
}
