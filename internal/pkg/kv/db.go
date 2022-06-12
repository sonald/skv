package kv

import (
	"fmt"
	"io"
	"io/fs"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sonald/skv/internal/pkg/storage"
	bd "github.com/sonald/skv/internal/pkg/storage/backend/disk"
	_ "github.com/sonald/skv/internal/pkg/storage/backend/mem"
)

type dumpJob struct {
	s       storage.Storage
	segment string
}

type KVImpl struct {
	memtable storage.Storage
	// the memtable that is being written into sstable
	outgoings []dumpJob
	lock      sync.Mutex

	// storage root path on file system
	root               string
	dumpPolicy         int
	dumpSizeThreshold  int
	dumpCountThreshold int
	debug              bool

	segmentSeq  int
	keySequence uint64

	closed bool
	jobs   chan dumpJob
	wg     sync.WaitGroup
}

func (kv *KVImpl) Stats() {
	panic("implement me")
}

func (kv *KVImpl) buildSegmentName(seq int) string {
	return fmt.Sprintf("%s/segment%08d", kv.root, seq)
}

func (kv *KVImpl) buildSnapshotName() string {
	timestamp := time.Now().UnixMilli()
	ts := strconv.FormatInt(timestamp, 10)
	return fmt.Sprintf("%s/%s-snapshot", kv.root, ts)
}

func (kv *KVImpl) BuildInternalKey(userKey string, tag uint8) *storage.InternalKey {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	ikey := storage.KeyFromUser([]byte(userKey), kv.keySequence, tag)
	kv.keySequence++

	return ikey
}

//FIXME: when writing sstable, Get operation should find data from old memtable
func (kv *KVImpl) writeSSTable(j dumpJob) {
	if kv.closed {
		panic("kv closed")
	}

	start := time.Now()
	kv.wg.Add(1)
	kv.jobs <- j
	log.Printf("writeSSTable: cost %v, send job %s\n", time.Now().Sub(start), j.segment)
}

func (kv *KVImpl) newMemtable() dumpJob {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	// dump to sstable and clear
	old := kv.memtable
	job := dumpJob{old, kv.buildSegmentName(kv.segmentSeq)}

	kv.segmentSeq++
	kv.outgoings = append(kv.outgoings, job)
	kv.memtable = storage.GetStorage("mem", storage.Options{})

	return job
}

func (kv *KVImpl) write(key string, tag uint8, value []byte) error {
	var dump = false
	switch kv.dumpPolicy {
	case DumpByCount:
		dump = kv.memtable.Count() >= kv.dumpCountThreshold
	case DumpBySize:
		dump = kv.memtable.Size() >= kv.dumpSizeThreshold
	}

	if dump {
		//log.Printf("dump: policy %d, count: (threshold %d, %d)\n", kv.dumpPolicy, kv.dumpCountThreshold,
		//	kv.memtable.Count())
		old := kv.newMemtable()
		kv.writeSSTable(old)
	}

	ikey := kv.BuildInternalKey(key, tag)

	switch tag {
	case storage.TagValue:
		return kv.memtable.Put(ikey, value)
	case storage.TagTombstone:
		return kv.memtable.Del(ikey)
	}

	return nil
}

func (kv *KVImpl) Del(key string) error {
	return kv.write(key, storage.TagTombstone, nil)
}

func (kv *KVImpl) Put(key string, value []byte) error {
	return kv.write(key, storage.TagValue, value)
}

func (kv *KVImpl) fastGet(key *storage.InternalKey) ([]byte, error) {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	if val, err := kv.memtable.Get(key); err == storage.ErrNotFound {
		// go to next level
	} else {
		return val, err
	}

	log.Printf("fastGet: memtable failed, go to outgoings\n")
	if kv.outgoings != nil {
		for i := len(kv.outgoings) - 1; i >= 0; i-- {
			if val, err := kv.outgoings[i].s.Get(key); err == storage.ErrNotFound {
				// go to next
			} else {
				return val, err
			}
		}
	}

	return nil, storage.ErrNotFound
}

//TODO: conccurent read?
func (kv *KVImpl) Get(key string) ([]byte, error) {
	ikey := storage.KeyFromUser([]byte(key), math.MaxUint64, storage.TagValue)

	if val, err := kv.fastGet(ikey); err == storage.ErrNotFound {
		// go to disk
	} else {
		// err may be ErrKeyDeleted
		return val, err
	}

	for i := kv.segmentSeq - 1; i >= 0; i-- {
		// TODO: cache opened segments
		ds := storage.GetStorage("disk", storage.Options{
			Args: map[string]interface{}{
				bd.SegmentNameOpt:       kv.buildSegmentName(i),
				storage.SegmentOpenMode: storage.SegmentOpenModeRO,
			},
		})

		if kv.debug {
			log.Printf("Get: fallback to %s\n", kv.buildSegmentName(i))
		}

		if val, err := ds.Get(ikey); err == storage.ErrNotFound {
		} else {
			ds.Close()
			// err may be ErrKeyDeleted
			return val, err
		}
		ds.Close()
	}

	return nil, storage.ErrNotFound
}

func (kv *KVImpl) MakeSnapshot(w io.WriteCloser) error {
	ds := storage.GetStorage("disk", storage.Options{
		Args: map[string]interface{}{
			bd.SegmentNameOpt:        "snapshot",
			bd.DiskWriterOverrideOpt: w,
			storage.SegmentOpenMode:  storage.SegmentOpenModeWR,
		},
	})
	// no need to close
	//defer ds.Close()

	var sequence uint64 = 1
	kv.Scan(func(k string, v []byte) bool {
		newKey := storage.KeyFromUser([]byte(k), sequence, storage.TagValue)
		sequence++
		ds.Put(newKey, v)
		return true
	})

	return nil
}

func (kv *KVImpl) GetSnapshot(r io.ReadCloser) (storage.Storage, error) {
	ds := storage.GetStorage("disk", storage.Options{
		Args: map[string]interface{}{
			bd.SegmentNameOpt:        "snapshot",
			bd.DiskReaderOverrideOpt: r,
			storage.SegmentOpenMode:  storage.SegmentOpenModeRO,
		},
	})

	return ds, nil
}

// Scan
// FIXME: performance is bad
// this is like replaying all put operations
func (kv *KVImpl) Scan(f func(k string, v []byte) bool) {
	var sequence uint64 = 0

	ms := storage.GetStorage("mem", storage.Options{})
	defer ms.Close()

	for i := 0; i < kv.segmentSeq; i++ {
		func() {
			ds := storage.GetStorage("disk", storage.Options{
				Args: map[string]interface{}{
					bd.SegmentNameOpt:       kv.buildSegmentName(i),
					storage.SegmentOpenMode: storage.SegmentOpenModeRO,
				},
			})

			defer ds.Close()

			ds.Scan(func(k *storage.InternalKey, v []byte) bool {
				newKey := storage.KeyFromUser(k.Key(), sequence, uint8(k.Tag()))
				sequence++

				if k.Tag() == storage.TagTombstone {
					log.Printf("Scan: map tombstone to del(%s)\n", string(k.Key()))
					return ms.Del(newKey) == nil
				}
				return ms.Put(newKey, v) == nil
			})
		}()
	}

	if kv.memtable != nil {
		kv.memtable.Scan(func(k *storage.InternalKey, v []byte) bool {
			newKey := storage.KeyFromUser(k.Key(), sequence, uint8(k.Tag()))
			sequence++

			if k.Tag() == storage.TagTombstone {
				return ms.Del(newKey) == nil
			}

			return ms.Put(newKey, v) == nil
		})
	}

	ms.Scan(func(k *storage.InternalKey, v []byte) bool {
		if k.Tag() == storage.TagTombstone {
			return true
		}
		f(string(k.Key()), v)
		return true
	})
}

func nextUsableSequence(root string) int {
	var maxSeq = -1
	filepath.WalkDir(root, func(_ string, d fs.DirEntry, _ error) error {
		if !d.Type().IsRegular() {
			return nil
		}

		if !strings.HasPrefix(d.Name(), "segment") {
			return nil
		}
		var seq int
		_, err := fmt.Sscanf(d.Name(), "segment%d", &seq)
		if err != nil {
			log.Printf("walk: %s %s\n", d.Name(), err.Error())
		}

		if seq > maxSeq {
			maxSeq = seq
		}
		return nil
	})

	log.Printf("nextUsableSequence %d\n", maxSeq+1)

	return maxSeq + 1
}

func (kv *KVImpl) startJobManager() {
	for {
		select {
		case job, ok := <-kv.jobs:
			if !ok {
				break
			}

			log.Printf("process job for %s\n", job.segment)
			{
				ds := storage.GetStorage("disk", storage.Options{
					Args: map[string]interface{}{
						bd.SegmentNameOpt:       job.segment,
						storage.SegmentOpenMode: storage.SegmentOpenModeWR,
					},
				})

				job.s.Scan(func(k *storage.InternalKey, v []byte) bool {
					return ds.Put(k, v) == nil
				})

				ds.Close()
				job.s.Close()
			}

			{
				kv.lock.Lock()
				if kv.outgoings[0].s != job.s || kv.outgoings[0].segment != job.segment {
					log.Fatalf("mismatch job data\n")
				}
				kv.outgoings = kv.outgoings[1:]
				kv.lock.Unlock()
			}

			kv.wg.Done()
		}
	}

	log.Println("quit background job")
}

func NewKV(opts ...KVOption) KV {
	kv := &KVImpl{
		memtable:           storage.GetStorage("mem", storage.Options{}),
		dumpCountThreshold: 1024,
		dumpSizeThreshold:  storage.Megabyte * 10,
		dumpPolicy:         DumpByCount,
		root:               "/tmp/skv",
		jobs:               make(chan dumpJob),
		keySequence:        0,
	}

	for _, opt := range opts {
		opt(kv)
	}

	if kv.debug {
		log.Printf("%+v\n", kv)
	}

	err := os.MkdirAll(kv.root, 0755)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}

	kv.segmentSeq = nextUsableSequence(kv.root)
	go kv.startJobManager()

	return kv
}

func (kv *KVImpl) Close() {
	if kv.closed {
		return
	}
	log.Println("Closing...")
	if kv.memtable != nil {
		log.Println("write down memtable")
		old := kv.newMemtable()
		kv.writeSSTable(old)
		kv.memtable = nil
	}

	kv.wg.Wait()
	close(kv.jobs)
	kv.jobs = nil
	kv.closed = true
}
