package kv

import (
	"fmt"
	"github.com/sonald/skv/pkg/storage"
	bd "github.com/sonald/skv/pkg/storage/backend/disk"
	_ "github.com/sonald/skv/pkg/storage/backend/mem"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

	segmentSeq int

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

//FIXME: when writing sstable, Get operation should find data from old memtable
func (kv *KVImpl) writeSSTable(j dumpJob) {
	if kv.closed {
		panic("kv closed")
	}

	kv.wg.Add(1)
	kv.jobs <- j
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

func (kv *KVImpl) Put(key string, value string) error {
	var dump = false
	switch kv.dumpPolicy {
	case DumpByCount:
		dump = kv.memtable.Count() >= kv.dumpCountThreshold
	case DumpBySize:
		dump = kv.memtable.Size() >= kv.dumpSizeThreshold
	}

	if dump {
		old := kv.newMemtable()
		kv.writeSSTable(old)

	}

	//log.Printf("Put(%s, %s)\n", key, value)
	return kv.memtable.Put(key, value)
}

func (kv *KVImpl) fastGet(key string) (string, error) {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	if val, err := kv.memtable.Get(key); err == nil {
		return val, err
	}

	if kv.outgoings != nil {
		for i := len(kv.outgoings) - 1; i >= 0; i++ {
			if val, err := kv.outgoings[i].s.Get(key); err == nil {
				return val, err
			}
		}
	}

	return "", storage.ErrNotFound
}

//TODO: use bloom filter
//TODO: conccurent read?
func (kv *KVImpl) Get(key string) (string, error) {
	if val, err := kv.fastGet(key); err == nil {
		return val, err
	}

	for i := kv.segmentSeq - 1; i >= 0; i-- {
		ds := storage.GetStorage("disk", storage.Options{
			Args: map[string]interface{}{
				bd.SegmentNameOpt:       kv.buildSegmentName(i),
				storage.SegmentOpenMode: storage.SegmentOpenModeRO,
			},
		})

		defer ds.Close()
		log.Printf("Get: fallback to %s\n", kv.buildSegmentName(i))

		if val, err := ds.Get(key); err == nil {
			return val, err
		}
	}

	return "", storage.ErrNotFound
}

func (kv *KVImpl) Del(key string) error {
	return kv.memtable.Del(key)
}

//FIXME: not good
func (kv *KVImpl) Scan(f func(k string, v string) bool) {
	ms := storage.GetStorage("mem", storage.Options{})
	defer ms.Close()

	for i := 0; i < kv.segmentSeq; i++ {
		ds := storage.GetStorage("disk", storage.Options{
			Args: map[string]interface{}{
				bd.SegmentNameOpt:       kv.buildSegmentName(i),
				storage.SegmentOpenMode: storage.SegmentOpenModeRO,
			},
		})

		defer ds.Close()

		ds.Scan(func(k, v string) bool {
			return ms.Put(k, v) == nil
		})
	}

	if kv.memtable != nil {
		kv.memtable.Scan(func(k, v string) bool {
			return ms.Put(k, v) == nil
		})
	}

	ms.Scan(f)
}

func nextUsableSequence(root string) int {
	var maxSeq = 0
	filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if !d.Type().IsRegular() {
			return nil
		}

		if !strings.HasPrefix(d.Name(), "segment") {
			return nil
		}
		var seq int
		_, err = fmt.Sscanf(d.Name(), "segment%d", &seq)
		if err != nil {
			log.Printf("walk: %s %s\n", d.Name(), err.Error())
		}

		if seq > maxSeq {
			maxSeq = seq
		}
		return nil
	})

	log.Printf("maxSeq = %d\n", maxSeq)

	return maxSeq + 1
}

func (kv *KVImpl) startJobManager() {

	go func() {
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

					job.s.Scan(func(k, v string) bool {
						//log.Printf("Job: ScanPut (%s, %s)\n", k, v)
						// for testing only
						//time.Sleep(time.Millisecond * 200)
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
	}()
}

//TODO: inject params from cfg
func NewKV(opts ...KVOption) KV {
	kv := &KVImpl{
		memtable:           storage.GetStorage("mem", storage.Options{}),
		dumpCountThreshold: 10,
		dumpSizeThreshold:  storage.Megabyte,
		dumpPolicy:         DumpByCount,
		root:               "/tmp/skv",
		jobs:               make(chan dumpJob),
	}

	kv.segmentSeq = nextUsableSequence(kv.root)

	var err error
	err = os.MkdirAll(kv.root, 0755)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}

	kv.startJobManager()

	//TODO: add option
	return NewFilter(kv)
}

func (kv *KVImpl) Close() {
	if kv.memtable != nil {
		old := kv.newMemtable()
		kv.writeSSTable(old)
		kv.memtable = nil
	}

	kv.wg.Wait()
	close(kv.jobs)
	kv.jobs = nil
	kv.closed = true
}
