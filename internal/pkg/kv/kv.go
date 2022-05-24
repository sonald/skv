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

const (
	DumpBySize = iota
	DumpByCount
)

type dumpJob struct {
	s       storage.Storage
	segment string
}

type KV struct {
	memtable storage.Storage
	// the memtable that is being written into sstable
	outgoings []dumpJob
	lock      sync.Mutex

	// storage root path on file system
	Root string

	DumpPolicy         int
	DumpSizeThreshold  int
	DumpCountThreshold int

	segmentSeq int

	closed bool
	jobs   chan dumpJob
	wg     sync.WaitGroup
}

func (kv *KV) buildSegmentName(seq int) string {
	return fmt.Sprintf("%s/segment%08d", kv.Root, seq)
}

//FIXME: when writing sstable, Get operation should find data from old memtable
func (kv *KV) writeSSTable(j dumpJob) {
	if kv.closed {
		panic("kv closed")
	}

	kv.wg.Add(1)
	kv.jobs <- j
}

func (kv *KV) newMemtable() dumpJob {
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

func (kv *KV) Put(key string, value string) error {
	var dump = false
	switch kv.DumpPolicy {
	case DumpByCount:
		dump = kv.memtable.Count() >= kv.DumpCountThreshold
	case DumpBySize:
		dump = kv.memtable.Size() >= kv.DumpSizeThreshold
	}

	if dump {
		old := kv.newMemtable()
		kv.writeSSTable(old)

	}

	//log.Printf("Put(%s, %s)\n", key, value)
	return kv.memtable.Put(key, value)
}

func (kv *KV) fastGet(key string) (string, error) {
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
func (kv *KV) Get(key string) (string, error) {
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

func (kv *KV) Del(key string) error {
	return kv.memtable.Del(key)
}

//FIXME: not good
func (kv *KV) Scan(f func(k string, v string) bool) {
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

//TODO: inject params from cfg
func NewKV() *KV {
	kv := &KV{
		memtable:           storage.GetStorage("mem", storage.Options{}),
		DumpCountThreshold: 10,
		DumpSizeThreshold:  storage.Megabyte,
		DumpPolicy:         DumpByCount,
		Root:               "/tmp/skv",
		jobs:               make(chan dumpJob),
	}

	kv.segmentSeq = nextUsableSequence(kv.Root)

	var err error
	err = os.MkdirAll(kv.Root, 0755)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}

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

	return kv
}

func (kv *KV) Close() {
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

func init() {
	fmt.Println("skv started")
}
