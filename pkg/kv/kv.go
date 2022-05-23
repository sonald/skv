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
	lock     sync.Mutex

	// storage root path on file system
	Root string

	DumpPolicy         int
	DumpSizeThreshold  int
	DumpCountThreshold int

	segmentSeq int

	jobs chan dumpJob
	wg   sync.WaitGroup
}

func (kv *KV) buildSegmentName(seq int) string {
	return fmt.Sprintf("%s/segment%08d", kv.Root, seq)
}

func (kv *KV) writeSSTable(s storage.Storage) {
	kv.wg.Add(1)
	kv.jobs <- dumpJob{s, kv.buildSegmentName(kv.segmentSeq)}
	kv.segmentSeq++
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
		kv.lock.Lock()
		defer kv.lock.Unlock()

		// dump to sstable and clear
		mt := kv.memtable
		kv.memtable = storage.GetStorage("mem", storage.Options{})
		kv.writeSSTable(mt)
	}

	return kv.memtable.Put(key, value)
}

func (kv *KV) Get(key string) (string, error) {
	val, err := kv.memtable.Get(key)
	if err == nil {
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

		val, err = ds.Get(key)
		if err == nil {
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

func findSegmentSeq(root string) int {
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
	// overflow?
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

	kv.segmentSeq = findSegmentSeq(kv.Root)

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
				defer job.s.Close()

				{
					ds := storage.GetStorage("disk", storage.Options{
						Args: map[string]interface{}{
							bd.SegmentNameOpt:       job.segment,
							storage.SegmentOpenMode: storage.SegmentOpenModeWR,
						},
					})
					defer ds.Close()

					job.s.Scan(func(k, v string) bool {
						log.Printf("Scan(%s, %s)\n", k, v)
						return ds.Put(k, v) == nil
					})
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
		kv.writeSSTable(kv.memtable)
		kv.memtable = nil
	}

	kv.wg.Wait()
	close(kv.jobs)
}

func init() {
	fmt.Println("skv started")
}
