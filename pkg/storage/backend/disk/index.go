package disk

import (
	"fmt"
	"github.com/huandu/skiplist"
	"github.com/sonald/skv/pkg/storage"
	"io"
	"log"
	"strconv"
)

type Index interface {
	// return file offset from key
	GetOffset(key string) (int64, error)
}

type DiskStorageIndex struct {
	data *skiplist.SkipList

	// path of index file
	path string
	// corresponding segment path
	segment string
}

func (idx *DiskStorageIndex) GetOffset(key string) (int64, error) {
	if elem := idx.data.Get(key); elem == nil {
		return 0, fmt.Errorf("key does not exist")
	} else {
		return elem.Value.(int64), nil
	}
}

func (idx *DiskStorageIndex) Save() {
	ds := storage.GetStorage("disk", storage.Options{
		Args: map[string]interface{}{
			SegmentNameOpt:          idx.path,
			storage.SegmentOpenMode: storage.SegmentOpenModeWR,
		},
	})
	defer ds.Close()

	log.Printf("index.save %s\n", idx.path)
	elem := idx.data.Front()
	for elem != nil {
		val := strconv.FormatInt(elem.Value.(int64), 10)
		if err := ds.Put(elem.Key().(string), val); err != nil {
			log.Printf("put: %s\n", err.Error())
			break
		}

		elem = elem.Next()
	}
}

func (idx *DiskStorageIndex) BuildIndex(rd io.ReadSeeker) {
	rd.Seek(0, io.SeekStart)

	for {
		pos, _ := rd.Seek(0, io.SeekCurrent)
		key, err := readSizedValue(rd)
		if err == io.EOF {
			break
		}

		err = skipSizedValue(rd)
		if err != nil {
			break
		}

		idx.data.Set(key, pos)
		//log.Printf("DiskStorageIndex(%s, %v)\n", key, pos)
	}
}

func LoadIndex(segment string) Index {
	idx := &DiskStorageIndex{
		data: skiplist.New(skiplist.String),
		path: fmt.Sprintf("%s_index", segment),
	}

	ds := storage.GetStorage("disk", storage.Options{
		Args: map[string]interface{}{
			SegmentNameOpt:          idx.path,
			storage.SegmentOpenMode: storage.SegmentOpenModeRO,
		},
	})
	defer ds.Close()

	ds.Scan(func(k, v string) bool {
		pos, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return false
		}

		idx.data.Set(k, pos)
		return true
	})

	return idx
}
