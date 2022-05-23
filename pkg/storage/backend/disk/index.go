package disk

import (
	"fmt"
	"github.com/huandu/skiplist"
	"github.com/sonald/skv/pkg/storage"
	"io"
	"log"
	"os"
	"strconv"
)

type Index struct {
	data *skiplist.SkipList

	// path of index file
	path string
	// corresponding segment path
	segment string
}

func (idx *Index) BuildIndex() {
	ds := storage.GetStorage("disk", storage.Options{
		Args: map[string]interface{}{
			SegmentNameOpt:          idx.segment,
			storage.SegmentOpenMode: storage.SegmentOpenModeRO,
		},
	})
	defer ds.Close()

	rd := ds.(*DiskStorage).r
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
	}
}

func (idx *Index) Load() {
	if idx.data.Len() > 0 {
		return
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
}

func (idx *Index) Save() {
	ds := storage.GetStorage("disk", storage.Options{
		Args: map[string]interface{}{
			SegmentNameOpt:          idx.path,
			storage.SegmentOpenMode: storage.SegmentOpenModeWR,
		},
	})
	defer ds.Close()

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

func NewIndexFor(segment string) *Index {
	idx := &Index{
		data:    skiplist.New(skiplist.String),
		path:    fmt.Sprintf("%s_index", segment),
		segment: segment,
	}

	_, err := os.Stat(idx.path)
	if err != nil {
		fmt.Println(err)
		idx.BuildIndex()
		idx.Save()

	} else {
		//load
		idx.Load()
	}

	return idx
}

func (idx *Index) Get(key string) (int64, error) {
	if elem := idx.data.Get(key); elem == nil {
		return 0, fmt.Errorf("key does not exist")
	} else {
		return elem.Value.(int64), nil
	}
}
