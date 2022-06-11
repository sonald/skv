package disk

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/huandu/skiplist"
	"github.com/sonald/skv/internal/pkg/storage"
	"github.com/sonald/skv/internal/pkg/storage/backend/noop"
	"github.com/sonald/skv/pkg/utils"
	"io"
	"log"
	"os"
	"strings"
)

func init() {
	storage.RegisterBackend("disk", NewDiskStorage)
}

type DiskStorage struct {
	segment string
	w       io.WriteCloser
	r       io.ReadCloser
	index   Index
	// only used when opened read
	filter    Filter
	transient bool
}

func NewDiskStorage(options storage.Options) storage.Storage {
	ds := &DiskStorage{
		segment: options.Args[SegmentNameOpt].(string),
	}

	mode, ok := options.Args[storage.SegmentOpenMode]
	if !ok {
		mode = storage.SegmentOpenModeWR
	}

	var err error
	switch mode {
	case storage.SegmentOpenModeRO:
		if opt, ok := options.Args[DiskReaderOverrideOpt]; ok {
			ds.r = opt.(io.ReadCloser)
			ds.transient = true
		} else {
			log.Printf("open disk storage %s\n", ds.segment)
			ds.r, err = os.Open(ds.segment)
		}

	case storage.SegmentOpenModeWR:
		if opt, ok := options.Args[DiskWriterOverrideOpt]; ok {
			log.Printf("override disk storage writer\n")
			ds.w = opt.(io.WriteCloser)
			ds.transient = true
		} else {
			log.Printf("create disk storage %s\n", ds.segment)
			ds.w, err = os.OpenFile(ds.segment, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
		}

	case storage.SegmentOpenModeRW:
		log.Printf("create disk storage rw %s\n", ds.segment)
		ds.w, err = os.OpenFile(ds.segment, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
		if err == nil {
			ds.r, err = os.Open(ds.segment)
		}
	}

	if err != nil {
		log.Println(err.Error())
		return &noop.NoopStorage{}
	}

	if ds.r != nil && !ds.transient {
		ds.filter = NewBloomFilter(ds)
	}

	return ds
}

func (ds *DiskStorage) IsIndexFile() bool {
	return strings.HasSuffix(ds.segment, "_index")
}

func (ds *DiskStorage) LoadIndex() {
	if ds.r != nil && !ds.transient && ds.index == nil && !ds.IsIndexFile() {
		ds.index = LoadIndex(ds.segment)
	}
}

func (ds *DiskStorage) Put(key *storage.InternalKey, value []byte) error {
	if ds.filter != nil {
		ds.filter.Set(key)
	}

	w := bufio.NewWriter(ds.w)
	ew := utils.NewErrWriter(w)

	data, err := storage.LengthPrefixed(value)
	if err != nil {
		return err
	}
	ew.Write(key.Encode())
	ew.Write(data)
	if err != nil {
		return ew.Err()
	}
	return w.Flush()
}

func (ds *DiskStorage) Get(key *storage.InternalKey) ([]byte, error) {
	var err error
	var offset int64 = 0

	if ds.filter != nil {
		if !ds.filter.Check(key) {
			return nil, storage.ErrNotFound
		}
	}

	seeker, ok := ds.r.(io.Seeker)
	if ok {
		// Lazy loading
		ds.LoadIndex()

		if ds.index != nil {
			if offset, err = ds.index.GetOffset(key); err != nil {
				log.Println(err)
				offset = 0
			}
			log.Printf("fast offset(%d) by index", offset)
		}

		_, err = seeker.Seek(offset, io.SeekStart)
		if err != nil {
			return nil, err
		}
	} else {
		log.Println("Get: r is not a seeker")
	}

	br := bufio.NewReader(ds.r)
	for {
		keyRead, err := storage.ReadInternalKey(br)
		if err == io.EOF {
			break
		}

		//TODO: check if key is deleted
		//log.Printf("read key: [%s]\n", string(keyRead.Key()))
		if keyRead.Equal(key) {
			return storage.ReadLengthPrefixed(br)
		} else {
			err = storage.DiscardLengthPrefixedValue(br)
		}

		if err != nil {
			return nil, err
		}
	}

	return nil, storage.ErrNotFound
}

func (ds *DiskStorage) Del(key *storage.InternalKey) error {
	log.Fatalln("disk should never delete a key")
	return nil
}

func (ds *DiskStorage) writeIndex() {
	if ds.IsIndexFile() {
		return
	}
	r, err := os.Open(ds.segment)
	if err != nil {
		log.Println(err)
		return
	}

	idx := &DiskStorageIndex{
		data: skiplist.New(skiplist.GreaterThanFunc(storage.GreaterThan)),
		path: fmt.Sprintf("%s_index", ds.segment),
	}

	_, err = os.Stat(idx.path)
	if err != nil {
		fmt.Println(err)
		idx.BuildIndex(r)
		idx.Save()
	}
}

func (ds *DiskStorage) Close() {
	if ds.r != nil {
		ds.r.Close()
	}

	if ds.w != nil {
		ds.w.Close()
		ds.writeIndex()
	}

}

func (ds *DiskStorage) Size() int {
	return 0
}

func (ds *DiskStorage) Count() int {
	return 0
}

func (ds *DiskStorage) Scan(f func(k *storage.InternalKey, v []byte) bool) {
	seeker, ok := ds.r.(io.Seeker)
	if ok {
		seeker.Seek(0, io.SeekStart)
	} else {
		log.Println("Scan: r is not a seeker")
	}

	br := bufio.NewReader(ds.r)

	var user_key []byte
	for {
		key, err := storage.ReadInternalKey(br)
		if err == io.EOF {
			break
		}

		val, err := storage.ReadLengthPrefixed(br)
		if err != nil {
			break
		}

		if !bytes.Equal(user_key, key.Key()) {
			if !f(key, val) {
				break
			}
		}

		user_key = key.Key()
	}
}
