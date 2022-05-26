package disk

import (
	"bufio"
	"bytes"
	"encoding/binary"
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
	log.Println("disk backend init")
	storage.RegisterBackend("disk", NewDiskStorage)
}

type DiskStorage struct {
	segment string
	w       io.WriteCloser
	r       io.ReadSeekCloser
	index   Index
	// only used when opened read
	filter Filter
}

const (
	SegmentNameOpt = "name"
)

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
		log.Printf("open disk storage %s\n", ds.segment)
		ds.r, err = os.Open(ds.segment)

	case storage.SegmentOpenModeWR:
		log.Printf("create disk storage %s\n", ds.segment)
		ds.w, err = os.OpenFile(ds.segment, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)

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

	//TODO: set by option
	if ds.r != nil {
		ds.filter = NewBloomFilter(ds)
	}

	return ds
}

func (ds *DiskStorage) IsIndexFile() bool {
	return strings.HasSuffix(ds.segment, "_index")
}

func (ds *DiskStorage) LoadIndex() {
	if ds.r != nil && ds.index == nil && !ds.IsIndexFile() {
		//TODO: load index
		ds.index = LoadIndex(ds.segment)
	}
}

func strToBytes(s string) ([]byte, error) {
	szBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(szBuf, uint32(len(s)))

	var b bytes.Buffer
	ew := utils.NewErrWriter(&b)

	ew.Write(szBuf)
	ew.Write([]byte(s))

	return b.Bytes(), ew.Err()
}

func (ds *DiskStorage) Put(key, value string) error {
	// TODO: better marshalling
	// write metadata
	// prefix namespace

	if ds.filter != nil {
		ds.filter.Set(key)
	}

	w := bufio.NewWriter(ds.w)

	var seq = []string{key, value}
	for _, payload := range seq {
		//log.Println("marshal " + payload)
		data, err := strToBytes(payload)
		if err != nil {
			return err
		}

		w.Write(data)
	}

	return w.Flush()
}

func (ds *DiskStorage) Get(key string) (string, error) {
	var err error
	var val string
	var offset int64 = 0

	if ds.filter != nil {
		if !ds.filter.Check(key) {
			return "", storage.ErrNotFound
		}
	}

	// Lazy loading
	ds.LoadIndex()

	if ds.index != nil {
		if offset, err = ds.index.GetOffset(key); err != nil {
			offset = 0
		}
		log.Printf("fast offset(%d) by index", offset)
	}

	_, err = ds.r.Seek(offset, io.SeekStart)
	if err != nil {
		return "", err
	}
	br := bufio.NewReader(ds.r)
	for {
		keyRead, err := readSizedValue(br)
		if err == io.EOF {
			break
		}

		//log.Printf("read key: [%s]\n", keyRead)
		if keyRead == key {
			return readSizedValue(br)
		} else {
			err = discardSizedValue(br)
		}

		if err != nil {
			break
		}
	}

	return val, storage.ErrNotFound
}

func (ds *DiskStorage) Del(key string) error {
	log.Fatalf("disk should never del a key")
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
		data: skiplist.New(skiplist.String),
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

func (ds *DiskStorage) Scan(f func(k string, v string) bool) {
	var err error

	ds.r.Seek(0, io.SeekStart)
	br := bufio.NewReader(ds.r)
	for {
		var key, val string
		key, err = readSizedValue(br)
		if err == io.EOF {
			break
		}

		val, err = readSizedValue(br)
		if err != nil {
			break
		}

		if !f(key, val) {
			break
		}
	}
}