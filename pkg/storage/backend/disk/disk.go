package disk

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/sonald/skv/internal/pkg/utils"
	"github.com/sonald/skv/pkg/storage"
	"github.com/sonald/skv/pkg/storage/backend/noop"

	"io"
	"log"
	"os"
)

func init() {
	log.Println("disk backend init")
	storage.RegisterBackend("disk", NewDiskStorage)
}

type DiskStorage struct {
	segment string
	w       io.WriteCloser
	r       io.ReadCloser
}

const (
	SegmentNameOpt = "name"
)

func (ds *DiskStorage) Close() {
	if ds.r != nil {
		ds.r.Close()
	}

	if ds.w != nil {
		ds.w.Close()
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
	}

	if err != nil {
		log.Println(err.Error())
		return &noop.NoopStorage{}
	}

	return ds
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

	w := bufio.NewWriter(ds.w)

	var seq = []string{key, value}
	for _, payload := range seq {
		log.Println("marshal " + payload)
		data, err := strToBytes(payload)
		if err != nil {
			return err
		}

		w.Write(data)
	}

	return w.Flush()
}

func readSizedValue(r io.Reader) (string, error) {
	var err error
	szBuf := make([]byte, 4)
	_, err = r.Read(szBuf)
	if err != nil {
		return "", err
	}
	sz := binary.BigEndian.Uint32(szBuf)

	payload := make([]byte, int(sz))
	r.Read(payload)
	if err != nil {
		return "", err
	}

	return string(payload), nil
}

func skipSizedValue(r *bufio.Reader) error {
	var err error
	szBuf := make([]byte, 4)
	_, err = r.Read(szBuf)
	if err != nil {
		return err
	}
	sz := binary.BigEndian.Uint32(szBuf)

	_, err = r.Discard(int(sz))
	return err
}

func (ds *DiskStorage) Get(key string) (string, error) {
	var err error
	var val string

	br := bufio.NewReader(ds.r)
	for {
		keyRead, err := readSizedValue(br)
		if err == io.EOF {
			break
		}

		log.Printf("read key: [%s]\n", keyRead)
		if keyRead == key {
			// get value
			val, err = readSizedValue(br)
		} else {
			err = skipSizedValue(br)
		}

		if err != nil {
			break
		}
	}

	return val, err
}

func (ds *DiskStorage) Del(key string) error {
	return nil
}
