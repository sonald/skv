package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/sonald/skv/pkg/utils"
	"io"
	"log"
)

// value types
const (
	// normal data
	TagValue = 1 << iota
	// marked as deleted
	TagTombstone
)

func LengthPrefixed(s []byte) ([]byte, error) {
	szBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(szBuf, uint32(len(s)))

	var b bytes.Buffer
	ew := utils.NewErrWriter(&b)

	ew.Write(szBuf)
	ew.Write(s)

	return b.Bytes(), ew.Err()
}

func ReadLengthPrefixed(r io.Reader) ([]byte, error) {
	var err error
	szBuf := make([]byte, 4)
	_, err = r.Read(szBuf)
	if err != nil {
		return nil, err
	}
	sz := binary.BigEndian.Uint32(szBuf)

	payload := make([]byte, int(sz))
	r.Read(payload)
	if err != nil {
		return nil, err
	}

	return payload, nil
}

func DiscardLengthPrefixedValue(r *bufio.Reader) error {
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

type InternalKey struct {
	key       []byte
	seqAndTag uint64
}

func KeyFromUser(key []byte, seq uint64, tag uint8) *InternalKey {
	return &InternalKey{
		key:       key,
		seqAndTag: seq<<8 | uint64(tag),
	}
}

func DecodeInternalKey(data []byte) *InternalKey {
	var ikey InternalKey
	var buf = bytes.NewBuffer(data)

	szBuf := buf.Next(4)
	if szBuf == nil {
		log.Fatalln("invalid key structure")
		return nil
	}

	len := binary.BigEndian.Uint32(szBuf)
	ikey.key = buf.Next(int(len) - 8)
	ikey.seqAndTag = binary.BigEndian.Uint64(buf.Next(8))
	return &ikey
}

func ReadInternalKey(r io.Reader) (*InternalKey, error) {
	var ikey InternalKey

	szbuf := make([]byte, 4)
	_, err := r.Read(szbuf)
	if err != nil {
		return nil, err
	}
	keylen := binary.BigEndian.Uint32(szbuf)

	payload := make([]byte, int(keylen))
	r.Read(payload)
	if err != nil {
		return nil, err
	}
	ikey.key = payload[:keylen-8]
	ikey.seqAndTag = binary.BigEndian.Uint64(payload[keylen-8:])

	return &ikey, nil
}

func (ikey *InternalKey) Tag() int {
	return int(ikey.seqAndTag & 0xff)
}

func (ikey *InternalKey) Sequence() uint64 {
	return ikey.seqAndTag >> 8
}

func (ikey *InternalKey) Key() []byte {
	return ikey.key
}

//TODO: should we consider sequence number?
func (ikey *InternalKey) Equal(other *InternalKey) bool {
	if bytes.Compare(ikey.key, other.key) == 0 {
		return ikey.Tag() == ikey.Tag()
	}

	return false
}

func (ikey *InternalKey) ToString() string {
	var tag string = "V"
	if ikey.Tag() == TagTombstone {
		tag = "D"
	}
	return fmt.Sprintf("Key{%s:%08d:%s}", string(ikey.key), ikey.Sequence(), tag)
}

func (ikey *InternalKey) Encode() []byte {
	var kbuf = bytes.NewBuffer(ikey.key)
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, ikey.seqAndTag)
	kbuf.Write(data)
	res, err := LengthPrefixed(kbuf.Bytes())
	if err != nil {
		log.Fatalln(err)
	}
	return res
}

// for skiplist used in memtable
// sequence number is sorted in decreasing order, so Find will found lastest
// value first
func GreaterThan(lhs interface{}, rhs interface{}) int {
	key1 := lhs.(*InternalKey)
	key2 := rhs.(*InternalKey)
	res := bytes.Compare(key1.key, key2.key)
	if res == 0 {
		if key1.Sequence() > key2.Sequence() {
			res = -1
		} else if key1.Sequence() < key2.Sequence() {
			res = 1
		}
	}
	return res
}

type Storage interface {
	Put(key *InternalKey, value []byte) error
	Get(key *InternalKey) ([]byte, error)
	Del(key *InternalKey) error
	Scan(func(k *InternalKey, v []byte) bool)
	Close()

	// estimated size
	Size() int
	// estimated key count
	Count() int
}

type Options struct {
	Args map[string]interface{}
}

const Megabyte = 1048576

var (
	ErrNotFound   = errors.New("key does not found")
	ErrKeyDeleted = errors.New("key has been deleted")
)

const (
	SegmentOpenMode = "SegmentOpenMode"
)

const (
	SegmentOpenModeRO = iota
	SegmentOpenModeWR
	SegmentOpenModeRW
)

var storages = make(map[string]func(Options) Storage)

func RegisterBackend(name string, impl func(Options) Storage) {
	storages[name] = impl
}

func GetStorage(name string, options Options) Storage {
	return storages[name](options)
}
