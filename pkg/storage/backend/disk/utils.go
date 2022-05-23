package disk

import (
	"bufio"
	"encoding/binary"
	"io"
)

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

func discardSizedValue(r *bufio.Reader) error {
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

func skipSizedValue(r io.ReadSeeker) error {
	var err error
	szBuf := make([]byte, 4)
	_, err = r.Read(szBuf)
	if err != nil {
		return err
	}
	sz := binary.BigEndian.Uint32(szBuf)

	_, err = r.Seek(int64(sz), io.SeekCurrent)
	return err
}
