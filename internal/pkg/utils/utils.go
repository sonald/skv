package utils

import "io"

type ErrWriter struct {
	w   io.Writer
	err error
}

func NewErrWriter(w io.Writer) *ErrWriter {
	return &ErrWriter{
		w: w,
	}
}

func (ew *ErrWriter) Err() error {
	return ew.err
}

func (ew *ErrWriter) Write(p []byte) {
	if ew.err != nil {
		return
	}

	_, ew.err = ew.w.Write(p)
}
