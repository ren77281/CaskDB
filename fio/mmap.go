package fio

import (
	"errors"
	"os"

	"golang.org/x/exp/mmap"
)

var (
	ErrMMapCanNotWrite = errors.New("MMap can only read")
	ErrMMapCanNotSync  = errors.New("MMap can only read")
)

type MMap struct {
	readAt *mmap.ReaderAt
}

func NewMMapIOManager(fileName string) (*MMap, error) {
	if _, err := os.OpenFile(fileName, os.O_CREATE, os.ModePerm); err != nil {
		return nil, err
	}
	readAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{readAt: readAt}, nil
}

func (m *MMap) Read(b []byte, off int64) (int, error) {
	return m.readAt.ReadAt(b, off)
}

func (m *MMap) Write(b []byte) (int, error) {
	return 0, ErrMMapCanNotWrite
}

func (m *MMap) Sync() error {
	return ErrMMapCanNotSync
}

func (m *MMap) Close() error {
	return m.readAt.Close()
}

func (m *MMap) Size() (int64, error) {
	return int64(m.readAt.Len()), nil
}
