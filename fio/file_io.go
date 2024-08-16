package fio

import "os"

const DataFilePerm = 0644

type FileIo struct {
	fd *os.File
}

func NewFileIOManager(name string) (*FileIo, error) {
	fd, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_RDWR, DataFilePerm)
	if err != nil {
		return nil, err
	}
	return &FileIo{fd: fd}, nil
}

func (fileIo *FileIo) Read(b []byte, off int64) (int, error) {
	return fileIo.fd.ReadAt(b, off)
}

func (fileIo *FileIo) Write(b []byte) (int, error) {
	return fileIo.fd.Write(b)
}

func (fileIo *FileIo) Sync() error {
	return fileIo.fd.Sync()
}

func (fileIo *FileIo) Close() error {
	return fileIo.fd.Close()
}

func (fileIo *FileIo) Size() (int64, error) {
	fileInfo, err := fileIo.fd.Stat()
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}