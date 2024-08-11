package fio

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"os"
)

func DeleteFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIoManager(t *testing.T) {
	path := filepath.Join("../tmp", "tmp.data")
	defer DeleteFile(path)
	fio, err := NewFileIoManager(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIoReadWrite(t *testing.T) {
	path := filepath.Join("../tmp", "tmp.data")
	defer DeleteFile(path)
	fio, err := NewFileIoManager(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte("hello world!!!"))
	assert.Equal(t, n, 14)
	assert.Nil(t, err)

	b1 := make([]byte, 14)
	n, err = fio.Read(b1, 0)
	assert.Nil(t, err)
	assert.Equal(t, n, 14)

	n, err = fio.Write([]byte("kkkkk"))
	assert.Equal(t, n, 5)
	assert.Nil(t, err)

	b2 := make([]byte, 5)
	n, err = fio.Read(b2, 14)
	assert.Nil(t, err)
	assert.Equal(t, n, 5)
}

func TestClose(t *testing.T) {
	path := filepath.Join("../tmp", "tmp.data")
	defer DeleteFile(path)
	fio, err := NewFileIoManager(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}

func TestSync(t *testing.T) {
	path := filepath.Join("../tmp", "tmp.data")
	defer DeleteFile(path)
	fio, err := NewFileIoManager(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte("hello world!!!"))
	assert.Equal(t, n, 14)
	assert.Nil(t, err)
	
	err = fio.Sync()
	assert.Nil(t, err)
}