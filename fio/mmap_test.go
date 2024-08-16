package fio

import (
	"kv-go/utils"
	"testing"
	"path/filepath"
	"github.com/stretchr/testify/assert"
)

func TestMMapIORead(t *testing.T) {
	path := filepath.Join("/tmp", "tmp.data")
	defer DeleteFile(path)
	file, err := NewFileIOManager(path)
	assert.Nil(t, err)
	assert.NotNil(t, file)
	{
		cnt := 10000
		sum := 0
		// 以FileIO的方式打开一个文件，写入数据
		for i := 0; i < cnt; i++ {
			n, err := file.Write(utils.GetTestKey(i))
			assert.Nil(t, err)
			sum += n
		}
		// 以MMapIO的方式打开一个文件，读取之前写入的数据
		mmapFile, err := NewMMapIOManager(path)
		assert.Nil(t, err)
		assert.NotNil(t, file)
		sz, _ := mmapFile.Size()
		assert.Equal(t, int64(sum), sz)
		var off int64 = 0
		for i := 0; i < cnt; i++ {
			b := make([]byte, len(utils.GetTestKey(1)))
			n, err := mmapFile.Read(b, off)
			assert.Nil(t, err)
			off += int64(n)
			assert.Equal(t, b, utils.GetTestKey(i))
		}
	}
	{
		// 测试一些被禁止的方法
		mmapFile, err := NewMMapIOManager(path)
		assert.Nil(t, err)
		assert.NotNil(t, file)
		_, err = mmapFile.Write(utils.GetTestKey(1))
		assert.NotNil(t, err)
		err = mmapFile.Sync()
		assert.NotNil(t, err)
		assert.Nil(t, mmapFile.Close())
	}
}