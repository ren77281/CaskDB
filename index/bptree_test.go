package index

import (
	"kv-go/data"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBPlusTreePut(t *testing.T) {
	bt := NewBPlusTree("/tmp", false)
	res1, _ := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 1})
	assert.True(t, res1)

	res2, _ := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)

	res3, _ := bt.Put(nil, &data.LogRecordPos{Fid: 2, Offset: 2})
	assert.False(t, res3)
}

func TestBPlusTreeGet(t *testing.T) {
	bt := NewBPlusTree("/tmp", false)

	res1, _ := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 1})
	assert.True(t, res1)

	res2, _ := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)

	res3, _ := bt.Put(nil, &data.LogRecordPos{Fid: 2, Offset: 2})
	assert.False(t, res3)

	res4 := bt.Get([]byte("a"))
	assert.NotNil(t, res4)
	assert.Equal(t, res4.Fid, uint32(1))
	assert.Equal(t, res4.Offset, int64(2))
	
	res5 := bt.Get(nil)
	assert.Nil(t, res5)
}

func TestBPlusTreeDelete(t *testing.T) {
	bt := NewBPlusTree("/tmp", false)

	res1, _ := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 1})
	assert.True(t, res1)

	res2, _ := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)

	res3, _ := bt.Put(nil, &data.LogRecordPos{Fid: 2, Offset: 2})
	assert.False(t, res3)

	res4, _ := bt.Delete([]byte("a"))
	assert.True(t, res4)

	res5, _ := bt.Delete([]byte("b"))
	assert.False(t, res5)
	
	res6 := bt.Get([]byte("a"))
	assert.Nil(t, res6)
}

func TestBPlusTreeIterator1(t *testing.T) {
	os.RemoveAll("/tmp/index-bptree")
	bt := NewBPlusTree("/tmp", false)
	{
		// 没有数据
		iter := bt.NewIterator(false)
		assert.True(t, iter.IsEnd())
		iter.Close()
	}
	{
		// 插入一条
		key1 := []byte("1")
		val1 := data.LogRecordPos{Fid: 1, Offset: 0}

		bt.Put(key1, &val1)
		iter := bt.NewIterator(false)
		assert.False(t, iter.IsEnd())

		iter.Rewind()
		assert.Equal(t, bt.Size(), 1)
		assert.Equal(t, iter.Key(), key1)
		assert.Equal(t, iter.Value(), &val1)
		assert.False(t, iter.IsEnd())

		iter.Next()
		assert.True(t, iter.IsEnd())
	}
}

func TestBPlusTreeIterator2(t *testing.T) {
	os.RemoveAll("/tmp/index-bptree")
	bt := NewBPlusTree("/tmp", false)
	{
		// 插入多条数据，并测试seek
		key1, key2 := []byte("2"), []byte("3")
		val1, val2 := data.LogRecordPos{Fid: 1, Offset: 0}, data.LogRecordPos{Fid: 1, Offset: 0}
		bt.Put(key1, &val1)
		bt.Put(key2, &val2)

		iter := bt.NewIterator(false)
		assert.Equal(t, bt.Size(), 2)
		assert.False(t, iter.IsEnd())

		assert.Equal(t, iter.Key(), key1)
		assert.Equal(t, iter.Value(), &val1)
		assert.False(t, iter.IsEnd())

		iter.Next()
		assert.False(t, iter.IsEnd())

		assert.Equal(t, iter.Key(), key2)
		assert.Equal(t, iter.Value(), &val2)
		assert.False(t, iter.IsEnd())

		iter.Next()
		assert.True(t, iter.IsEnd())

		iter.Seek([]byte("1"))
		assert.Equal(t, iter.Key(), key1)
		assert.Equal(t, iter.Value(), &val1)
		assert.False(t, iter.IsEnd())
	}
}

func TestBPlusTreeIterator3(t *testing.T) {
	os.RemoveAll("/tmp/index-bptree")
	bt := NewBPlusTree("/tmp", false)
	{
		// 插入多条数据，并测试seek(反向)
		key1, key2 := []byte("2"), []byte("3")
		val1, val2 := data.LogRecordPos{Fid: 1, Offset: 0}, data.LogRecordPos{Fid: 1, Offset: 0}
		bt.Put(key1, &val1)
		bt.Put(key2, &val2)

		iter := bt.NewIterator(true)
		assert.Equal(t, bt.Size(), 2)
		assert.False(t, iter.IsEnd())

		assert.Equal(t, iter.Key(), key2)
		assert.Equal(t, iter.Value(), &val2)
		assert.False(t, iter.IsEnd())

		iter.Next()
		assert.False(t, iter.IsEnd())

		assert.Equal(t, iter.Key(), key1)
		assert.Equal(t, iter.Value(), &val1)
		assert.False(t, iter.IsEnd())

		iter.Next()
		assert.True(t, iter.IsEnd())

		iter.Seek([]byte("4"))
		assert.Equal(t, iter.Key(), key2)
		assert.Equal(t, iter.Value(), &val2)
		assert.False(t, iter.IsEnd())
	}
}