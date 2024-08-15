package db

import (
	"kv-go/utils"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDBIterNew(t *testing.T) {
	opts := DefaultDBOptions
	dir, _ := os.MkdirTemp("", "KeyCache-test-iter-new")
	opts.DirPath = dir
	db, err := Open(opts)
	assert.Nil(t, err)

	itopts := DefaultItOptions
	iter := db.NewIterator(itopts)
	assert.NotNil(t, iter)
	assert.True(t, iter.IsEnd())
}

func TestDBIter1(t *testing.T) {
	opts := DefaultDBOptions
	dir, _ := os.MkdirTemp("", "KeyCache-iter-test-1")
	opts.DirPath = dir
	db, err := Open(opts)
	assert.Nil(t, err)

	{
		// 插入一条数据
		key1 := utils.GetTestKey(1)
		val1 := utils.GetTestValue(128)
		err := db.Put(key1, val1)
		assert.Nil(t, err)
		itopts := DefaultItOptions
		iter := db.NewIterator(itopts)
		assert.False(t, iter.IsEnd())
		assert.Equal(t, iter.Key(), key1)
		itval1, _ := iter.Value()
		assert.Equal(t, itval1, val1)
	}
}

func TestDBIter2(t *testing.T) {
	opts := DefaultDBOptions
	dir, _ := os.MkdirTemp("", "KeyCache-iter-test-2")
	opts.DirPath = dir
	db, err := Open(opts)
	assert.Nil(t, err)

	{
		// 插入多条数据
		keys := make([][]byte, 1000)
		vals := make([][]byte, 1000)
		for i := 0; i < 1000; i++ {
			key := utils.GetTestKey(i)
			val := utils.GetTestValue(128)
			keys[i] = key
			vals[i] = val
			
			err := db.Put(key, val)
			assert.Nil(t, err)
		}
		itopts := DefaultItOptions
		iter := db.NewIterator(itopts)
		assert.Equal(t, iter.indexIter.Size(), 1000)
		assert.False(t, iter.IsEnd())
		for i := 0; i < 1000; i++ {
			assert.False(t, iter.IsEnd())
			assert.Equal(t, iter.Key(), keys[i])
			itval, _ := iter.Value()
			assert.Equal(t, itval, vals[i])
			iter.Next()
		}
		assert.True(t, iter.IsEnd())
	}
}

func TestDBIter3(t *testing.T) {
	opts := DefaultDBOptions
	dir, _ := os.MkdirTemp("", "KeyCache-iter-test-3")
	opts.DirPath = dir
	db, err := Open(opts)
	assert.Nil(t, err)

	{
		// 插入多条数据，并反向迭代
		keys := make([][]byte, 1000)
		vals := make([][]byte, 1000)
		for i := 0; i < 1000; i++ {
			key := utils.GetTestKey(i)
			val := utils.GetTestValue(128)
			keys[i] = key
			vals[i] = val
			
			err := db.Put(key, val)
			assert.Nil(t, err)
		}
		itopts := DefaultItOptions
		itopts.Reverse = true
		iter := db.NewIterator(itopts)
		assert.Equal(t, iter.indexIter.Size(), 1000)
		assert.False(t, iter.IsEnd())
		for i := 999; i >= 0; i-- {
			assert.False(t, iter.IsEnd())
			assert.Equal(t, iter.Key(), keys[i])
			itval, _ := iter.Value()
			assert.Equal(t, itval, vals[i])
			iter.Next()
		}
		assert.True(t, iter.IsEnd())
	}
}

func TestDBIter4(t *testing.T) {
	opts := DefaultDBOptions
	dir, _ := os.MkdirTemp("", "KeyCache-iter-test-4")
	opts.DirPath = dir
	db, err := Open(opts)
	assert.Nil(t, err)

	{
		// 插入多条数据，并指定前缀正向迭代
		var keys [][]byte 
		var vals [][]byte 
		for i := 0; i < 500; i++ {
			key := utils.GetTestKey(i)
			val := utils.GetTestValue(128)
			keys = append(keys, key)
			vals = append(vals, val)
			err := db.Put(key, val)
			assert.Nil(t, err)

			key = []byte(strconv.Itoa(i))
			val = utils.GetTestValue(128)
			keys = append(keys, key)
			vals = append(vals, val)
			err = db.Put(key, val)
			assert.Nil(t, err)
		}

		itopts := DefaultItOptions
		itopts.Prefix = []byte("go-kv-key")
		iter := db.NewIterator(itopts)
		assert.Equal(t, iter.indexIter.Size(), 1000)
		assert.False(t, iter.IsEnd())
		for i := 0; i < 1000; i += 2 {
			assert.False(t, iter.IsEnd())
			assert.Equal(t, iter.Key(), keys[i])
			itval, _ := iter.Value()
			assert.Equal(t, itval, vals[i])
			iter.Next()
		}
		assert.True(t, iter.IsEnd())
	}
}

func TestDBIter5(t *testing.T) {
	opts := DefaultDBOptions
	dir, _ := os.MkdirTemp("", "KeyCache-iter-test-5")
	opts.DirPath = dir
	db, err := Open(opts)
	assert.Nil(t, err)

	{
		// 插入多条数据，并指定前缀反向迭代
		var keys [][]byte 
		var vals [][]byte 
		for i := 0; i < 500; i++ {
			key := utils.GetTestKey(i)
			val := utils.GetTestValue(128)
			keys = append(keys, key)
			vals = append(vals, val)
			err := db.Put(key, val)
			assert.Nil(t, err)

			key = []byte(strconv.Itoa(i))
			val = utils.GetTestValue(128)
			keys = append(keys, key)
			vals = append(vals, val)
			err = db.Put(key, val)
			assert.Nil(t, err)
		}

		itopts := DefaultItOptions
		itopts.Prefix = []byte("go-kv-key")
		itopts.Reverse = true
		iter := db.NewIterator(itopts)
		assert.Equal(t, iter.indexIter.Size(), 1000)
		assert.False(t, iter.IsEnd())
		for i := 998; i >= 0; i -= 2 {
			assert.False(t, iter.IsEnd())
			assert.Equal(t, iter.Key(), keys[i])
			itval, _ := iter.Value()
			assert.Equal(t, itval, vals[i])
			iter.Next()
		}
		assert.True(t, iter.IsEnd())
	}
}