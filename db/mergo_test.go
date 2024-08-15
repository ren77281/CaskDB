package db

import (
	"kv-go/utils"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMerge1(t *testing.T) {
	opts := DefaultDBOptions
	opts.DirPath = "/home/kv-go/db/tmp"
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destoryDB(db)
	// 正常写入并merge
	{
		for i := 0; i < 10000; i++ {
			db.Put(utils.GetTestKey(1), utils.GetTestValue(128))
		}
		err := db.merge()
		assert.Nil(t, err)
	}
}

func TestMerge2(t *testing.T) {
	opts := DefaultDBOptions
	opts.DirPath = "/home/kv-go/db/tmp"
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destoryDB(db)
	cnt := 10000
	// 写入数据后，merge，再重启，看这些数据是否存在
	{
		keys := make([][]byte, cnt)
		vals := make([][]byte, cnt)
		for i := 0; i < cnt; i++ {
			key := utils.GetTestKey(i)
			val := utils.GetTestValue(128)
			db.Put(key, val)
			keys[i] = key
			vals[i] = val
		}
		for i := 0; i < cnt; i++ {
			key := utils.GetTestKey(i)
			res, err := db.Get(key)
			assert.Nil(t, err)
			assert.Equal(t, res, vals[i])
		}
		// merge并重启
		err := db.Sync()
		assert.Nil(t, err)
		err = db.merge()
		assert.Nil(t, err)
		db.Close()
		db2, err := Open(opts)
		assert.Nil(t, err)
		// 再读取，看数据是否存在
		for i := 0; i < cnt; i++ {
			val, err := db2.Get(keys[i])
			assert.Nil(t, err)
			assert.Equal(t, val, vals[i])
		}
	}
}

func TestMerge3(t *testing.T) {
	opts := DefaultDBOptions
	opts.DirPath = "/home/kv-go/db/tmp"
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destoryDB(db)
	cnt := 100000
	// 写入重复数据后，merge，再重启，看这些数据是否存在
	{
		keys := make([][]byte, cnt)
		vals := make([][]byte, cnt)
		for i := 0; i < cnt; i++ {
			db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
			db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
			db.Put(utils.GetTestKey(i), utils.GetTestValue(128))

			key := utils.GetTestKey(i)
			val := utils.GetTestValue(128)
			db.Put(key, val)
			keys[i] = key
			vals[i] = val
		}
		for i := 0; i < cnt; i++ {
			key := utils.GetTestKey(i)
			res, err := db.Get(key)
			assert.Nil(t, err)
			assert.Equal(t, res, vals[i])
		}
		// merge并重启
		err := db.Sync()
		assert.Nil(t, err)
		err = db.merge()
		assert.Nil(t, err)
		db.Close()
		db2, err := Open(opts)
		assert.Nil(t, err)
		// 再读取，看数据是否存在
		for i := 0; i < cnt; i++ {
			val, err := db2.Get(keys[i])
			assert.Nil(t, err)
			assert.Equal(t, val, vals[i])
		}
	}
}