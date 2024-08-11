package db

import (
	"kv-go/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 用来删除用来测试的临时文件
func destoryDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			db.Close()
		}
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestOpen(t *testing.T) {
	opts := DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("./tmp", "test-open")
	db, err := Open(opts)
	defer destoryDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)
}

func TestPutGet(t *testing.T) {
	opts := DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("./tmp", "test-put")
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)

	{
		// put 一条正常的k-v 并 get 它
		val := utils.GetTestValue(24)
		err := db.Put(utils.GetTestKey(1), val)
		assert.Nil(t, err)
		res1, err := db.Get(utils.GetTestKey(1))
		assert.NotNil(t, res1)
		assert.Nil(t, err)
		assert.Equal(t, res1, val)
	}

	{
		// put 重复的k-v 并 get 它
		val1 := utils.GetTestValue(24)
		err := db.Put(utils.GetTestKey(1), val1)
		assert.Nil(t, err)
		res1, err := db.Get(utils.GetTestKey(1))
		assert.NotNil(t, res1)
		assert.Nil(t, err)
		assert.Equal(t, res1, val1)

		val2 := utils.GetTestValue(24)
		err = db.Put(utils.GetTestKey(1), val2)
		assert.Nil(t, err)
		res2, err := db.Get(utils.GetTestKey(1))
		assert.NotNil(t, res1)
		assert.Nil(t, err)
		assert.Equal(t, res2, val2)
	}

	{
		// put 一条 key为empty 的k-v
		err := db.Put(nil, utils.GetTestValue(24))
		assert.Equal(t, err, errEmptyKey)
	}

	{
		// put 一条 value为empty 的k-v
		err := db.Put(utils.GetTestKey(2), nil)
		assert.Nil(t, err)
		res1, err := db.Get(utils.GetTestKey(2))
		assert.Nil(t, err)
		assert.Equal(t, len(res1), 0)
		// TODO: res1不为nil而是空切片
		// fmt.Printf("logRecord.Value is nil: %v\n", res1 == nil)
	}

	{
		// put 直到创建了新的data file
		for i := 0; i < 1000000; i++ {
			err := db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
			assert.Nil(t, err)
		}
		assert.NotEqual(t, len(db.inActivaFile), 0)
	}
}

func TestPutGetAfterRestart(t *testing.T) {
	opts := DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("./tmp", "test-put")
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)
	{
		// put k-v 然后重启，再put看是否能正常使用，最后再get之前写入的数据
		vals := make([][]byte, 100000)
		for i := 0; i < 100000; i++ {
			val := utils.GetTestValue(128)
			vals[i] = make([]byte, len(val))
			vals[i] = val
			err := db.Put(utils.GetTestKey(i), vals[i])
			assert.Nil(t, err)
		}
		// 重启
		err := db.activeFile.Sync()
		assert.Nil(t, err)
		err = db.Close()
		assert.Nil(t, err)
		db2, err := Open(opts)
		assert.Nil(t, err)

		// 继续put k-v
		val := utils.GetTestValue(128)
		err = db2.Put(utils.GetTestKey(1000011), val)
		assert.Nil(t, err)
		res1, err := db2.Get(utils.GetTestKey(1000011))
		assert.Nil(t, err)
		assert.NotNil(t, res1)
		assert.Equal(t, val, res1)

		// 再get k-v
		for i := 0; i < 10000; i++ {
			val, err := db2.Get(utils.GetTestKey(i))
			assert.Nil(t, err)
			assert.NotNil(t, val)
			assert.Equal(t, val, vals[i])
		}
	}
}

func TestGet(t *testing.T) {
	opts := DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("./tmp", "test-put")
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)
	{
		// put k-v后删除再get
		val1 := utils.GetTestValue(24)
		err := db.Put(utils.GetTestKey(1), val1)
		assert.Nil(t, err)
		res1, err := db.Get(utils.GetTestKey(1))
		assert.NotNil(t, res1)
		assert.Nil(t, err)
		assert.Equal(t, res1, val1)

		val2 := utils.GetTestValue(24)
		err = db.Put(utils.GetTestKey(1), val2)
		assert.Nil(t, err)
		res2, err := db.Get(utils.GetTestKey(1))
		assert.NotNil(t, res1)
		assert.Nil(t, err)
		assert.Equal(t, res2, val2)

		// 删除
		err = db.Delete(utils.GetTestKey(1))
		assert.Nil(t, err)
		res3, err := db.Get(utils.GetTestKey(1))
		assert.NotNil(t, err)
		assert.Nil(t, res3)
	}

	{
		// get 不存在的k-v
		res, err := db.Get(utils.GetTestKey(1111))
		assert.Nil(t, res)
		assert.NotNil(t, err)
	}
}

func TestDelete(t *testing.T) {
	opts := DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("./tmp", "test-put")
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)
	{
		// 正常删除 + 二次删除，删除后再put
		val1 := utils.GetTestValue(24)
		err := db.Put(utils.GetTestKey(1), val1)
		assert.Nil(t, err)
		res1, err := db.Get(utils.GetTestKey(1))
		assert.NotNil(t, res1)
		assert.Nil(t, err)
		assert.Equal(t, res1, val1)

		err = db.Delete(utils.GetTestKey(1))
		assert.Nil(t, err)
		res2, err := db.Get(utils.GetTestKey(1))
		assert.NotNil(t, err)
		assert.Equal(t, err, errKeyNotFound)
		assert.Nil(t, res2)

		err = db.Delete(utils.GetTestKey(1))
		assert.NotNil(t, err)
		assert.Equal(t, err, errKeyNotFound)
		res3, err := db.Get(utils.GetTestKey(1))
		assert.NotNil(t, err)
		assert.Equal(t, err, errKeyNotFound)
		assert.Nil(t, res3)

		val4 := utils.GetTestValue(128)
		err = db.Put(utils.GetTestKey(1), val4)
		assert.Nil(t, err)
		res4, err := db.Get(utils.GetTestKey(1))
		assert.Nil(t, err)
		assert.Equal(t, res4, val4)
	}

	{
		// 删除key为empty 的k-v
		err := db.Delete(nil)
		assert.NotNil(t, err)
		assert.Equal(t, err, errEmptyKey)
	}

	{
		// put多条数据，再删除，重启后，不应该得到这些数据
		vals := make([][]byte, 100000)
		for i := 0; i < 100000; i++ {
			val := utils.GetTestValue(128)
			vals[i] = make([]byte, len(val))
			vals[i] = val
			err := db.Put(utils.GetTestKey(i), vals[i])
			assert.Nil(t, err)
		}
		// 先get，验证存在再删除
		for i := 0; i < 100000; i++ {
			res, err := db.Get(utils.GetTestKey(i))
			assert.Nil(t, err)
			assert.NotNil(t, res)
			assert.Equal(t, res, vals[i])
			err = db.Delete(utils.GetTestKey(i))
			assert.Nil(t, err)
		}
		// 重启数据库
		err := db.activeFile.Sync()
		assert.Nil(t, err)
		err = db.Close()
		assert.Nil(t, err)
		db2, err := Open(opts)
		assert.Nil(t, err)
		// 再get这些被删除数据
		for i := 0; i < 100000; i++ {
			res, err := db2.Get(utils.GetTestKey(i))
			assert.Nil(t, res)
			assert.NotNil(t, err)
			assert.NotNil(t, err, errKeyNotFound)
		}
	}
}