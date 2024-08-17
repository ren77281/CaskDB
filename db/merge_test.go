package db

import (
	"kv-go/utils"
	"os"
	"sync"
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

func TestMergeAtRatio1(t *testing.T) {
	opts := DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("/tmp", "test-merge-at-ratio1")
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destoryDB(db)
	{
		// 向数据库写入数据，再删除数据以产生无效数据
		// 再写入重复的数据
		// 当无效数据达到阈值时，手动merge并重启数据库，此时通过Stat获取的数据目录大小应该是不同的
		cnt := 100000
		for i := 0; i < cnt; i++ {
			err := db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
			assert.Nil(t, err)
		}
		for i := 0; i < cnt/2; i++ {
			err := db.Delete(utils.GetTestKey(i))
			assert.Nil(t, err)
		}
		for i := cnt/2; i < cnt; i++ {
			err := db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
			assert.Nil(t, err)
		}
		before, err := db.Stat()
		assert.Nil(t, err)
		db.merge()
		db.Close()
		db2, err := Open(opts)
		assert.Nil(t, err)
		assert.NotNil(t, db2)
		after, err := db2.Stat()
		assert.Nil(t, err)
		assert.Greater(t, before.DiskSize, after.DiskSize)
		assert.Equal(t, after.InvalidSize, int64(0))
	}
}

func TestMergeAtRatio2(t *testing.T) {
	opts := DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("/tmp", "test-merge-at-ratio2")
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destoryDB(db)
	{
		// 对空数据库进行合并，以及没有达到阈值时合并，将无法合并
		err := db.merge()
		assert.Nil(t, err)
		for i := 0; i < 100; i++ {
			db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
		}
		err = db.merge()
		assert.NotNil(t, err)
	}
}

func TestMergeAtRatio3(t *testing.T) {
	opts := DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("/tmp", "test-merge-at-ratio999")
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	db.Put(utils.GetTestKey(1000000), utils.GetTestValue(128))
	db.Close()
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer destoryDB(db)
	{
		// 所有数据都是有效数据，merge前后的数据目录大小应该是相同的
		// 由于wbid file的存在，空数据库Close不会产生wbid file，所以先put了一条数据
		cnt := 100000
		for i := 0; i < cnt; i++ {
			err := db2.Put(utils.GetTestKey(i), utils.GetTestValue(128))
			assert.Nil(t, err)
		}
		before, err := db2.Stat()
		assert.Nil(t, err)
		err = db2.merge()
		assert.NotNil(t, err)
		db2.Close()
		db3, err := Open(opts)
		assert.Nil(t, err)
		assert.NotNil(t, db3)

		after, err := db3.Stat()
		assert.Equal(t, before, after)
		assert.Nil(t, err)
	}
}

func TestMergeAtRatio4(t *testing.T) {
	opts := DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("/tmp", "test-merge-at-ratio4")
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destoryDB(db)
	{
		// 所有的数据都是无效数据，merge后，数据目录的大小应该小于100
		for i := 0; i < 10000; i++ {
			db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
		}
		for i := 0; i < 10000; i++ {
			db.Delete(utils.GetTestKey(i))
		}
		err = db.merge()
		assert.Nil(t, err)
		err = db.Close()
		assert.Nil(t, err)
		db, err = Open(opts)
		assert.Nil(t, err)
		stat, err := db.Stat()
		assert.Nil(t, err)
		assert.Less(t, stat.DiskSize, int64(100))
	}
}

func TestMergeAtRatio5(t *testing.T) {
	opts := DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("/tmp", "test-merge-at-ratio5")
	opts.MergeRatio = 0
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destoryDB(db)
	{
		// merge的过程中，其他线程同时在put，并且删除已经存在的数据
		// 需要验证merge不会干扰其他线程的put以及delete
		err := db.merge()
		assert.Nil(t, err)
		cnt := 50000
		for i := 0; i < cnt; i++ {
			db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
		}
		wg := new(sync.WaitGroup)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < cnt; i++ {
				db.Delete(utils.GetTestKey(i))
			}
			for i := cnt; i < 2*cnt; i++ {
				db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
			}
		}()
		err = db.merge()
		assert.Nil(t, err)
		wg.Wait()

		err = db.Close()
		assert.Nil(t, err)
		db, err = Open(opts)
		assert.Nil(t, err)
		keys := db.ListKeys(false)
		assert.Equal(t, cnt, len(keys))
		for i := cnt; i < 2*cnt; i++ {
			_, err := db.Get(utils.GetTestKey(i))
			assert.Nil(t, err)
		}
	}
}
