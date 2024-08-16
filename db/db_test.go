package db

import (
	"bytes"
	"kv-go/index"
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
		err := os.RemoveAll(db.opts.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestOpen(t *testing.T) {
	opts := DefaultDBOptions
	opts.Indexer = index.BPlusTreeType
	opts.DirPath, _ = os.MkdirTemp("/tmp", "test-open")
	db, err := Open(opts)
	defer destoryDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)
}

func TestPutGet(t *testing.T) {
	opts := DefaultDBOptions
	opts.Indexer = index.BPlusTreeType
	opts.DirPath, _ = os.MkdirTemp("/tmp", "test-put")
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
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
		assert.Equal(t, err, ErrEmptyKey)
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
	opts.Indexer = index.BPlusTreeType
	opts.DirPath, _ = os.MkdirTemp("/tmp", "test-put")
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)
	var cnt = 10000
	{
		// put k-v 然后重启，再put看是否能正常使用，最后再get之前写入的数据
		vals := make([][]byte, cnt)
		for i := 0; i < cnt; i++ {
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
		for i := 0; i < cnt; i++ {
			val, err := db2.Get(utils.GetTestKey(i))
			assert.Nil(t, err)
			assert.NotNil(t, val)
			assert.Equal(t, val, vals[i])
		}
	}
}

func TestGet(t *testing.T) {
	opts := DefaultDBOptions
	opts.Indexer = index.BPlusTreeType
	opts.DirPath, _ = os.MkdirTemp("/tmp", "test-put")
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
	opts.Indexer = index.BPlusTreeType
	opts.DirPath, _ = os.MkdirTemp("/tmp", "test-put")
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
		assert.Equal(t, err, ErrKeyNotFound)
		assert.Nil(t, res2)

		err = db.Delete(utils.GetTestKey(1))
		assert.NotNil(t, err)
		assert.Equal(t, err, ErrKeyNotFound)
		res3, err := db.Get(utils.GetTestKey(1))
		assert.NotNil(t, err)
		assert.Equal(t, err, ErrKeyNotFound)
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
		assert.Equal(t, err, ErrEmptyKey)
	}

	{
		var cnt = 10000
		// put多条数据，再删除，重启后，不应该得到这些数据
		vals := make([][]byte, cnt)
		for i := 0; i < cnt; i++ {
			val := utils.GetTestValue(128)
			vals[i] = make([]byte, len(val))
			vals[i] = val
			err := db.Put(utils.GetTestKey(i), vals[i])
			assert.Nil(t, err)
		}
		// 先get，验证存在再删除
		for i := 0; i < cnt; i++ {
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
		for i := 0; i < cnt; i++ {
			res, err := db2.Get(utils.GetTestKey(i))
			assert.Nil(t, res)
			assert.NotNil(t, err)
			assert.NotNil(t, err, ErrKeyNotFound)
		}
	}
}

func TestDBListKeys1(t *testing.T) {
	opts := DefaultDBOptions
	opts.Indexer = index.BPlusTreeType
	opts.DirPath, _ = os.MkdirTemp("", "KeyCache-test-listkeys")
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	{
		// 没有数据
		keys := db.ListKeys(false)
		assert.Equal(t, len(keys), 0)
	}

	{
		// 一条数据
		db.Put(utils.GetTestKey(1), utils.GetTestKey(128))
		keys := db.ListKeys(false)
		assert.Equal(t, len(keys), 1)
		assert.Equal(t, keys[0], utils.GetTestKey(1))
	}
}

func TestDBListKeys2(t *testing.T) {
	opts := DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("", "KeyCache-test-listkeys")
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)

	{
		// 多条数据
		var keys [][]byte
		for i := 0; i < 10000; i++ {
			key := utils.GetTestKey(i)
			keys = append(keys, key)
			db.Put(key, utils.GetTestValue(128))
		}
		listKeys := db.ListKeys(false)
		assert.Equal(t, len(keys), 10000)
		for i := 0; i < 10000; i++ {
			assert.Equal(t, keys[i], listKeys[i])
		}
	}
}

func TestDBFold1(t *testing.T) {
	opts := DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("", "KeyCache-test-fold")
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)

	{
		// 插入多条数据，并通过fold验证数据的正确插入
		var keys [][]byte
		var vals [][]byte
		for i := 0; i < 10000; i++ {
			key := utils.GetTestKey(i)
			val := utils.GetTestValue(128)
			keys = append(keys, key)
			vals = append(vals, val)
			err := db.Put(key, val)
			assert.Nil(t, err)
		}
		var i = 0
		db.Fold(func(key []byte, val []byte) bool {
			res := bytes.Equal(keys[i], key) && bytes.Equal(vals[i], val)
			i++
			return res
		})
	}
}

func TestDBClose1(t *testing.T) {
	opts := DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("", "KeyCache-test-close")
	db, err := Open(opts)
	assert.Nil(t, err)

	{
		// 直接关闭
		err := db.Close()
		assert.Nil(t, err)
	}
}

func TestDBClose2(t *testing.T) {
	opts := DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("", "KeyCache-test-close")
	db, err := Open(opts)
	assert.Nil(t, err)

	{
		// 插入数据后再关闭
		for i := 0; i < 10000; i++ {
			key := utils.GetTestKey(i)
			val := utils.GetTestValue(128)
			err := db.Put(key, val)
			assert.Nil(t, err)
		}
		err := db.Close()
		assert.Nil(t, err)
	}
}

func TestDBSync(t *testing.T) {
	opts := DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("", "KeyCache-test-sync")
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)

	{
		// 直接持久化
		err := db.Sync()
		assert.Nil(t, err)
	}

	{
		// 插入数据后进行持久化
		for i := 0; i < 10000; i++ {
			key := utils.GetTestKey(i)
			val := utils.GetTestValue(128)
			err := db.Put(key, val)
			assert.Nil(t, err)
		}
		err := db.Sync()
		assert.Nil(t, err)
	}
}

func TestBPlusWbId(t *testing.T) {
	opts := DefaultDBOptions
	opts.Indexer = index.BPlusTreeType
	opts.DirPath, _ = os.MkdirTemp("/tmp", "test-put")
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destoryDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)
	var cnt = 10000
	{
		// put k-v 然后重启，再put看是否能正常使用，最后再get之前写入的数据
		vals := make([][]byte, cnt)
		wbopts := DefaultWBOptions
		// 验证wb是否正确：执行一次wb
		wb := db.NewWriteBatch(wbopts)
		assert.Nil(t, err)
		for i := 0; i < cnt; i++ {
			val := utils.GetTestValue(128)
			vals[i] = make([]byte, len(val))
			vals[i] = val
			err := wb.Put(utils.GetTestKey(i), vals[i])
			assert.Nil(t, err)
		}
		wb.Commit()

		// 重启
		err := db.activeFile.Sync()
		assert.Nil(t, err)
		err = db.Close()
		assert.Nil(t, err)
		db2, err := Open(opts)
		assert.Nil(t, err)
		// 验证wbid
		assert.Equal(t, db2.wbId, uint64(1))
		// 继续put k-v
		val := utils.GetTestValue(128)
		err = db2.Put(utils.GetTestKey(1000011), val)
		assert.Nil(t, err)
		res1, err := db2.Get(utils.GetTestKey(1000011))
		assert.Nil(t, err)
		assert.NotNil(t, res1)
		assert.Equal(t, val, res1)

		// 再get k-v
		for i := 0; i < cnt; i++ {
			val, err := db2.Get(utils.GetTestKey(i))
			assert.Nil(t, err)
			assert.NotNil(t, val)
			assert.Equal(t, val, vals[i])
		}
	}
}

func TestFileLock(t *testing.T) {
	// 创建两个进程打开同一个DB实例，后来的进程将打开失败
	opts := DefaultDBOptions
	dirPath, err := os.MkdirTemp("", "filelock")
	opts.DirPath = dirPath
	assert.Nil(t, err)
	db1, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db1)

	db2, err := Open(opts)
	assert.NotNil(t, err)
	assert.Nil(t, db2)
}

func TestBytesSync(t *testing.T) {
	opts := DefaultDBOptions
	opts.BytesSync = 10000
	opts.AlwaysSync = false
	opts.DataFileSize = 100000
	dirPath, err := os.MkdirTemp("", "bytes_sync2")
	opts.DirPath = dirPath
	assert.Nil(t, err)
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destoryDB(db)
	{
		// 写入一些数据达到阈值后，通过底层的IOManager检查文件是否被成功写入（持久化）
		i := 0
		cnt := 100000
		for i = 0; i < cnt; i++ {
			err := db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
			if err != nil {
				// 这里新建了一个用来测试的变量，用来告知此时发生了持久化，我们只需要看持久化是否成功即可
				if err == errTest {
					n, _ := db.activeFile.IOManager.Size()
					assert.NotEqual(t, n, int64(0))
					break
				}
			}
		}
	}
}