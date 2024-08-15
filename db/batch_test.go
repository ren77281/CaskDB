package db

import (
	"bytes"
	"kv-go/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatchSerialize(t *testing.T) {
	key := []byte("1111111111")
	var id uint64 = 11
	keybyte := serializeKeyId(key, id)
	kkey, iid := parseKeyId(keybyte)
	assert.Equal(t, id, iid)
	assert.True(t, bytes.Equal(key, kkey))
}

func TestBatchPutCommit1(t *testing.T) {
	opts := DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("", "KeyCache-test-put-commit-1")
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	wbopts := DefaultWBOptions
	wb := db.NewWriteBatch(wbopts)
	{
		// Put多个数据后，Commit
		for i := 0; i < 1000; i++ {
			err := wb.Put(utils.GetTestKey(1), utils.GetTestValue(128))
			assert.Nil(t, err)
		}
		err := wb.Commit()
		assert.Nil(t, err)
	}
}

func TestBatchPutCommitDelete(t *testing.T) {
	opts := DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("", "KeyCache-test-put-commit-delete")
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	wbopts := DefaultWBOptions
	{
		wb := db.NewWriteBatch(wbopts)
		// 正常Put后Commit看db中是否存在数据
		var vals [][]byte
		for i := 0; i < 10000; i++ {
			val := utils.GetTestValue(128)
			err := wb.Put(utils.GetTestKey(i), val)
			vals = append(vals, val)
			assert.Nil(t, err)
		}

		wb.Commit()
		for i := 0; i < 10000; i++ {
			val, err := db.Get(utils.GetTestKey(i))
			assert.Nil(t, err)
			assert.Equal(t, val, vals[i])
		}
		// 再删除之前Put的一些数据，Commit，看这些数据是否被删除
		wb = db.NewWriteBatch(wbopts)
		for i := 0; i < 5000; i++ {
			err := wb.Delete(utils.GetTestKey(i))
			assert.Nil(t, err)
		}
		wb.Commit()
		// 看被删除的数据是否存在 
		for i := 0; i < 5000; i++ {
			_, err := db.Get(utils.GetTestKey(i))
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrKeyNotFound)
		}
		// 看剩下的数据是否存在
		for i := 5000; i < 10000; i++ {
			val, err := db.Get(utils.GetTestKey(i))
			assert.Nil(t, err)
			assert.Equal(t, val, vals[i])
		}
	}
}

func TestBatchPutCommitDeleteRestart(t *testing.T) {
	opts := DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("", "KeyCache-test-put-commit-delete-restart")
	db, err := Open(opts)
	assert.Nil(t, err)
	wbopts := DefaultWBOptions
	{
		wb := db.NewWriteBatch(wbopts)
		// 正常Put后Commit看db中是否存在数据
		var vals [][]byte
		for i := 0; i < 100000; i++ {
			val := utils.GetTestValue(128)
			err := wb.Put(utils.GetTestKey(i), val)
			vals = append(vals, val)
			assert.Nil(t, err)
		}

		err := wb.Commit()
		assert.Nil(t, err)
		for i := 0; i < 100000; i++ {
			val, err := db.Get(utils.GetTestKey(i))
			assert.Nil(t, err)
			assert.Equal(t, val, vals[i])
		}
		// 再删除之前Put的一些数据，Commit，看这些数据是否被删除
		wb = db.NewWriteBatch(wbopts)
		for i := 0; i < 50000; i++ {
			err := wb.Delete(utils.GetTestKey(i))
			assert.Nil(t, err)
		}
		err = wb.Commit()
		assert.Nil(t, err)
		// 看被删除的数据是否存在 
		for i := 0; i < 50000; i++ {
			_, err := db.Get(utils.GetTestKey(i))
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrKeyNotFound)
		}
		// 看剩下的数据是否存在
		for i := 50000; i < 100000; i++ {
			val, err := db.Get(utils.GetTestKey(i))
			assert.Nil(t, err)
			assert.Equal(t, val, vals[i])
		}
		// 重启db后，看之前的操作是否有效
		db.Close()
		db2, err := Open(opts)
		assert.Nil(t, err)
		// 看被删除的数据是否存在 
		for i := 0; i < 50000; i++ {
			_, err := db2.Get(utils.GetTestKey(i))
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrKeyNotFound)
		}
		// 看剩下的数据是否存在
		for i := 50000; i < 100000; i++ {
			val, err := db2.Get(utils.GetTestKey(i))
			assert.Nil(t, err)
			assert.Equal(t, val, vals[i])
		}
		// 检验wbId
		assert.Equal(t, db.id, uint64(2))
	}
}