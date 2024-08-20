package redis

import (
	bitcask "kv-go/db"
	"kv-go/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestString(t *testing.T) {
	opts := bitcask.DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("", "redis-string")
	db, err := bitcask.Open(opts)
	assert.Nil(t, err)
	rds := &RedisDataStructure{
		db: db,
	}
	{
		// 在没有数据的情况下，Delete，Get
		val, err := rds.Get(utils.GetTestKey(1))
		assert.Nil(t, val)
		assert.Equal(t, err, bitcask.ErrKeyNotFound)
		err = rds.Del(utils.GetTestKey(2))
		assert.Equal(t, err, bitcask.ErrKeyNotFound)
	}
	{
		// 测试string的所有功能
		// Set一些数据，Get验证
		// 删除一些数据后，Get验证
		cnt := 100000
		vals := make([][]byte, cnt)
		for i := 0; i < cnt; i++ {
			val := utils.GetTestValue(128)
			err := rds.Set(utils.GetTestKey(i), val, 0)
			vals[i] = val
			assert.Nil(t, err)
		}
		for i := 0; i < cnt; i++ {
			val, err := rds.Get(utils.GetTestKey(i))
			assert.Nil(t, err)
			assert.Equal(t, val, vals[i])
		}
		for i := 0; i < cnt/2; i++ {
			err := rds.Del(utils.GetTestKey(i))
			assert.Nil(t, err)
		}
		for i := 0; i < cnt/2; i++ {
			val, err := rds.Get(utils.GetTestKey(i))
			assert.Equal(t, err, bitcask.ErrKeyNotFound)
			assert.Nil(t, val)
		}
		for i := cnt/2; i < cnt; i++ {
			val, err := rds.Get(utils.GetTestKey(i))
			assert.Nil(t, err)
			assert.Equal(t, val, vals[i])
		}
	}
}

func TestHash1(t *testing.T) {
	opts := bitcask.DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("", "redis-hash")
	db, err := bitcask.Open(opts)
	assert.Nil(t, err)
	rds := &RedisDataStructure{
		db: db,
	}
	{
		// 在没有数据的情况下，Delete，Get
		val, err := rds.HGet(utils.GetTestKey(1), utils.GetTestKey(100))
		assert.Nil(t, val)
		assert.Equal(t, err, bitcask.ErrKeyNotFound)
		fields := make([][]byte, 1)
		fields[0] = utils.GetTestKey(100)
		cnt, err := rds.HDel(utils.GetTestKey(2), fields)
		assert.Equal(t, err, bitcask.ErrKeyNotFound)
		assert.Equal(t, 0, cnt)
	}
	{
		// 往同一个key中插入cnt条数据
		cnt := 100000
		fields := make([][]byte, cnt)
		vals := make([][]byte, cnt)
		for i := 0; i < cnt; i++ {
			fields[i] = utils.GetTestKey(i)
			vals[i] = utils.GetTestValue(128)
		}
		realCnt, err := rds.HSet(utils.GetTestKey(1), fields, vals)
		assert.Nil(t, err)
		assert.Equal(t, realCnt, cnt)
		// Get验证所有数据
		for i := 0; i < cnt; i++ {
			val, err := rds.HGet(utils.GetTestKey(1), fields[i])
			assert.Nil(t, err)
			assert.Equal(t, val, vals[i])
		}
		// 删除前一半的数据
		realCnt, err = rds.HDel(utils.GetTestKey(1), fields[:cnt/2])
		assert.Nil(t, err)
		assert.Equal(t, realCnt, cnt/2)
		// 再次Get验证
		for i := 0; i < cnt/2; i++ {
			val, err := rds.HGet(utils.GetTestKey(1), fields[i])
			assert.Nil(t, val)
			assert.Equal(t, err, bitcask.ErrKeyNotFound)
		}
		for i := cnt/2; i < cnt; i++ {
			val, err := rds.HGet(utils.GetTestKey(1), fields[i])
			assert.Nil(t, err)
			assert.Equal(t, val, vals[i])
		}
	}

}

// TODO:为什么下面的测试用时比上面的长？向同一key中插入与向不同key中插入，总的数据量相同
func TestHash2(t *testing.T) {
	opts := bitcask.DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("", "redis-hash2")
	db, err := bitcask.Open(opts)
	assert.Nil(t, err)
	rds := &RedisDataStructure{
		db: db,
	}
	{
		// 往不同的key中插入一条field，get验证
		cnt := 1000
		vals := make([][]byte, cnt)
		for i := 0; i < cnt; i++ {
			vals[i] = utils.GetTestValue(128)
			realCnt, err := rds.HSet(utils.GetTestKey(i), [][]byte{utils.GetTestKey(i)}, [][]byte{vals[i]})
			assert.Nil(t, err)
			assert.Equal(t, 1, realCnt)
		}
		// get验证
		for i := 0; i < cnt; i++ {
			val, err := rds.HGet(utils.GetTestKey(i), utils.GetTestKey(i))
			assert.Nil(t, err)
			assert.Equal(t, val, vals[i])
		}
		// 再删除一些数据
		for i := 0; i < cnt/2; i++ {
			realCnt, err := rds.HDel(utils.GetTestKey(i), [][]byte{utils.GetTestKey(i)})
			assert.Nil(t, err)
			assert.Equal(t, 1, realCnt)
		}
		// 再get验证
		for i := cnt/2; i < cnt; i++ {
			val, err := rds.HGet(utils.GetTestKey(i), utils.GetTestKey(i))
			assert.Nil(t, err)
			assert.Equal(t, val, vals[i])
		}
	}
}

func TestHash3(t *testing.T) {
	opts := bitcask.DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("", "redis-hash3")
	db, err := bitcask.Open(opts)
	assert.Nil(t, err)
	rds := &RedisDataStructure{
		db: db,
	}
	{
		// 往不同的key中插入多条field，get验证
		fieldCnt := 5000
		fieldVals := make([][]byte, fieldCnt)
		fieldKeys := make([][]byte, fieldCnt)
		// 先保存每个key将要存储的field-value
		for i := 0; i < fieldCnt; i++ {
			fieldKeys[i] = utils.GetTestKey(i+10000)
			fieldVals[i] = utils.GetTestValue(128)
		}
		// HSet
		keyCnt := 50
		for i := 0; i < keyCnt; i++ {
			realCnt, err := rds.HSet(utils.GetTestKey(i), fieldKeys, fieldVals)
			assert.Nil(t, err)
			assert.Equal(t, fieldCnt, realCnt)
		}
		// get验证
		for i := 0; i < keyCnt; i++ {
			for j := 0; j < fieldCnt; j++ {
				val, err := rds.HGet(utils.GetTestKey(i), fieldKeys[j])
				assert.Nil(t, err)
				assert.Equal(t, val, fieldVals[j])
			}
		}
		// 再删除一些数据
		for i := 0; i < keyCnt; i++ {
			rds.HDel(utils.GetTestKey(i), fieldKeys[:fieldCnt/2])
		}
		// 再get验证
		for i := 0; i < keyCnt; i++ {
			for j := fieldCnt/2; j < fieldCnt; j++ {
				val, err := rds.HGet(utils.GetTestKey(i), fieldKeys[j])
				assert.Nil(t, err)
				assert.Equal(t, val, fieldVals[j])
			}
		}
	}
}

func TestSet1(t *testing.T) {
	opts := bitcask.DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("", "redis-set")
	db, err := bitcask.Open(opts)
	assert.Nil(t, err)
	rds := &RedisDataStructure{
		db: db,
	}
	{
		// 没有数据时，get，delete
		cnt := 100000
		for i := 0; i < cnt; i++ {
			ok, err := rds.SIsMember(utils.GetTestKey(i), utils.GetTestKey(i))
			assert.NotNil(t, err)
			assert.False(t,ok)
		}
		for i := 0; i < cnt; i++ {
			realCnt, err := rds.SRem(utils.GetTestKey(i), [][]byte{utils.GetTestKey(i)})
			assert.NotNil(t, err)
			assert.Equal(t, 0, realCnt)
		}
	}
	{
		// 向一个key，add多个member，get验证，再删除，再get验证
		cnt := 100000
		members := make([][]byte, cnt)
		for i := 0; i < cnt; i++ {
			members[i] = utils.GetTestKey(i)
		}
		realCnt, err := rds.SAdd(utils.GetTestKey(1), members)
		assert.Nil(t, err)
		assert.Equal(t, realCnt, cnt)
		for i := 0; i < cnt; i++ {
			ok, err := rds.SIsMember(utils.GetTestKey(1), members[i])
			assert.Nil(t, err)
			assert.True(t, ok)
		}
		realCnt, err = rds.SRem(utils.GetTestKey(1), members[cnt/2:])
		assert.Nil(t, err)
		assert.Equal(t, realCnt, cnt/2)
		for i := 0; i < cnt/2; i++ {
			ok, err := rds.SIsMember(utils.GetTestKey(1), members[i])
			assert.Nil(t, err)
			assert.True(t, ok)
		}
		for i := cnt/2; i < cnt; i++ {
			ok, err := rds.SIsMember(utils.GetTestKey(1), members[i])
			assert.NotNil(t, err)
			assert.False(t, ok)
		}
	}
}

func TestSet2(t *testing.T) {
	opts := bitcask.DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("", "redis-set2")
	db, err := bitcask.Open(opts)
	assert.Nil(t, err)
	rds := &RedisDataStructure{
		db: db,
	}
	{
		// 向不同的key插入一个member
		cnt := 100000
		members := make([][]byte, cnt)
		for i := 0; i < cnt; i++ {
			members[i] = utils.GetTestKey(i)
		}
		realCnt, err := rds.SAdd(utils.GetTestKey(1), members)
		assert.Nil(t, err)
		assert.Equal(t, realCnt, cnt)
		// get验证
		for i := 0; i < cnt; i++ {
			ok, err := rds.SIsMember(utils.GetTestKey(1), members[i])
			assert.Nil(t, err)
			assert.Equal(t, ok, true)
		}
		// 再删除一些数据
		realCnt, err = rds.SRem(utils.GetTestKey(1), members[cnt/2:])
		assert.Nil(t, err)
		assert.Equal(t, realCnt, cnt/2)
		for i := 0; i < cnt/2; i++ {
			ok, err := rds.SIsMember(utils.GetTestKey(1), members[i])
			assert.Nil(t, err)
			assert.Equal(t, ok, true)
		}
		for i := cnt/2; i < cnt; i++ {
			ok, err := rds.SIsMember(utils.GetTestKey(1), members[i])
			assert.NotNil(t, err)
			assert.Equal(t, ok, false)
		}
	}
}

func TestSet3(t *testing.T) {
	opts := bitcask.DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("", "redis-set4")
	db, err := bitcask.Open(opts)
	assert.Nil(t, err)
	rds := &RedisDataStructure{
		db: db,
	}
	{
		// 向不同的key插入多个member
		memberCnt := 1000
		keyCnt := 100
		memberKeys := make([][]byte, memberCnt)
		// 先保存每个key的member
		for i := 0; i < memberCnt; i++ {
			memberKeys[i] = utils.GetTestKey(i)
		}
		for i := 0; i < keyCnt; i++ {
			realCnt, err := rds.SAdd(utils.GetTestKey(i), memberKeys)
			assert.Nil(t, err)
			assert.Equal(t, realCnt, memberCnt)
		}
		// get验证
		for i := 0; i < keyCnt; i++ {
			for j := 0; j < memberCnt; j++ {
				ok, err := rds.SIsMember(utils.GetTestKey(i), memberKeys[i])
				assert.Nil(t, err)
				assert.Equal(t, ok, true)
			}
		}
		// 再删除一些数据
		for i := 0; i < keyCnt; i++ {
			realCnt, err := rds.SRem(utils.GetTestKey(i), memberKeys[memberCnt/2:])
			assert.Nil(t, err)
			assert.Equal(t, realCnt, memberCnt/2)
		}
		// 再get验证
		for i := 0; i < keyCnt; i++ {
			for j := 0; j < memberCnt/2; j++ {
				ok, err := rds.SIsMember(utils.GetTestKey(i), memberKeys[j])
				assert.Nil(t, err)
				assert.Equal(t, ok, true)
			}
		}
		for i := 0; i < keyCnt; i++ {
			for j := memberCnt/2; j < memberCnt; j++ {
				ok, err := rds.SIsMember(utils.GetTestKey(i), memberKeys[j])
				assert.Equal(t, err, bitcask.ErrKeyNotFound)
				assert.Equal(t, ok, false)
			}
		}
	}
}

func TestList1(t *testing.T) {
	opts := bitcask.DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("", "redis-list111")
	db, err := bitcask.Open(opts)
	assert.Nil(t, err)
	rds := &RedisDataStructure{
		db: db,
	}
	{
		// 没有数据时，pop
		val, err := rds.LPop(utils.GetTestKey(1))
		assert.NotNil(t, err)
		assert.Nil(t, val)
		val, err = rds.RPop(utils.GetTestKey(1))
		assert.NotNil(t, err)
		assert.Nil(t, val)
		// 向一个key，push多条数据，验证数量是否正确
		cnt := 1000
		keyNum := 1
		for i := 0; i < cnt; i++ {
			sz, err := rds.LPush(utils.GetTestKey(keyNum), utils.GetTestKey(i))
			assert.Nil(t, err)
			assert.Equal(t, sz, uint32(i+1))
		}
		for i := 0; i < cnt; i++ {
			sz, err := rds.RPush(utils.GetTestKey(keyNum), utils.GetTestKey(i))
			assert.Nil(t, err)
			assert.Equal(t, sz, uint32(cnt+i+1))
		}
		// 再删除验证数据是否正确
		for i := cnt-1; i >= 0; i-- {
			val, err := rds.LPop(utils.GetTestKey(keyNum))
			assert.Nil(t, err)
			assert.Equal(t, val, utils.GetTestKey(i))
		}
		for i := cnt-1; i >= 0; i-- {
			val, err := rds.RPop(utils.GetTestKey(keyNum))
			assert.Nil(t, err)
			assert.Equal(t, val, utils.GetTestKey(i))
		}
		// 再向一个key，push多条数据，验证数量是否正确
		keyNum = 5
		for i := 0; i < cnt; i++ {
			sz, err := rds.LPush(utils.GetTestKey(keyNum), utils.GetTestKey(i))
			assert.Nil(t, err)
			assert.Equal(t, sz, uint32(i+1))
		}
		for i := 0; i < cnt; i++ {
			sz, err := rds.RPush(utils.GetTestKey(keyNum), utils.GetTestKey(i))
			assert.Nil(t, err)
			assert.Equal(t, sz, uint32(cnt+i+1))
		}
		// 再删除验证数据是否正确
		for i := cnt-1; i >= 0; i-- {
			val, err := rds.LPop(utils.GetTestKey(keyNum))
			assert.Nil(t, err)
			assert.Equal(t, val, utils.GetTestKey(i))
		}
		for i := cnt-1; i >= 0; i-- {
			val, err := rds.RPop(utils.GetTestKey(keyNum))
			assert.Nil(t, err)
			assert.Equal(t, val, utils.GetTestKey(i))
		}
	}
}