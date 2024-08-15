package data

import (
	"kv-go/utils"
	"os"
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestDataFileOpen(t *testing.T) {
	df, err := OpenDataFile(os.TempDir(), 1)
	assert.NotNil(t, df)
	assert.Nil(t, err)

	df1, err := OpenDataFile(os.TempDir(), 1)
	assert.NotNil(t, df1)
	assert.Nil(t, err)
}

func TestDataFileWrite(t *testing.T) {
	df, err := OpenDataFile(os.TempDir(), 1)
	assert.NotNil(t, df)
	assert.Nil(t, err)

	err = df.Write([]byte("aaaaaa"))
	assert.Nil(t, err)

	err = df.Write([]byte("ccccc"))
	assert.Nil(t, err)

	err = df.Write([]byte("bbbbb"))
	assert.Nil(t, err)
}

func TestDataFileCloseSync(t *testing.T) {
	df, err := OpenDataFile(os.TempDir(), 1)
	assert.NotNil(t, df)
	assert.Nil(t, err)

	err = df.Write([]byte("aaaaaa"))
	assert.Nil(t, err)

	err = df.Write([]byte("ccccc"))
	assert.Nil(t, err)

	err = df.Write([]byte("bbbbb"))
	assert.Nil(t, err)
	
	err = df.Sync()
	assert.Nil(t, err)

	err = df.Close()
	assert.Nil(t, err)
}

func TestDataFileRead(t *testing.T) {
	df, err := OpenDataFile("../tmp", 1)
	assert.NotNil(t, df)
	assert.Nil(t, err)
	var offset int64 = 0
	for i := 0; i < 10; i++ {
		lr := &LogRecord{
			Key: utils.GetTestKey(1),
			Value: utils.GetTestValue(100),
			Typ: LogRecordNormal,
		}
		datas, sz := EncodeLogRecord(lr)
		err := df.Write(datas)
		assert.Nil(t, err)
		// 读取被写入的record
		llr, ssz, err := df.ReadLogRecord(offset)
		assert.Nil(t, err)
		assert.Equal(t, lr, llr)
		assert.Equal(t, sz, ssz)
		// 再次读取
		llr, ssz, err = df.ReadLogRecord(offset)
		assert.Nil(t, err)
		assert.Equal(t, lr, llr)
		assert.Equal(t, sz, ssz)
		offset += ssz
	}

	// 只有一条record
	df, err = OpenDataFile("../tmp", 2)
	assert.NotNil(t, df)
	assert.Nil(t, err)
	offset = 0
	lr := &LogRecord{
		Key: utils.GetTestKey(10),
		Value: utils.GetTestValue(100),
		Typ: LogRecordNormal,
	}
	datas, sz := EncodeLogRecord(lr)
	err = df.Write(datas)
	assert.Nil(t, err)
	// 读取被写入的record
	llr, ssz, err := df.ReadLogRecord(offset)
	assert.Nil(t, err)
	assert.Equal(t, lr, llr)
	assert.Equal(t, sz, ssz)
	// 再次读取
	llr, ssz, err = df.ReadLogRecord(offset)
	assert.Nil(t, err)
	assert.Equal(t, lr, llr)
	assert.Equal(t, sz, ssz)
	offset += ssz

	{
		// data中有多条record
		df3, err3 := OpenDataFile("../tmp", 3)
		assert.NotNil(t, df3)
		assert.Nil(t, err3)
		sizes3 := make([]int64, 0)
		lrs3 := make([]*LogRecord, 0)
		for i := 0; i < 10; i++ {
			lr3 := &LogRecord{
				Key: utils.GetTestKey(1),
				Value: utils.GetTestValue(100),
				Typ: LogRecordNormal,
			}
			lrs3 = append(lrs3, lr3)
			datas, sz3 := EncodeLogRecord(lr3)
			sizes3 = append(sizes3, sz3)
			err := df3.Write(datas)
			assert.Nil(t, err)
		}
		var offset3 int64 = 0
		for i := 0; i < 10; i++ {
			// 读取被写入的record
			llr, ssz, err := df3.ReadLogRecord(offset3)
			assert.Nil(t, err)
			assert.Equal(t, lrs3[i], llr)
			assert.Equal(t, sizes3[i], ssz)
			// 再次读取
			llr, ssz, err = df3.ReadLogRecord(offset3)
			assert.Nil(t, err)
			assert.Equal(t, lrs3[i], llr)
			assert.Equal(t, sizes3[i], ssz)
			offset3 += ssz
		}
	}
	// data中有多条被删除的record
	df4, err4 := OpenDataFile("../tmp", 4)
	assert.NotNil(t, df4)
	assert.Nil(t, err4)
	sizes4 := make([]int64, 0)
	lrs4 := make([]*LogRecord, 0)
	for i := 0; i < 10; i++ {
		lr4 := &LogRecord{
			Key: utils.GetTestKey(1),
			Value: utils.GetTestValue(100),
			Typ: LogRecordDeleted,
		}
		lrs4 = append(lrs4, lr4)
		datas, sz3 := EncodeLogRecord(lr4)
		sizes4 = append(sizes4, sz3)
		err := df4.Write(datas)
		assert.Nil(t, err)
	}
	var offset4 int64 = 0
	for i := 0; i < 10; i++ {
		// 读取被写入的record
		llr, ssz, err := df4.ReadLogRecord(offset4)
		assert.Nil(t, err)
		assert.Equal(t, lrs4[i], llr)
		assert.Equal(t, sizes4[i], ssz)
		assert.Equal(t, llr.Typ, byte(LogRecordDeleted))
		// 再次读取
		llr, ssz, err = df4.ReadLogRecord(offset4)
		assert.Nil(t, err)
		assert.Equal(t, lrs4[i], llr)
		assert.Equal(t, sizes4[i], ssz)
		assert.Equal(t, llr.Typ, byte(LogRecordDeleted))
		offset4 += ssz
	}
}