package benchmark

import (
	bitcask "kv-go/db"
	"kv-go/utils"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	db     *bitcask.DB
	rander *rand.Rand
)

func init() {
	opts := bitcask.DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("", "bench-test")
	var err error
	db, err = bitcask.Open(opts)
	if err != nil {
		panic("bench-test fail to open db")
	}
	// 初始化局部随机数生成器
	rander = rand.New(rand.NewSource(time.Now().UnixNano()))
}

func BenchmarkPut(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(1024))
		assert.Nil(b, err)
	}
}

func BenchmarkGet(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(1024))
		assert.Nil(b, err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(rander.Int()))
		if err != nil && err != bitcask.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func BenchmarkDelete(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(1024))
		assert.Nil(b, err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(rander.Int()))
		if err != nil && err != bitcask.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}