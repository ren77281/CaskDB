package benchmark

import (
	"kv-go/utils"
	"log"
	"os"
	"time"
	"testing"
	"math/rand"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	levelDb *leveldb.DB
)

func init() {
	dir, _ := os.MkdirTemp("", "bench-goleveldb-test")
	var err error
	levelDb, err = leveldb.OpenFile(dir, nil)
	if err != nil {
		log.Fatal(err)
	}
	// 初始化随机数生成器
	rander = rand.New(rand.NewSource(time.Now().UnixNano()))
	// initLevelDBData(valLen)
}

// func initLevelDBData(n int) {
// 	for i := 0; i < dataNum; i++ {
// 		key := utils.GetTestKey(rander.Int())
// 		val := utils.GetTestValue(n)
// 		err := levelDb.Put(key, val, nil)
// 		if err != nil {
// 			log.Fatal("leveldb write data err.", err)
// 		}
// 	}
// }

func Benchmark_PutValue_GoLevelDB(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := utils.GetTestKey(rander.Int())
		val := utils.GetTestValue(valLen)
		err := levelDb.Put(key, val, &opt.WriteOptions{Sync: false})
		if err != nil {
			log.Fatal("leveldb write data err.", err)
		}
	}
}

func Benchmark_GetValue_GoLevelDB(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := levelDb.Get(utils.GetTestKey(rander.Int()), nil)
		if err != nil && err != leveldb.ErrNotFound {
			log.Fatal("leveldb read data err.", err)
		}
	}
}

func Benchmark_PutLargeValue_GoLevelDB(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := utils.GetTestKey(rander.Int())
		val := utils.GetTestValue(largeValLen)
		err := levelDb.Put(key, val, &opt.WriteOptions{Sync: false})
		if err != nil {
			log.Fatal("leveldb write data err.", err)
		}
	}
}

func Benchmark_GetLargeValue_GoLevelDB(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := levelDb.Get(utils.GetTestKey(rander.Int()), nil)
		if err != nil && err != leveldb.ErrNotFound {
			log.Fatal("leveldb read data err.", err)
		}
	}
}