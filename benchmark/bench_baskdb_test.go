package benchmark

import (
	bitcask "kv-go/db"
	"kv-go/utils"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
)

var (
	baskDB      *bitcask.DB
	rander      *rand.Rand
	valLen      = 512
	largeValLen = 1024 * 1024
)

func init() {
	opts := bitcask.DefaultDBOptions
	opts.DirPath, _ = os.MkdirTemp("", "bench-baskdb-test")
	var err error
	baskDB, err = bitcask.Open(opts)
	if err != nil {
		panic("bench-test fail to open db")
	}
	// 初始化随机数生成器
	rander = rand.New(rand.NewSource(time.Now().UnixNano()))
	// initBaskDBData(valLen)
}

func BenchmarkAll(b *testing.B) {
    // 先运行 BaskDB 的基准测试
    b.Run("BaskDB/PutValue", Benchmark_PutValue_BaskDB)
    b.Run("BaskDB/GetValue", Benchmark_GetValue_BaskDB)
    b.Run("BaskDB/PutLargeValue", Benchmark_PutLargeValue_BaskDB)
    b.Run("BaskDB/GetLargeValue", Benchmark_GetLargeValue_BaskDB)

    // 然后运行其他数据库的基准测试
    b.Run("Badger/PutValue", Benchmark_PutValue_Badger)
    b.Run("Badger/GetValue", Benchmark_GetValue_Badger)
    b.Run("Badger/PutLargeValue", Benchmark_PutLargeValue_Badger)
    b.Run("Badger/GetLargeValue", Benchmark_GetLargeValue_Badger)

    b.Run("BoltDB/PutValue", Benchmark_PutValue_BoltDB)
    b.Run("BoltDB/GetValue", Benchmark_GetValue_BoltDB)
    b.Run("BoltDB/PutLargeValue", Benchmark_PutLargeValue_BoltDB)
    b.Run("BoltDB/GetLargeValue", Benchmark_GetLargeValue_BoltDB)

    b.Run("GoLevelDB/PutValue", Benchmark_PutValue_GoLevelDB)
    b.Run("GoLevelDB/GetValue", Benchmark_GetValue_GoLevelDB)
    b.Run("GoLevelDB/PutLargeValue", Benchmark_PutLargeValue_GoLevelDB)
    b.Run("GoLevelDB/GetLargeValue", Benchmark_GetLargeValue_GoLevelDB)

    b.Run("RoseDB/PutValue", Benchmark_PutValue_RoseDB)
    b.Run("RoseDB/GetValue", Benchmark_GetValue_RoseDB)
    b.Run("RoseDB/PutLargeValue", Benchmark_PutLargeValue_RoseDB)
    b.Run("RoseDB/GetLargeValue", Benchmark_GetLargeValue_RoseDB)
}

// func initBaskDBData(n int) {
// 	for i := 0; i < dataNum; i++ {
// 		err := baskDB.Put(utils.GetTestKey(rander.Int()), utils.GetTestValue(n))
// 		if err != nil {
// 			log.Fatal(err)
// 		}
// 	}
// }

func Benchmark_PutValue_BaskDB(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := baskDB.Put(utils.GetTestKey(rander.Int()), utils.GetTestValue(valLen))
		if err != nil {
			log.Fatal(err)
		}
	}
}

func Benchmark_GetValue_BaskDB(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := baskDB.Get(utils.GetTestKey(rander.Int()))
		if err != nil && err != bitcask.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_PutLargeValue_BaskDB(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := baskDB.Put(utils.GetTestKey(rander.Int()), utils.GetTestValue(largeValLen))
		if err != nil {
			log.Fatal(err)
		}
	}
}

func Benchmark_GetLargeValue_BaskDB(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := baskDB.Get(utils.GetTestKey(rander.Int()))
		if err != nil && err != bitcask.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}
