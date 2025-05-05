package benchmark

import (
	"log"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	bitcask "kv-go/db"
	"kv-go/redis"
	"kv-go/utils"
)

var (
	baskDB      *bitcask.DB
	rander      *rand.Rand
	valLen      = 512
	largeValLen = 4 * 1024
	rds         *redis.RedisDataStructure
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
	rds = &redis.RedisDataStructure{
		Db: baskDB,
	}
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

	// 最后运行 Redis 的基准测试
	b.Run("Redis/PutValue", Benchmark_PutValue_Redis)
	b.Run("Redis/GetValue", Benchmark_GetValue_Redis)
	b.Run("Redis/PutLargeValue", Benchmark_PutLargeValue_Redis)
	b.Run("Redis/GetLargeValue", Benchmark_GetLargeValue_Redis)
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
	var durations []int64

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		err := baskDB.Put(utils.GetTestKey(rander.Int()), utils.GetTestValue(valLen))
		if err != nil {
			log.Fatal(err)
		}
		elapsed := time.Since(start).Microseconds()
		durations = append(durations, elapsed)
	}

	b.StopTimer()
	reportP99Latency(durations, "PutValue_BaskDB")
}

func Benchmark_GetValue_BaskDB(b *testing.B) {
	var durations []int64

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		_, err := baskDB.Get(utils.GetTestKey(rander.Int()))
		if err != nil && err != bitcask.ErrKeyNotFound {
			b.Fatal(err)
		}
		elapsed := time.Since(start).Microseconds()
		durations = append(durations, elapsed)
	}

	b.StopTimer()

	reportP99Latency(durations, "GetValue_BaskDB")
}

func Benchmark_PutLargeValue_BaskDB(b *testing.B) {
	var durations []int64

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		err := baskDB.Put(utils.GetTestKey(rander.Int()), utils.GetTestValue(largeValLen))
		if err != nil {
			log.Fatal(err)
		}
		elapsed := time.Since(start).Microseconds()
		durations = append(durations, elapsed)
	}

	b.StopTimer()
	reportP99Latency(durations, "PutLargeValue_BaskDB")
}

func Benchmark_GetLargeValue_BaskDB(b *testing.B) {
	var durations []int64

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		_, err := baskDB.Get(utils.GetTestKey(rander.Int()))
		if err != nil && err != bitcask.ErrKeyNotFound {
			b.Fatal(err)
		}
		elapsed := time.Since(start).Microseconds()
		durations = append(durations, elapsed)
	}

	b.StopTimer()
	reportP99Latency(durations, "GetLargeValue_BaskDB")
}

func reportP99Latency(durations []int64, label string) {
	if len(durations) == 0 {
		return
	}
	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})
	p99Index := int(float64(len(durations)) * 0.99)
	p99 := durations[p99Index]
	avg := int64(0)
	for _, d := range durations {
		avg += d
	}
	avg /= int64(len(durations))
	log.Printf("\n[P99] %s: P99=%dμs, AVG=%dμs (n=%d)", label, p99, avg, len(durations))
}

func BenchmarkALLTX(b *testing.B) {
	b.Run("BaskDB/PutValue", Benchmark_PutValue_BaskDB)
	b.Run("BaskDB/PutValue_WithTX", Benchmark_PutValue_BaskDB_WithTX)
	b.Run("BaskDB/PutLargeValue", Benchmark_PutLargeValue_BaskDB)
	b.Run("BaskDB/PutLargeValue_WithTX", Benchmark_PutLargeValue_BaskDB_WithTX)

}

func Benchmark_PutValue_BaskDB_WithTX(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	members := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		members[i] = utils.GetTestValue(valLen)
	}
	_, err := rds.SAdd(utils.GetTestKey(rander.Int()), members)
	if err != nil {
		b.Fatal(err)
	}
}

func Benchmark_PutLargeValue_BaskDB_WithTX(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	members := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		members[i] = utils.GetTestValue(largeValLen)
	}
	_, err := rds.SAdd(utils.GetTestKey(rander.Int()), members)
	if err != nil {
		b.Fatal(err)
	}
}
