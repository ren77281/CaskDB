package benchmark

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"kv-go/utils"

	"github.com/rosedblabs/rosedb/v2"
)

var roseDB *rosedb.DB

func init() {
	dir, _ := os.MkdirTemp("", "bench-goleveldb-test")
	opts := rosedb.DefaultOptions
	opts.DirPath = filepath.Join(dir, "rosedb.data")
	var err error
	roseDB, err = rosedb.Open(opts)
	if err != nil {
		panic(err)
	}
	// 初始化随机数生成器
	rander = rand.New(rand.NewSource(time.Now().UnixNano()))
	// initRoseDBData(valLen)
}

// func initRoseDBData(n int) {
// 	for i := 0; i < dataNum; i++ {
// 		err := roseDB.Put(utils.GetTestKey(rander.Int()), utils.GetTestValue(n))
// 		if err != nil {
// 			panic(err)
// 		}
// 	}
// }
func Benchmark_PutValue_RoseDB(b *testing.B) {
	var durations []int64

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		err := roseDB.Put(utils.GetTestKey(rander.Int()), utils.GetTestValue(valLen))
		if err != nil {
			panic(err)
		}
		elapsed := time.Since(start).Microseconds()
		durations = append(durations, elapsed)
	}

	b.StopTimer()
	reportP99Latency(durations, "PutValue_RoseDB")
}

func Benchmark_GetValue_RoseDB(b *testing.B) {
	var durations []int64

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		_, err := roseDB.Get(utils.GetTestKey(rander.Int()))
		if err != nil && err != rosedb.ErrKeyNotFound {
			panic(err)
		}
		elapsed := time.Since(start).Microseconds()
		durations = append(durations, elapsed)
	}

	b.StopTimer()
	reportP99Latency(durations, "GetValue_RoseDB")
}

func Benchmark_PutLargeValue_RoseDB(b *testing.B) {
	var durations []int64

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		err := roseDB.Put(utils.GetTestKey(rander.Int()), utils.GetTestValue(largeValLen))
		if err != nil {
			panic(err)
		}
		elapsed := time.Since(start).Microseconds()
		durations = append(durations, elapsed)
	}

	b.StopTimer()
	reportP99Latency(durations, "PutLargeValue_RoseDB")
}

func Benchmark_GetLargeValue_RoseDB(b *testing.B) {
	var durations []int64

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		_, err := roseDB.Get(utils.GetTestKey(rander.Int()))
		if err != nil && err != rosedb.ErrKeyNotFound {
			panic(err)
		}
		elapsed := time.Since(start).Microseconds()
		durations = append(durations, elapsed)
	}

	b.StopTimer()
	reportP99Latency(durations, "GetLargeValue_RoseDB")
}
