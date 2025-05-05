package benchmark

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"kv-go/utils"

	"github.com/dgraph-io/badger/v3"
)

var (
	badgerdb *badger.DB
)

func init() {
	dir, _ := os.MkdirTemp("", "bench-boltdb-test")
	opts := badger.DefaultOptions(filepath.Join(dir, "badger.data")).WithLoggingLevel(badger.ERROR) // 仅记录错误级别的日志
	opts.SyncWrites = false
	var err error
	badgerdb, err = badger.Open(opts)
	if err != nil {
		panic(err)
	}
	// 初始化随机数生成器
	rander = rand.New(rand.NewSource(time.Now().UnixNano()))
	// initBadgerDBData(valLen)
}

// func initBadgerDBData(n int) {
// 	for i := 0; i < dataNum; i++ {
// 		badgerdb.Update(func(txn *badger.Txn) error {
// 			return txn.Set(utils.GetTestKey(rander.Int()), utils.GetTestValue(n))
// 		})
// 	}
// }

func Benchmark_PutValue_Badger(b *testing.B) {
	var durations []int64

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		start := time.Now()
		err := badgerdb.Update(func(txn *badger.Txn) error {
			return txn.Set(utils.GetTestKey(rander.Int()), utils.GetTestValue(valLen))
		})
		if err != nil {
			b.Fatal(err)
		}
		elapsed := time.Since(start).Microseconds()
		durations = append(durations, elapsed)
	}

	b.StopTimer()
	reportP99Latency(durations, "PutValue_Badger")
}

func Benchmark_GetValue_Badger(b *testing.B) {
	var durations []int64

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		start := time.Now()
		err := badgerdb.View(func(txn *badger.Txn) error {
			_, err := txn.Get(utils.GetTestKey(rander.Int()))
			return err
		})
		if err != nil && err != badger.ErrKeyNotFound {
			b.Fatal(err)
		}
		elapsed := time.Since(start).Microseconds()
		durations = append(durations, elapsed)
	}

	b.StopTimer()
	reportP99Latency(durations, "GetValue_Badger")
}

func Benchmark_PutLargeValue_Badger(b *testing.B) {
	var durations []int64

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		start := time.Now()
		err := badgerdb.Update(func(txn *badger.Txn) error {
			return txn.Set(utils.GetTestKey(rander.Int()), utils.GetTestValue(largeValLen))
		})
		if err != nil {
			b.Fatal(err)
		}
		elapsed := time.Since(start).Microseconds()
		durations = append(durations, elapsed)
	}

	b.StopTimer()
	reportP99Latency(durations, "PutLargeValue_Badger")
}

func Benchmark_GetLargeValue_Badger(b *testing.B) {
	var durations []int64

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		start := time.Now()
		err := badgerdb.View(func(txn *badger.Txn) error {
			_, err := txn.Get(utils.GetTestKey(rander.Int()))
			return err
		})
		if err != nil && err != badger.ErrKeyNotFound {
			b.Fatal(err)
		}
		elapsed := time.Since(start).Microseconds()
		durations = append(durations, elapsed)
	}

	b.StopTimer()
	reportP99Latency(durations, "GetLargeValue_Badger")
}
