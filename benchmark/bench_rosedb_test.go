package benchmark

import (
	"kv-go/utils"
	"path/filepath"
	"testing"
	"os"
	"time"
	"math/rand"

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
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := roseDB.Put(utils.GetTestKey(rander.Int()), utils.GetTestValue(valLen))
		if err != nil {
			panic(err)
		}
	}
}

func Benchmark_GetValue_RoseDB(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := roseDB.Get(utils.GetTestKey(rander.Int()))
		if err != nil && err != rosedb.ErrKeyNotFound {
			panic(err)
		}
	}
}

func Benchmark_PutLargeValue_RoseDB(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := roseDB.Put(utils.GetTestKey(rander.Int()), utils.GetTestValue(largeValLen))
		if err != nil {
			panic(err)
		}
	}
}

func Benchmark_GetLargeValue_RoseDB(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := roseDB.Get(utils.GetTestKey(rander.Int()))
		if err != nil && err != rosedb.ErrKeyNotFound {
			panic(err)
		}
	}
}