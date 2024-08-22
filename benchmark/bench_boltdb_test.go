package benchmark

import (
	"kv-go/utils"
	"math/rand"
	"os"
	"path/filepath"
	"time"
	"testing"

	"go.etcd.io/bbolt"
)

var boltDB *bbolt.DB

func init() {
	opts := bbolt.DefaultOptions
	opts.NoSync = true
	var err error
	dir, _ := os.MkdirTemp("", "bench-boltdb-test")
	boltDB, err = bbolt.Open(filepath.Join(dir, "bolt.data"), 0644, opts)
	if err != nil {
		panic(err)
	}

	boltDB.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket([]byte("test-bucket"))
		if err != nil {
			panic(err)
		}
		return nil
	})
	// 初始化随机数生成器
	rander = rand.New(rand.NewSource(time.Now().UnixNano()))
	// initBoltDBData(valLen)
}

// func initBoltDBData(n int) {
// 	for i := 0; i < 5; i++ {
// 		boltDB.Update(func(tx *bbolt.Tx) error {
// 			for j := 0; j < dataNum/5; j++ {
// 				err := tx.Bucket([]byte("test-bucket")).Put(utils.GetTestKey(rander.Int()), utils.GetTestValue(n))
// 				if err != nil {
// 					panic(err)
// 				}
// 			}
// 			return nil
// 		})
// 	}
// }

func Benchmark_PutValue_BoltDB(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		boltDB.Update(func(tx *bbolt.Tx) error {
			err := tx.Bucket([]byte("test-bucket")).Put(utils.GetTestKey(rander.Int()), utils.GetTestValue(valLen))
			if err != nil {
				panic(err)
			}
			return nil
		})
	}
}

func Benchmark_GetValue_BoltDB(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		boltDB.View(func(tx *bbolt.Tx) error {
			tx.Bucket([]byte("test-bucket")).Get(utils.GetTestKey(rander.Int()))
			return nil
		})
	}
}


func Benchmark_PutLargeValue_BoltDB(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		boltDB.Update(func(tx *bbolt.Tx) error {
			err := tx.Bucket([]byte("test-bucket")).Put(utils.GetTestKey(rander.Int()), utils.GetTestValue(largeValLen))
			if err != nil {
				panic(err)
			}
			return nil
		})
	}
}

func Benchmark_GetLargeValue_BoltDB(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		boltDB.View(func(tx *bbolt.Tx) error {
			tx.Bucket([]byte("test-bucket")).Get(utils.GetTestKey(rander.Int()))
			return nil
		})
	}
}