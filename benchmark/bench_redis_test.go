package benchmark

import (
	"context"
	"kv-go/utils"
	"math/rand"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

var rdb *redis.Client

func init() {
	// 初始化Redis客户端
	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379", // 设置Redis服务器地址
		DB:   0,                // 使用默认数据库
	})

	// 测试连接
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		panic(err)
	}

	// 初始化随机数生成器
	rander = rand.New(rand.NewSource(time.Now().UnixNano()))
}

func Benchmark_PutValue_Redis(b *testing.B) {
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := rdb.Set(ctx, string(utils.GetTestKey(rander.Int())), utils.GetTestValue(valLen), 0).Err()
		if err != nil {
			panic(err)
		}
	}
}

func Benchmark_GetValue_Redis(b *testing.B) {
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rdb.Get(ctx, string(utils.GetTestKey(rander.Int())))
	}
}

func Benchmark_PutLargeValue_Redis(b *testing.B) {
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := rdb.Set(ctx, string(utils.GetTestKey(rander.Int())), utils.GetTestValue(largeValLen), 0).Err()
		if err != nil {
			panic(err)
		}
	}
}

func Benchmark_GetLargeValue_Redis(b *testing.B) {
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rdb.Get(ctx, string(utils.GetTestKey(rander.Int())))
	}
}
