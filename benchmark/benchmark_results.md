这是BaskDB的基准测试结果，同时测试的还有以下数据库：
- [badger](https://github.com/dgraph-io/badger)
- [bbolt](https://github.com/etcd-io/bbolt)
- [nutsdb](https://github.com/nutsdb/nutsdb)
- [rosedb](https://github.com/rosedblabs/rosedb)
- [goleveldb](https://github.com/syndtr/goleveldb)

```bash
go test -bench=BenchmarkAll -benchtime=1000x
goos: linux
goarch: 386
pkg: kv-go/benchmark
cpu: Intel(R) Xeon(R) CPU E5-2683 v4 @ 2.10GHz
BenchmarkAll/BaskDB/PutValue                1000             14533 ns/op             719 B/op         10 allocs/op
BenchmarkAll/BaskDB/GetValue                1000              2049 ns/op              68 B/op          4 allocs/op
BenchmarkAll/BaskDB/PutLargeValue           1000          30356314 ns/op         4221663 B/op         26 allocs/op
BenchmarkAll/BaskDB/GetLargeValue           1000              1455 ns/op              68 B/op          4 allocs/op
BenchmarkAll/Badger/PutValue                1000             29314 ns/op            1434 B/op         42 allocs/op
BenchmarkAll/Badger/GetValue                1000              4352 ns/op             331 B/op         10 allocs/op
BenchmarkAll/Badger/PutLargeValue           1000          32391833 ns/op         4222932 B/op         69 allocs/op
BenchmarkAll/Badger/GetLargeValue           1000              5183 ns/op             332 B/op         10 allocs/op
BenchmarkAll/BoltDB/PutValue                1000             84911 ns/op            9537 B/op         58 allocs/op
BenchmarkAll/BoltDB/GetValue                1000              4301 ns/op             388 B/op         10 allocs/op
BenchmarkAll/BoltDB/PutLargeValue           1000          15462954 ns/op         2369370 B/op         62 allocs/op
BenchmarkAll/BoltDB/GetLargeValue           1000                63.21 ns/op            0 B/op          0 allocs/op
BenchmarkAll/GoLevelDB/PutValue             1000             12457 ns/op             591 B/op          9 allocs/op
BenchmarkAll/GoLevelDB/GetValue             1000              2283 ns/op             108 B/op          6 allocs/op
BenchmarkAll/GoLevelDB/PutLargeValue        1000         112034984 ns/op        25147585 B/op        503 allocs/op
BenchmarkAll/GoLevelDB/GetLargeValue        1000           7509283 ns/op         4377475 B/op         77 allocs/op
BenchmarkAll/RoseDB/PutValue                1000             38206 ns/op             823 B/op         14 allocs/op
BenchmarkAll/RoseDB/GetValue                1000              3052 ns/op              68 B/op          4 allocs/op
BenchmarkAll/RoseDB/PutLargeValue           1000          31560368 ns/op         3170763 B/op         32 allocs/op
BenchmarkAll/RoseDB/GetLargeValue           1000              1771 ns/op              68 B/op          4 allocs/op
PASS
ok      kv-go/benchmark 230.043s
```