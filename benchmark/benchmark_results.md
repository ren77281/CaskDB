## TestDB
这是BaskDB的基准测试结果，同时测试的还有以下数据库：
- [badger](https://github.com/dgraph-io/badger)
- [bbolt](https://github.com/etcd-io/bbolt)
- [nutsdb](https://github.com/nutsdb/nutsdb)
- [rosedb](https://github.com/rosedblabs/rosedb)
- [goleveldb](https://github.com/syndtr/goleveldb)
## Options
- key size:         14 byte
- value size:       128 byte
- large value size: 1024 * 1024 byte
## Result
```bash
go test -bench=BenchmarkAll -benchtime=1000x
goos: linux
goarch: 386
pkg: kv-go/benchmark
cpu: Intel(R) Xeon(R) CPU E5-2683 v4 @ 2.10GHz
BenchmarkAll/BaskDB/PutValue                1000             16506 ns/op             719 B/op         10 allocs/op
BenchmarkAll/BaskDB/GetValue                1000              1246 ns/op              68 B/op          4 allocs/op
BenchmarkAll/BaskDB/PutLargeValue           1000          30227818 ns/op         4221633 B/op         26 allocs/op
BenchmarkAll/BaskDB/GetLargeValue           1000              1234 ns/op              68 B/op          4 allocs/op
BenchmarkAll/Badger/PutValue                1000             29047 ns/op            1435 B/op         42 allocs/op
BenchmarkAll/Badger/GetValue                1000              4288 ns/op             331 B/op         10 allocs/op
BenchmarkAll/Badger/PutLargeValue           1000          30548028 ns/op         4222768 B/op         68 allocs/op
BenchmarkAll/Badger/GetLargeValue           1000              4737 ns/op             331 B/op         10 allocs/op
BenchmarkAll/BoltDB/PutValue                1000             60662 ns/op            9578 B/op         58 allocs/op
BenchmarkAll/BoltDB/GetValue                1000              4712 ns/op             388 B/op         10 allocs/op
BenchmarkAll/BoltDB/PutLargeValue           1000          32373241 ns/op         5137906 B/op        109 allocs/op
BenchmarkAll/BoltDB/GetLargeValue           1000             65.48 ns/op               0 B/op          0 allocs/op
BenchmarkAll/GoLevelDB/PutValue             1000             17080 ns/op             593 B/op          9 allocs/op
BenchmarkAll/GoLevelDB/GetValue             1000              4296 ns/op             108 B/op          6 allocs/op
BenchmarkAll/GoLevelDB/PutLargeValue        1000          99599559 ns/op        25552241 B/op        504 allocs/op
BenchmarkAll/GoLevelDB/GetLargeValue        1000           6459305 ns/op         3792580 B/op         70 allocs/op
BenchmarkAll/RoseDB/PutValue                1000             19620 ns/op             820 B/op         14 allocs/op
BenchmarkAll/RoseDB/GetValue                1000              3498 ns/op              68 B/op          4 allocs/op
BenchmarkAll/RoseDB/PutLargeValue           1000          29533991 ns/op         3170582 B/op         30 allocs/op
BenchmarkAll/RoseDB/GetLargeValue           1000              1938 ns/op              68 B/op          4 allocs/op
PASS
ok      kv-go/benchmark 229.428s
```