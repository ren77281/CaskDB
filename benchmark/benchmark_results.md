## TestDB
这是BaskDB的基准测试结果，同时测试的还有以下数据库：
- [badger](https://github.com/dgraph-io/badger)
- [bbolt](https://github.com/etcd-io/bbolt)
- [nutsdb](https://github.com/nutsdb/nutsdb)
- [rosedb](https://github.com/rosedblabs/rosedb)
- [goleveldb](https://github.com/syndtr/goleveldb)
## Options
- key size:         14 byte
- value size:       512 byte
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

```bash
go test -bench=BenchmarkAll
goos: linux
goarch: 386
pkg: kv-go/benchmark
cpu: Intel(R) Xeon(R) CPU E5-2683 v4 @ 2.10GHz
BenchmarkAll/BaskDB/PutValue               48248             30762 ns/op            2368 B/op         10 allocs/op
BenchmarkAll/BaskDB/GetValue              391124              2918 ns/op              68 B/op          4 allocs/op
BenchmarkAll/BaskDB/PutLargeValue             37          33754858 ns/op         4222001 B/op         29 allocs/op
BenchmarkAll/BaskDB/GetLargeValue         417996              2757 ns/op              68 B/op          4 allocs/op
BenchmarkAll/Badger/PutValue               24774             53033 ns/op            2708 B/op         45 allocs/op
BenchmarkAll/Badger/GetValue              170787              6629 ns/op             332 B/op         11 allocs/op
BenchmarkAll/Badger/PutLargeValue             37          31567100 ns/op         4222843 B/op         68 allocs/op
BenchmarkAll/Badger/GetLargeValue         194761              7935 ns/op             332 B/op         11 allocs/op
BenchmarkAll/BoltDB/PutValue               15968             82257 ns/op           14609 B/op        105 allocs/op
BenchmarkAll/BoltDB/GetValue              260457              4851 ns/op             535 B/op         22 allocs/op
BenchmarkAll/BoltDB/PutLargeValue             46          36166904 ns/op         4309341 B/op        138 allocs/op
BenchmarkAll/BoltDB/GetLargeValue         205400              5215 ns/op             535 B/op         22 allocs/op
BenchmarkAll/GoLevelDB/PutValue            41792             33943 ns/op            1978 B/op          9 allocs/op
BenchmarkAll/GoLevelDB/GetValue            71870             17803 ns/op            1144 B/op         18 allocs/op
BenchmarkAll/GoLevelDB/PutLargeValue          32          63807324 ns/op         6902116 B/op       1211 allocs/op
BenchmarkAll/GoLevelDB/GetLargeValue         792           2038169 ns/op         2459654 B/op         69 allocs/op
BenchmarkAll/RoseDB/PutValue               38895             33369 ns/op            2069 B/op         14 allocs/op
BenchmarkAll/RoseDB/GetValue              457434              2489 ns/op              68 B/op          4 allocs/op
BenchmarkAll/RoseDB/PutLargeValue             34          35381570 ns/op         9287073 B/op         52 allocs/op
BenchmarkAll/RoseDB/GetLargeValue         425936              2754 ns/op              68 B/op          4 allocs/op
PASS
ok      kv-go/benchmark 36.091s
```