## TestDB
这是BaskDB的基准测试结果，同时测试的还有以下数据库：
- [badger](https://github.com/dgraph-io/badger)
- [bbolt](https://github.com/etcd-io/bbolt)
- [nutsdb](https://github.com/nutsdb/nutsdb)
- [goleveldb](https://github.com/syndtr/goleveldb)
- [rosedb](https://github.com/rosedblabs/rosedb)
## Options
- key size:         14 byte
- value size:       512 byte
- large value size: 4 * 1024 byte
## Result
```bash
go test -bench=BenchmarkAll -benchtime=10000x
goos: linux
goarch: 386
pkg: kv-go/benchmark
cpu: Intel(R) Xeon(R) CPU E5-2683 v4 @ 2.10GHz
BenchmarkAll/BaskDB/PutValue               10000             22322 ns/op            2366 B/op         10 allocs/op
BenchmarkAll/BaskDB/GetValue               10000              1797 ns/op              68 B/op          4 allocs/op
BenchmarkAll/BaskDB/PutLargeValue          10000            141831 ns/op           18821 B/op         10 allocs/op
BenchmarkAll/BaskDB/GetLargeValue          10000              1704 ns/op              68 B/op          4 allocs/op
BenchmarkAll/Badger/PutValue               10000             36982 ns/op            2698 B/op         44 allocs/op
BenchmarkAll/Badger/GetValue               10000              5423 ns/op             332 B/op         10 allocs/op
BenchmarkAll/Badger/PutLargeValue          10000            166108 ns/op           14878 B/op         45 allocs/op
BenchmarkAll/Badger/GetLargeValue          10000              6784 ns/op             332 B/op         11 allocs/op
BenchmarkAll/BoltDB/PutValue               10000             78604 ns/op           13686 B/op         99 allocs/op
BenchmarkAll/BoltDB/GetValue               10000              4232 ns/op             533 B/op         22 allocs/op
BenchmarkAll/BoltDB/PutLargeValue          10000            237312 ns/op           46834 B/op        107 allocs/op
BenchmarkAll/BoltDB/GetLargeValue          10000              5611 ns/op             526 B/op         21 allocs/op
BenchmarkAll/GoLevelDB/PutValue            10000             35526 ns/op            2297 B/op          9 allocs/op
BenchmarkAll/GoLevelDB/GetValue            10000              9127 ns/op            1523 B/op         15 allocs/op
BenchmarkAll/GoLevelDB/PutLargeValue       10000            232223 ns/op           15178 B/op         12 allocs/op
BenchmarkAll/GoLevelDB/GetLargeValue       10000            105240 ns/op           13799 B/op         43 allocs/op
BenchmarkAll/RoseDB/PutValue               10000             33861 ns/op            2069 B/op         14 allocs/op
BenchmarkAll/RoseDB/GetValue               10000              2991 ns/op              68 B/op          4 allocs/op
BenchmarkAll/RoseDB/PutLargeValue          10000            138470 ns/op           14239 B/op         14 allocs/op
BenchmarkAll/RoseDB/GetLargeValue          10000              3766 ns/op              68 B/op          4 allocs/op
BenchmarkAll/Redis/PutValue                10000            185856 ns/op            1884 B/op         15 allocs/op
BenchmarkAll/Redis/GetValue                10000            129911 ns/op             163 B/op         10 allocs/op
BenchmarkAll/Redis/PutLargeValue           10000            311454 ns/op           14055 B/op         15 allocs/op
BenchmarkAll/Redis/GetLargeValue           10000            104030 ns/op             161 B/op         10 allocs/op
PASS
ok      kv-go/benchmark 20.859s
```

```bash
go test -bench=BenchmarkAll
goos: linux
goarch: 386
pkg: kv-go/benchmark
cpu: Intel(R) Xeon(R) CPU E5-2683 v4 @ 2.10GHz
BenchmarkAll/BaskDB/PutValue               49093             27989 ns/op            2368 B/op         10 allocs/op
BenchmarkAll/BaskDB/GetValue              442052              2596 ns/op              68 B/op          4 allocs/op
BenchmarkAll/BaskDB/PutLargeValue           9453            140729 ns/op           18829 B/op         10 allocs/op
BenchmarkAll/BaskDB/GetLargeValue         446866              2425 ns/op              68 B/op          4 allocs/op
BenchmarkAll/Badger/PutValue               26706             47929 ns/op            2708 B/op         45 allocs/op
BenchmarkAll/Badger/GetValue              175374              6408 ns/op             332 B/op         11 allocs/op
BenchmarkAll/Badger/PutLargeValue           7862            186935 ns/op           14880 B/op         45 allocs/op
BenchmarkAll/Badger/GetLargeValue         160528              7393 ns/op             332 B/op         11 allocs/op
BenchmarkAll/BoltDB/PutValue               14750             93017 ns/op           14514 B/op        105 allocs/op
BenchmarkAll/BoltDB/GetValue              275037              5099 ns/op             534 B/op         22 allocs/op
BenchmarkAll/BoltDB/PutLargeValue           5847            282930 ns/op           44532 B/op        110 allocs/op
BenchmarkAll/BoltDB/GetLargeValue         205180              5114 ns/op             558 B/op         25 allocs/op
BenchmarkAll/GoLevelDB/PutValue            41610             37877 ns/op            1980 B/op          9 allocs/op
BenchmarkAll/GoLevelDB/GetValue            70051             18626 ns/op            1145 B/op         18 allocs/op
BenchmarkAll/GoLevelDB/PutLargeValue        5928            238017 ns/op           14827 B/op         13 allocs/op
BenchmarkAll/GoLevelDB/GetLargeValue       10000            111256 ns/op           13223 B/op         44 allocs/op
BenchmarkAll/RoseDB/PutValue               41217             26532 ns/op            2068 B/op         14 allocs/op
BenchmarkAll/RoseDB/GetValue              391232              3158 ns/op              68 B/op          4 allocs/op
BenchmarkAll/RoseDB/PutLargeValue           7299            165088 ns/op           26335 B/op         20 allocs/op
BenchmarkAll/RoseDB/GetLargeValue         351598              3228 ns/op              68 B/op          4 allocs/op
BenchmarkAll/Redis/PutValue                10000            120964 ns/op            1878 B/op         15 allocs/op
BenchmarkAll/Redis/GetValue                10000            107153 ns/op             161 B/op         10 allocs/op
BenchmarkAll/Redis/PutLargeValue            4053            304567 ns/op           14054 B/op         15 allocs/op
BenchmarkAll/Redis/GetLargeValue           10000            107535 ns/op             161 B/op         10 allocs/op
PASS
ok      kv-go/benchmark 38.347s
```