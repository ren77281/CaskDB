## TestDB
这是BaskDB的基准测试结果，同时测试的还有以下数据库：
- [badger](https://github.com/dgraph-io/badger)
- [bbolt](https://github.com/etcd-io/bbolt)
- [nutsdb](https://github.com/nutsdb/nutsdb)
- [goleveldb](https://github.com/syndtr/goleveldb)
- [rosedb](https://github.com/rosedblabs/rosedb)
- [redis](https://github.com/redis/redis)

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

```bash
bench=BenchmarkALLTX -benchtime=10000x
2025/05/02 17:09:55 
[P99] PutValue_BaskDB: P99=280μs, AVG=280μs (n=1)
goos: darwin
goarch: amd64
pkg: kv-go/benchmark
cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
BenchmarkALLTX/BaskDB/PutValue-12               2025/05/02 17:09:55 
[P99] PutValue_BaskDB: P99=31μs, AVG=13μs (n=10000)
   10000             13959 ns/op            2483 B/op         10 allocs/op
BenchmarkALLTX/BaskDB/PutValue_WithTX-12           10000             17928 ns/op            6149 B/op         15 allocs/op
2025/05/02 17:09:55 
[P99] PutLargeValue_BaskDB: P99=179μs, AVG=179μs (n=1)
BenchmarkALLTX/BaskDB/PutLargeValue-12          2025/05/02 17:09:56 
[P99] PutLargeValue_BaskDB: P99=99μs, AVG=55μs (n=10000)
   10000             55739 ns/op           18938 B/op         10 allocs/op
BenchmarkALLTX/BaskDB/PutLargeValue_WithTX-12              10000             71025 ns/op           48333 B/op         15 allocs/op
PASS
ok      kv-go/benchmark 1.886s
```

```bash
2025/05/02 16:35:47 
[P99] PutValue_BaskDB: P99=307μs, AVG=307μs (n=1)
goos: darwin
goarch: amd64
pkg: kv-go/benchmark
cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
BenchmarkAll/BaskDB/PutValue-12                 2025/05/02 16:35:48 
[P99] PutValue_BaskDB: P99=30μs, AVG=13μs (n=10000)
   10000             13720 ns/op            2483 B/op         10 allocs/op
2025/05/02 16:35:48 
[P99] GetValue_BaskDB: P99=16μs, AVG=16μs (n=1)
BenchmarkAll/BaskDB/GetValue-12                 2025/05/02 16:35:48 
[P99] GetValue_BaskDB: P99=1μs, AVG=0μs (n=10000)
   10000               885.3 ns/op           139 B/op          4 allocs/op
2025/05/02 16:35:48 
[P99] PutLargeValue_BaskDB: P99=89μs, AVG=89μs (n=1)
BenchmarkAll/BaskDB/PutLargeValue-12            2025/05/02 16:35:48 
[P99] PutLargeValue_BaskDB: P99=111μs, AVG=60μs (n=10000)
   10000             60712 ns/op           18935 B/op         10 allocs/op
2025/05/02 16:35:48 
[P99] GetLargeValue_BaskDB: P99=14μs, AVG=14μs (n=1)
BenchmarkAll/BaskDB/GetLargeValue-12            2025/05/02 16:35:48 
[P99] GetLargeValue_BaskDB: P99=1μs, AVG=0μs (n=10000)
   10000               850.3 ns/op           139 B/op          4 allocs/op
2025/05/02 16:35:48 
[P99] PutValue_Badger: P99=82μs, AVG=82μs (n=1)
BenchmarkAll/Badger/PutValue-12                 2025/05/02 16:35:48 
[P99] PutValue_Badger: P99=41μs, AVG=18μs (n=10000)
   10000             19206 ns/op            3229 B/op         44 allocs/op
2025/05/02 16:35:48 
[P99] GetValue_Badger: P99=31μs, AVG=31μs (n=1)
BenchmarkAll/Badger/GetValue-12                 2025/05/02 16:35:48 
[P99] GetValue_Badger: P99=6μs, AVG=1μs (n=10000)
   10000              2032 ns/op             532 B/op         11 allocs/op
2025/05/02 16:35:48 
[P99] PutLargeValue_Badger: P99=102μs, AVG=102μs (n=1)
BenchmarkAll/Badger/PutLargeValue-12            2025/05/02 16:35:49 
[P99] PutLargeValue_Badger: P99=110μs, AVG=61μs (n=10000)
   10000             62475 ns/op           15405 B/op         45 allocs/op
2025/05/02 16:35:49 
[P99] GetLargeValue_Badger: P99=45μs, AVG=45μs (n=1)
BenchmarkAll/Badger/GetLargeValue-12            2025/05/02 16:35:49 
[P99] GetLargeValue_Badger: P99=7μs, AVG=1μs (n=10000)
   10000              2319 ns/op             532 B/op         11 allocs/op
2025/05/02 16:35:49 
[P99] PutValue_BoltDB: P99=75μs, AVG=75μs (n=1)
BenchmarkAll/BoltDB/PutValue-12                 2025/05/02 16:35:50 
[P99] PutValue_BoltDB: P99=102μs, AVG=61μs (n=10000)
   10000             62518 ns/op           17390 B/op         99 allocs/op
2025/05/02 16:35:50 
[P99] GetValue_BoltDB: P99=27μs, AVG=27μs (n=1)
BenchmarkAll/BoltDB/GetValue-12                 2025/05/02 16:35:50 
[P99] GetValue_BoltDB: P99=5μs, AVG=1μs (n=10000)
   10000              2148 ns/op             745 B/op         22 allocs/op
2025/05/02 16:35:50 
[P99] PutLargeValue_BoltDB: P99=158μs, AVG=158μs (n=1)
BenchmarkAll/BoltDB/PutLargeValue-12            2025/05/02 16:35:51 
[P99] PutLargeValue_BoltDB: P99=220μs, AVG=129μs (n=10000)
   10000            129881 ns/op           51868 B/op        113 allocs/op
2025/05/02 16:35:51 
[P99] GetLargeValue_BoltDB: P99=29μs, AVG=29μs (n=1)
BenchmarkAll/BoltDB/GetLargeValue-12            2025/05/02 16:35:51 
[P99] GetLargeValue_BoltDB: P99=7μs, AVG=2μs (n=10000)
   10000              3221 ns/op             766 B/op         25 allocs/op
2025/05/02 16:35:51 
[P99] PutValue_GoLevelDB: P99=154μs, AVG=154μs (n=1)
BenchmarkAll/GoLevelDB/PutValue-12              2025/05/02 16:35:51 
[P99] PutValue_GoLevelDB: P99=40μs, AVG=19μs (n=10000)
   10000             20105 ns/op            2496 B/op          9 allocs/op
2025/05/02 16:35:51 
[P99] GetValue_GoLevelDB: P99=217μs, AVG=217μs (n=1)
BenchmarkAll/GoLevelDB/GetValue-12              2025/05/02 16:35:51 
[P99] GetValue_GoLevelDB: P99=12μs, AVG=3μs (n=10000)
   10000              4088 ns/op            1890 B/op         15 allocs/op
2025/05/02 16:35:51 
[P99] PutLargeValue_GoLevelDB: P99=116μs, AVG=116μs (n=1)
BenchmarkAll/GoLevelDB/PutLargeValue-12         2025/05/02 16:35:53 
[P99] PutLargeValue_GoLevelDB: P99=216μs, AVG=164μs (n=10000)
   10000            165572 ns/op           15492 B/op         12 allocs/op
2025/05/02 16:35:53 
[P99] GetLargeValue_GoLevelDB: P99=600μs, AVG=600μs (n=1)
BenchmarkAll/GoLevelDB/GetLargeValue-12         2025/05/02 16:35:53 
[P99] GetLargeValue_GoLevelDB: P99=94μs, AVG=36μs (n=10000)
   10000             37393 ns/op           29317 B/op         75 allocs/op
2025/05/02 16:35:53 
[P99] PutValue_RoseDB: P99=265μs, AVG=265μs (n=1)
BenchmarkAll/RoseDB/PutValue-12                 2025/05/02 16:35:54 
[P99] PutValue_RoseDB: P99=59μs, AVG=39μs (n=10000)
   10000             39968 ns/op            2711 B/op         15 allocs/op
2025/05/02 16:35:54 
[P99] GetValue_RoseDB: P99=30μs, AVG=30μs (n=1)
BenchmarkAll/RoseDB/GetValue-12                 2025/05/02 16:35:54 
[P99] GetValue_RoseDB: P99=4μs, AVG=0μs (n=10000)
   10000              1442 ns/op             146 B/op          4 allocs/op
2025/05/02 16:35:54 
[P99] PutLargeValue_RoseDB: P99=98μs, AVG=98μs (n=1)
BenchmarkAll/RoseDB/PutLargeValue-12            2025/05/02 16:35:55 
[P99] PutLargeValue_RoseDB: P99=118μs, AVG=65μs (n=10000)
   10000             66591 ns/op           14538 B/op         14 allocs/op
2025/05/02 16:35:55 
[P99] GetLargeValue_RoseDB: P99=24μs, AVG=24μs (n=1)
BenchmarkAll/RoseDB/GetLargeValue-12            2025/05/02 16:35:55 
[P99] GetLargeValue_RoseDB: P99=2μs, AVG=0μs (n=10000)
   10000              1174 ns/op             140 B/op          4 allocs/op
2025/05/02 16:35:55 
[P99] PutValue_Redis: P99=211μs, AVG=211μs (n=1)
BenchmarkAll/Redis/PutValue-12                  2025/05/02 16:35:56 
[P99] PutValue_Redis: P99=189μs, AVG=111μs (n=10000)
   10000            111985 ns/op            2067 B/op         15 allocs/op
2025/05/02 16:35:56 
[P99] GetValue_Redis: P99=140μs, AVG=140μs (n=1)
BenchmarkAll/Redis/GetValue-12                  2025/05/02 16:35:57 
[P99] GetValue_Redis: P99=136μs, AVG=93μs (n=10000)
   10000             94792 ns/op             329 B/op         10 allocs/op
2025/05/02 16:35:57 
[P99] PutLargeValue_Redis: P99=260μs, AVG=260μs (n=1)
BenchmarkAll/Redis/PutLargeValue-12             2025/05/02 16:35:59 
[P99] PutLargeValue_Redis: P99=281μs, AVG=198μs (n=10000)
   10000            199422 ns/op           14240 B/op         15 allocs/op
2025/05/02 16:35:59 
[P99] GetLargeValue_Redis: P99=15547μs, AVG=15547μs (n=1)
BenchmarkAll/Redis/GetLargeValue-12             2025/05/02 16:36:00 
[P99] GetLargeValue_Redis: P99=128μs, AVG=85μs (n=10000)
   10000             86664 ns/op             328 B/op         10 allocs/op
PASS
ok      kv-go/benchmark 12.366s
```