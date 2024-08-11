package db

import (
	"io"
	"kv-go/data"
	"kv-go/index"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB 数据库实例
type DB struct {
	activeFile   *data.DataFile            // 活跃文件的信息
	inActivaFile map[uint32]*data.DataFile // 不活跃文件的信息
	index        index.Indexer             // key在内存中的索引
	options      DBOptions                 // 用户的配置选项
	mu           *sync.RWMutex             // 用于多线程并发安全
}

// 打开/创建数据库实例
func Open(options DBOptions) (*DB, error) {
	// 检查用户的配置
	if err := checkOptions(options); err != nil {
		return nil, err
	}
	// 检查数据库目录是否存在
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	// 构建 DB 实例
	db := &DB{
		inActivaFile: make(map[uint32]*data.DataFile),
		index:        index.NewIndexer(options.Indexer),
		options:      options,
		mu:           new(sync.RWMutex),
	}
	// 加载data file与index
	if err := db.loadDataFileAndIndex(); err != nil {
		return nil, err
	}
	return db, nil
}

// TODO: Close Sync可以多次调用，没有设置脏位！
// 关闭数据库，返回失败的具体原因
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.activeFile == nil {
		return nil
	}
	// 先持久化活跃文件再关闭
	err := db.activeFile.Sync()
	if err != nil {
		return err
	}
	err = db.activeFile.Close()
	if err != nil {
		return err
	}
	// 因为不活跃文件已经关闭，所以直接关闭即可
	for _, file := range db.inActivaFile {
		err := file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// 持久化数据库，返回失败的具体原因
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	// 只需要持久化活跃文件即可
	err := db.activeFile.Sync()
	if err != nil {
		return err
	}
	return nil
}

// 获取数据库的迭代器
func (db *DB) NewIterator(opts ItOptions) *DBIterator {
	return &DBIterator{
		indexIter: db.index.NewIterator(opts.Reverse),
		db: db,
		opts: opts,
	}
}

// Put 插入k-v到数据库中
func (db *DB) Put(key []byte, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	// key不能为空
	if len(key) == 0 {
		return errEmptyKey
	}
	// 构建要Put的记录
	logRecord := &data.LogRecord{
		Key:           key,
		Value:         value,
		LogRecordType: data.LogRecordNormal,
	}
	// 将记录追加到文件中
	logRecordLog, err := db.appendRecord(logRecord)
	if err != nil {
		return err
	}
	// 根据记录的位置信息 logRecordLog 维护索引
	if !db.index.Put(key, logRecordLog) {
		return errUpdateIndexFailed
	}
	return nil
}

func (db *DB) ListKeys() [][]byte {
	// 获取内存index的迭代器，遍历index
	iter := db.index.NewIterator(false)
	keys := make([][]byte, iter.Size())
	for iter.Rewind(); !iter.IsEnd(); iter.Next() {
		keys = append(keys, iter.Key())
	}
	return keys
}

func (db *DB) Fold(fn func(key []byte, val []byte) bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	// 获取内存index的迭代器，遍历index
	iter := db.index.NewIterator(false)
	for iter.Rewind(); !iter.IsEnd(); iter.Next() {
		logRecordPos := iter.Value()
		val, err := db.GetValueByPos(logRecordPos)
		if err != nil {
			return err
		}
		if !fn(iter.Key(), val) {
			break;
		}
	}
	return nil
}

// Get 获取key对应的value, 若key不存在返回nil
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	// key不能为空
	if len(key) == 0 {
		return nil, errEmptyKey
	}
	// 通过索引获取记录的位置信息 logRecordPos
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, errKeyNotFound
	}
	return db.GetValueByPos(logRecordPos)
}

func (db *DB) GetValueByPos(logRecordPos *data.LogRecordPos) ([]byte, error) {
	// 根据logRecordPos.FileId读取文件
	fileId := logRecordPos.Fid
	var dataFile *data.DataFile
	// 根据fildId获取dataFile
	if fileId == db.activeFile.FileId {
		dataFile = db.activeFile
	} else {
		dataFile = db.inActivaFile[logRecordPos.Fid]
	}
	if dataFile == nil {
		return nil, errDataFileNotFound
	}
	// 读取dataFile
	datas, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	// 这里不应该出现被删除的key，因为被删除后index不存在记录，会返回errKeyNotFound
	// TODO: 抛异常
	if datas.LogRecordType == data.LogRecordDeleted {
		return nil, errDeletedKey
	}
	return datas.Value, nil
}

func (db *DB) Delete(key []byte) error {
	// 不能删除空的key
	if len(key) == 0 {
		return errEmptyKey
	}
	// 向index查询key是否存在(可能被删除，可能本就不存在)
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return errKeyNotFound
	}
	// 构造墓碑值
	logRecord := &data.LogRecord{Key: key, LogRecordType: data.LogRecordDeleted}
	_, err := db.appendRecord(logRecord)
	if err != nil {
		return err
	}
	// 维护index, 这里应该是成功删除，因为之前Get key成功了
	// TODO: 抛异常
	ok := db.index.Delete(key)
	if !ok {
		return errUpdateIndexFailed
	}
	return nil
}

// appendRecord 向文件中追加记录
func (db *DB) appendRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// 第一次写入数据，此时没有活跃文件
	if db.activeFile == nil {
		if err := db.initActiveFile(); err != nil {
			return nil, err
		}
	}
	// 将结构体序列化成[]byte
	encRecord, sz := data.EncodeLogRecord(logRecord)
	// 判断此次写入是否会超出阈值
	if db.activeFile.WriteOff+sz > db.options.DataFileSize {
		// 先保存当前数据文件，持久化+维护inActivaFile
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		db.inActivaFile[db.activeFile.FileId] = db.activeFile
		// 打开新的活跃文件
		if err := db.initActiveFile(); err != nil {
			return nil, err
		}
	}
	// 保存当前 WriteOff
	writeOff := db.activeFile.WriteOff
	// 先当前活跃文件写入encRecord
	if err := db.activeFile.Append(encRecord); err != nil {
		return nil, err
	}
	// 根据配置信息决定是否持久化
	if db.options.AlwaysSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}
	// 构造记录的位置信息LogRecordPos
	return &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
	}, nil
}

// 初始化活跃文件
func (db *DB) initActiveFile() error {
	// fileId从0开始，是一个递增序列
	var fileId uint32 = 0
	if db.activeFile != nil {
		fileId = db.activeFile.FileId + 1
	}
	// 在数据库目录下，创建新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, fileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 加载data file
func (db *DB) loadDataFileAndIndex() error {
	// 读取目录下的所有文件
	dirEntryes, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	// 遍历目录下的所有文件
	var fileIds []int
	for _, entry := range dirEntryes {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splits := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splits[0])
			if err != nil {
				return errDataFileNameCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}
	// 对fileIds进行排序
	sort.Ints(fileIds)
	// 保存data file
	for i, fileId := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fileId))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.inActivaFile[uint32(fileId)] = dataFile
		}
	}
	// 保存index信息
	for i, fileId := range fileIds {
		var dataFile data.DataFile
		if i == len(fileIds)-1 {
			dataFile = *db.activeFile
		} else {
			dataFile = *db.inActivaFile[uint32(fileId)]
		}
		var offset int64 = 0
		for {
			logRecord, sz, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			var ok bool = true
			if logRecord.LogRecordType == data.LogRecordDeleted {
				ok = db.index.Delete(logRecord.Key)
			} else {
				ok = db.index.Put(logRecord.Key, &data.LogRecordPos{
					Fid:    uint32(fileId),
					Offset: offset,
				})
			}
			offset += sz
			if !ok {
				return errUpdateIndexFailed
			}
		}
		// 如果是活跃文件，需要更新WriteOff，记录已经写入的位置
		if i == len(fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	return nil
}

// 检查用户配置的选项
func checkOptions(options DBOptions) error {
	if options.DataFileSize <= 0 {
		return errInvalidDataFileSize
	} else if len(options.DirPath) == 0 {
		return errInvalidDirPath
	}
	return nil
}
