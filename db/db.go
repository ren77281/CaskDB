package db

import (
	"fmt"
	"io"
	"kv-go/data"
	"kv-go/fio"
	"kv-go/index"
	"kv-go/utils"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/gofrs/flock"
)

var (
	fileLockName = "filelock"
)

// DB 数据库实例
type DB struct {
	activeFile     *data.DataFile            // 活跃文件的信息
	inActivaFile   map[uint32]*data.DataFile // 不活跃文件的信息
	index          index.Indexer             // key在内存中的索引
	opts           DBOptions                 // 用户的配置选项
	mu             *sync.RWMutex             // 用于多线程并发安全
	wbId           uint64                    // 用于支持原子写操作，表示事务id
	isMerge        bool                      // 是否正在进行merge操作
	wbIdFileExists bool                      // wbIdFile是否存在
	isInitial      bool                      // 是否第一次初始化数据目录
	fileLock       *flock.Flock              // 用于保持进程互斥的文件锁
	writeBytes     int64                     // 未持久化的字节数
	invalidSize    int64                     // 更新导致的无效数据
}

type DBStat struct {
	KeyNum      int64 // key的数量
	DataFileNum int64 // 使用的数据文件数量
	InvalidSize int64 // 无效数据量(Byte)
	DiskSize    int64 // 占用磁盘的空间(Byte)
}

func (db *DB) Stat() (*DBStat, error) {
	dataFileNum := len(db.inActivaFile)
	if db.activeFile != nil {
		dataFileNum++
	}
	diskSize, err := utils.DirSize(db.opts.DirPath)
	if err != nil {
		return nil, err
	}
	return &DBStat{
		KeyNum: int64(db.index.Size()),
		DataFileNum: int64(dataFileNum),
		InvalidSize: db.invalidSize,
		DiskSize: diskSize,
	}, nil
}

func (db *DB) BackUp(dir string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	exclude := map[string]struct{}{}
	exclude[fileLockName] = struct{}{}
	return utils.CopyDir(db.opts.DirPath, dir, exclude)
}

// 打开/创建数据库实例
func Open(opts DBOptions) (*DB, error) {
	// 检查用户的配置
	if err := checkOptions(opts); err != nil {
		return nil, err
	}
	var isInitial = false
	// 检查数据库目录是否存在
	if _, err := os.Stat(opts.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(opts.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	// 获取文件锁TODO!!!是否要检查文件的存在
	fileLock := flock.New(filepath.Join(opts.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDBUsing
	}
	// 判断是否第一次初始化数据目录
	entrys, err := os.ReadDir(opts.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entrys) == 0 {
		isInitial = true
	}
	// 构建 DB 实例
	db := &DB{
		inActivaFile: make(map[uint32]*data.DataFile),
		index:        index.NewIndexer(opts.Indexer, opts.DirPath, opts.AlwaysSync),
		opts:         opts,
		mu:           new(sync.RWMutex),
		isInitial:    isInitial,
		fileLock:     fileLock,
	}
	// 加载data file与index
	if err := db.loadDataFileAndIndex(opts); err != nil {
		return nil, err
	}
	// 如果用户选择了mmap以加载文件，需要重置IO类型为file IO
	if opts.MMapStartUp {
		if err := db.resetToFileIOType(); err != nil {
			return nil, err
		}
	}
	return db, nil
}

// TODO: Close Sync可以多次调用，没有设置脏位！
// 关闭数据库，返回失败的具体原因
func (db *DB) Close() error {
	// Close的最后释放文件锁
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock file, %v", err))
		}
	}()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.activeFile == nil {
		return nil
	}
	// 先关闭index
	if err := db.index.Close(); err != nil {
		return err
	}
	// 保存wbid到指定文件中
	wbIdFile, err := data.OpenWriteBatchFile(db.opts.DirPath)
	if err != nil {
		return err
	}
	logRecord := &data.LogRecord{
		Key:   []byte(wbIbKey),
		Value: []byte(strconv.FormatUint(db.wbId, 10)),
	}
	encRecord, _ := data.EncodeLogRecord(logRecord)
	if err := wbIdFile.Write(encRecord); err != nil {
		return err
	}
	if err := wbIdFile.Sync(); err != nil {
		return err
	}

	// 先持久化活跃文件再关闭
	err = db.activeFile.Sync()
	if err != nil {
		return err
	}
	err = db.activeFile.Close()
	if err != nil {
		return err
	}
	// 因为不活跃文件已经持久化，所以直接关闭即可
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

// Put 插入k-v到数据库中
func (db *DB) Put(key []byte, value []byte) error {
	// key不能为空
	if len(key) == 0 {
		return ErrEmptyKey
	}
	// 构建要Put的记录
	logRecord := &data.LogRecord{
		Key:   serializeKeyId(key, zeroWbId),
		Value: value,
		Typ:   data.LogRecordNormal,
	}
	// 将记录追加到文件中
	logRecordLog, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	// 根据记录的位置信息 logRecordLog 维护索引
	ok, oldPos := db.index.Put(key, logRecordLog)
	if !ok {
		return ErrUpdateIndexFailed
	}
	if oldPos != nil {
		// 统计无效字节数
		db.invalidSize += int64(oldPos.RecordSize)
	}
	return nil
}

func (db *DB) resetToFileIOType() error {
	if db.activeFile == nil {
		return nil
	}
	err := db.activeFile.ResetIOManager(filepath.Join(
		data.GetDataFileNameById(db.opts.DirPath, db.activeFile.FileId)),
		fio.FileIOType)
	if err != nil {
		return err
	}
	for _, dataFile := range db.inActivaFile {
		err := dataFile.ResetIOManager(
			data.GetDataFileNameById(db.opts.DirPath, dataFile.FileId),
			fio.FileIOType)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) ListKeys(reverse bool) [][]byte {
	// 获取内存index的迭代器，遍历index
	iter := db.index.NewIterator(reverse)
	keys := make([][]byte, 0, db.index.Size())
	for iter.Rewind(); !iter.IsEnd(); iter.Next() {
		keys = append(keys, iter.Key())
	}
	iter.Close()
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
			break
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
		return nil, ErrEmptyKey
	}
	// 通过索引获取记录的位置信息 logRecordPos
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
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
		return nil, ErrDataFileNotFound
	}
	// 读取dataFile
	datas, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	// 这里不应该出现被删除的key，因为被删除后index不存在记录，会返回errKeyNotFound
	// TODO: 抛异常
	if datas.Typ == data.LogRecordDeleted {
		return nil, ErrDeletedKey
	}
	return datas.Value, nil
}

func (db *DB) Delete(key []byte) error {
	// 不能删除空的key
	if len(key) == 0 {
		return ErrEmptyKey
	}
	// 向index查询key是否存在(可能被删除，可能本就不存在)
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return ErrKeyNotFound
	}
	// 构造墓碑值
	logRecord := &data.LogRecord{Key: serializeKeyId(key, zeroWbId), Typ: data.LogRecordDeleted}
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}
	// delete时，追加的record也是无效的
	db.invalidSize += int64(pos.RecordSize)
	// 维护index, 这里应该是成功删除，因为之前Get key成功了
	// TODO: 抛异常
	ok, oldPos := db.index.Delete(key)
	if !ok {
		return ErrUpdateIndexFailed
	}
	if oldPos != nil {
		// 统计无效字节数
		db.invalidSize += int64(oldPos.RecordSize)
	}
	return nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// appendLogRecord 向文件中追加记录
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// 第一次写入数据，此时没有活跃文件
	if db.activeFile == nil {
		if err := db.newActiveFile(); err != nil {
			return nil, err
		}
	}
	// 将结构体序列化成[]byte
	encRecord, sz := data.EncodeLogRecord(logRecord)
	// 判断此次写入是否会超出阈值
	if db.activeFile.WriteOff+sz > db.opts.DataFileSize {
		// 先保存当前数据文件，持久化+维护inActivaFile
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		db.inActivaFile[db.activeFile.FileId] = db.activeFile
		// 打开新的活跃文件
		if err := db.newActiveFile(); err != nil {
			return nil, err
		}
	}
	// 保存当前 WriteOff
	writeOff := db.activeFile.WriteOff
	// 先当前活跃文件写入encRecord
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}
	// 根据配置信息决定是否持久化
	if db.opts.AlwaysSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	} else {
		// 统计未持久化的字节数
		db.writeBytes += sz
		if db.opts.BytesSync > 0 && db.writeBytes >= db.opts.BytesSync {
			// 如果达到持久化阈值就持久化，并重置未持久化的字节数
			db.writeBytes = 0
			if err := db.activeFile.Sync(); err != nil {
				return nil, err
			}
		}
	}
	// 构造记录的位置信息LogRecordPos
	return &data.LogRecordPos{
		Fid:        db.activeFile.FileId,
		Offset:     writeOff,
		RecordSize: uint32(sz),
	}, nil
}

// 创建新的活跃文件（替换当前活跃文件，不会保存！！！）
func (db *DB) newActiveFile() error {
	// fileId从1开始，是一个递增序列
	var fileId uint32 = 1
	if db.activeFile != nil {
		fileId = db.activeFile.FileId + 1
	}
	// 在数据库目录下，创建新的数据文件
	dataFile, err := data.OpenDataFile(db.opts.DirPath, fileId, fio.FileIOType)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 加载data file
func (db *DB) loadDataFileAndIndex(opts DBOptions) error {
	// 优先加载合并后的文件
	if err := db.loadMergeFiles(); err != nil {
		return err
	}
	// 读取目录下的所有文件
	dirEntryes, err := os.ReadDir(db.opts.DirPath)
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
				return ErrDataFileNameCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}
	// 对fileIds进行排序
	sort.Ints(fileIds)
	// 根据DB配置，决定加载文件时的IO类型
	var dataFileIOType fio.IOType
	if opts.MMapStartUp {
		dataFileIOType = fio.MMapIOType
	} else {
		dataFileIOType = fio.FileIOType
	}
	// 加载data file信息
	for i, fileId := range fileIds {
		// 根据fileId打开文件，并加载数据到dataFile中
		dataFile, err := data.OpenDataFile(db.opts.DirPath, uint32(fileId), dataFileIOType)
		if err != nil {
			return err
		}
		// 根据fileId保存数据到不同dataFile中
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.inActivaFile[uint32(fileId)] = dataFile
		}
	}
	if opts.Indexer != index.BPlusTreeType {
		// 加载index信息，先从hint file中加载
		if err := db.loadIndexFromHintFile(); err != nil {
			return err
		}
		if err := db.loadIndexFromDataFile(fileIds); err != nil {
			return err
		}
	} else {
		// B+ tree不用从磁盘加载index，因为它用磁盘存储了index
		// 只需要加载wbIdFile
		if err := db.loadWbIdFile(); err != nil {
			return err
		}
		// 还需要保存活跃文件的writeOff，这里耦合度太高了？？？
		if db.activeFile != nil {
			sz, err := db.activeFile.IOManager.Size()
			if err != nil {
				return err
			}
			// 直接读取文件大小也能获取writeOff
			db.activeFile.WriteOff = sz
		}
	}
	return nil
}

// 加载index信息
func (db *DB) loadIndexFromDataFile(fileIds []int) error {
	// 定义更新/删除index的闭包
	load := func(key []byte, typ data.LogRecordType, logRecordPos *data.LogRecordPos) {
		if typ == data.LogRecordDeleted {
			db.index.Delete(key)
		} else if typ == data.LogRecordNormal {
			db.index.Put(key, logRecordPos)
		} else {
			panic("invalid record type")
		}
	}
	// 暂存WriteBatch的writes, 读到wbfinish时将writes加载到index中，并将其从writes中删除
	writes := make(map[uint64][]*data.WBLogRecord)
	// 维护数据库中最大的wb Id
	maxWbId := zeroWbId
	// 保存merge finish信息
	var hasMerged = false
	var maxMergeFileId uint32 = 0
	mergeFileName := filepath.Join(db.opts.DirPath, data.MergeFilishedFileName)
	if _, err := os.Stat(mergeFileName); err == nil {
		id, err := db.getMaxMergeFileId(db.opts.DirPath)
		if err != nil {
			return err
		}
		maxMergeFileId = id
		hasMerged = true
	}
	// 获取data file中的信息，以加载index
	for i, fileId := range fileIds {
		// 已经从hintfile中获取索引信息，无需读取data file
		if hasMerged && fileId <= int(maxMergeFileId) {
			continue
		}
		var dataFile data.DataFile
		if i == len(fileIds)-1 {
			dataFile = *db.activeFile
		} else {
			dataFile = *db.inActivaFile[uint32(fileId)]
		}
		var offset int64 = 0
		// 读取dataFile中的所有LogRecord
		for {
			logRecord, sz, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 构造logRecordPos
			logRecordPos := &data.LogRecordPos{
				Fid:        uint32(fileId),
				Offset:     offset,
				RecordSize: uint32(sz),
			}
			// 解析key获取wbId
			realKey, wbId := parseKeyId(logRecord.Key)
			// 根据wbId判断该记录是否是一个wb操作
			if wbId == zeroWbId {
				// 不是wb操作，正常加载index
				load(realKey, logRecord.Typ, logRecordPos)
			} else {
				// 是wb操作，根据record类型决定是更新索引还是暂存record信息
				// 先更新maxWbId
				maxWbId = max(maxWbId, wbId)
				if logRecord.Typ == data.LogRecordFinished {
					// 更新索引
					for _, record := range writes[wbId] {
						load(record.Key, record.Typ, record.Pos)
					}
					delete(writes, wbId)
				} else {
					// 暂存record信息
					writes[wbId] = append(writes[wbId], &data.WBLogRecord{
						Key: realKey,
						Pos: logRecordPos,
						Typ: logRecord.Typ,
					})
				}
			}
			offset += sz
		}
		// 如果是活跃文件，需要更新WriteOff
		if i == len(fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	// 最后更新wbId
	db.wbId = maxWbId
	return nil
}

// 从指定文件中加载wbid
func (db *DB) loadWbIdFile() error {
	fileName := filepath.Join(db.opts.DirPath, data.NextWriteBatchIdFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}
	// 打开指定文件并读取record，提取出wbid
	wbIdFile, err := data.OpenWriteBatchFile(db.opts.DirPath)
	if err != nil {
		return err
	}
	logRecord, _, err := wbIdFile.ReadLogRecord(0)
	if err != nil {
		return err
	}
	id, err := strconv.ParseUint(string(logRecord.Value), 10, 64)
	if err != nil {
		return err
	}
	db.wbId = id
	db.wbIdFileExists = true
	// TODO!!! 这里删除wbIdFile是否有必要？
	return os.Remove(fileName)
}

func (db *DB) NewWriteBatch(opts WBOptions) *WriteBatch {
	if db.opts.Indexer == index.BPlusTreeType && !db.wbIdFileExists && !db.isInitial {
		return nil
	}
	return &WriteBatch{
		opts:          opts,
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
		mu:            new(sync.Mutex),
	}
}

// 检查用户配置的选项
func checkOptions(opts DBOptions) error {
	if opts.DataFileSize <= 0 {
		return ErrInvalidDataFileSize
	}
	if len(opts.DirPath) == 0 {
		return ErrInvalidDirPath
	}
	if opts.MergeRatio < 0 || opts.MergeRatio > 1 {
		return ErrInvalidMergeRatio
	}
	return nil
}
