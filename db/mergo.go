package db

import (
	"io"
	"kv-go/data"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "finish"
)

func (db *DB) merge() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	// 同一时间只能有一个线程在merge，当然，检查之前需要上锁
	if db.isMerge {
		db.mu.Unlock()
		return ErrDBMerging
	}
	// 设置isMerge
	db.isMerge = true
	defer func() {
		db.isMerge = false
	}()
	// 将当前活跃文件关闭，保存为不活跃文件
	// 先持久化并保存该文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	// 保存merge过的最大file id
	maxMergeFileId := db.activeFile.FileId
	db.inActivaFile[db.activeFile.FileId] = db.activeFile
	// 创建新的文件以替换当前活跃文件
	if err := db.newActiveFile(); err != nil {
		db.mu.Unlock()
		return err
	}
	// 保存所有不活跃文件
	dataFiles := make([]*data.DataFile, 0, len(db.inActivaFile))
	for _, dataFile := range db.inActivaFile {
		dataFiles = append(dataFiles, dataFile)
	}
	// 解db锁
	db.mu.Unlock()
	// 将数据文件以fileId排序
	sort.Slice(dataFiles, func(i, j int) bool {
		return dataFiles[i].FileId < dataFiles[j].FileId
	})
	// 如果之前merge过，需要先删除用来merge的目录
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	// 新建一个用来merge的目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	// 打开新的db实例
	mergeOpts := DefaultDBOptions
	mergeOpts.AlwaysSync = false
	mergeOpts.DirPath = mergePath
	mergeDB, err := Open(mergeOpts)
	if err != nil {
		return err
	}
	// 打开hint文件
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}
	// 遍历，加载所有dataFile
	for _, datafile := range dataFiles {
		var off int64 = 0
		// 读取其所有record
		for {
			logRecord, sz, err := datafile.ReadLogRecord(off)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 解析key
			realKey, _ := parseKeyId(logRecord.Key)
			// 根据realKey在index中查找
			logRecordPos := db.index.Get(realKey)
			// 如果是最新的record，需要重写
			if logRecordPos != nil && logRecordPos.Fid == datafile.FileId && logRecordPos.Offset == off {
				// 直接db.Put会获取锁，这是无意义的操作
				// 而且也会更新index，我们不需要更新index，所以手动append
				// 在append之前，需要擦除record的wbId
				logRecord.Key = serializeKeyId(realKey, zeroWbId)
				newLogRecordPos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				// 维护hint file, 这里需要写入realKey-encPos
				encLogRecordPos := data.EncodeLogRecordPos(newLogRecordPos)
				encLogRecord, _ := data.EncodeLogRecord(&data.LogRecord{
					Key:   realKey,
					Value: encLogRecordPos,
				})
				if err = hintFile.Write(encLogRecord); err != nil {
					return err
				}
				// 维护offset
				off += sz
			}
		}
	}
	// 持久化DB与hint file
	if err := mergeDB.Sync(); err != nil {
		return err
	}
	if err := hintFile.Sync(); err != nil {
		return nil
	}
	// 最后创建finish文件并写入maxMergeFileId
	mergeFinishedFile, err := data.OpenMergeFinsihedFile(mergePath)
	if err != nil {
		return err
	}
	// 以minMergeFileId, maxMergeFileId为value，构造record
	finishedRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(maxMergeFileId))),
	}
	encFinishedRecord, _ := data.EncodeLogRecord(finishedRecord)
	if err := mergeFinishedFile.Write(encFinishedRecord); err != nil {
		return err
	}
	// 写入完成后，不要忘记持久化
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}
	return nil
}

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.opts.DirPath))
	base := path.Base(db.opts.DirPath)
	return filepath.Join(dir, base+mergeDirName)
}

// 用merge后的数据文件替换原目录的数据文件
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	// 不存在则不替换
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		os.RemoveAll(mergePath)
	}()
	// 读取目录下的所有文件
	entrys, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}
	// 保存merge目录下所有的文件名
	var finished = false
	fileNames := make([]string, 0)
	for _, entry := range entrys {
		fileName := entry.Name()
		if fileName == data.MergeFilishedFileName {
			finished = true
		}
		fileNames = append(fileNames, fileName)
	}
	if !finished {
		return nil
	}
	// 获取merge完成的最大file id
	maxMergeFileId, err := db.getMaxMergeFileId(mergePath)
	if err != nil {
		return err
	}
	// 删除原数据目录中，被merge过的file
	var fileId uint32 = 0
	for ; fileId <= maxMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.opts.DirPath, fileId)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.RemoveAll(fileName); err != nil {
				return nil
			}
		}
	}
	// 将merge目录下的数据文件移动到原目录下
	for _, fileName := range fileNames {
		srcName := filepath.Join(mergePath, fileName)
		desName := filepath.Join(db.opts.DirPath, fileName)
		if err := os.Rename(srcName, desName); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) getMaxMergeFileId(dirPath string) (uint32, error) {
	fileName := filepath.Join(dirPath, data.MergeFilishedFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return 0, err
	}
	// 打开finished文件
	finishedFile, err := data.OpenMergeFinsihedFile(dirPath)
	if err != nil {
		return 0, err
	}
	// 读取finished文件中的record
	logRecord, _, err := finishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	maxMergeFileId, err := strconv.Atoi(string(logRecord.Value))
	if err != nil {
		return 0, err
	}
	return uint32(maxMergeFileId), nil
}

// TODO:系统是如何查看一个文件的？os.Stat()
func (db *DB) loadIndexFromHintFile() error {
	// 判断hint文件是否存在
	hintFileName := filepath.Join(db.opts.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}
	// 开启并加载hint文件的数据到data file中
	hintFile, err := data.OpenHintFile(db.opts.DirPath)
	if err != nil {
		return err
	}
	var off int64 = 0
	for {
		logRecord, sz, err := hintFile.ReadLogRecord(off)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil
		}
		off += sz
		logRecordPos := data.DecodeLogRecordPos(logRecord.Value)
		if ok := db.index.Put(logRecord.Key, logRecordPos); !ok {
			return ErrUpdateIndexFailed
		}
	}
	return nil
}
