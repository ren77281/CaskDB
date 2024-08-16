package data

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"kv-go/fio"
	"path/filepath"
)

const (
	DataFileNameSuffix       string = ".data"
	HintFileName             string = "hint-index"
	MergeFilishedFileName    string = "merge-finish"
	NextWriteBatchIdFileName string = "wbid"
)

var (
	ErrInvalidCrc    = errors.New("invalid crc, log record is not right")
	ErrEmptyKey      = errors.New("empty key in the data file")
	ErrInvalidOffset = errors.New("invalid offset, large than file size")
)

type DataFile struct {
	FileId    uint32        // 与fd类似，用来唯一标识数据库中的数据文件
	WriteOff  int64         // 当前文件数据的偏移量
	IOManager fio.IOManager // 提供IO方法的接口
}

func newDataFile(fileName string, fileId uint32, ioType fio.IOType) (*DataFile, error) {
	iomanager, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IOManager: iomanager,
	}, nil
}

// TODO:这些函数调用关系似乎有点乱, dirPath与filename的拼接...

func OpenWriteBatchFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, NextWriteBatchIdFileName)
	return newDataFile(fileName, 0, fio.FileIOType)
}

// 打开文件，并保存其IO方法到DataFile中
func OpenDataFile(dirPath string, fileId uint32, ioType fio.IOType) (*DataFile, error) {
	fileName := GetDataFileNameById(dirPath, fileId)
	return newDataFile(fileName, fileId, ioType)
}

// 通过目录名与文件id构造data file文件名
func GetDataFileNameById(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}

// 创建并打开merge finish file
func OpenMergeFinsihedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFilishedFileName)
	return newDataFile(fileName, 0, fio.FileIOType)
}

// 创建并打开hint file
// TODO!!! 如果已经发生了merge，那么hintfile的fid为0是否会影响下一次的merge？？？
// !!! 同理finishfile的fid为0呢？？？
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0, fio.FileIOType)
}

// 往文件末尾追加 datas
func (dataFile *DataFile) Write(datas []byte) error {
	// 调用文件的Write方法
	n, err := dataFile.IOManager.Write(datas)
	if err != nil {
		return err
	}
	dataFile.WriteOff += int64(n)
	return nil
}

func (dataFile *DataFile) Sync() error {
	return dataFile.IOManager.Sync()
}

func (dataFile *DataFile) Close() error {
	return dataFile.IOManager.Close()
}

// 读取文件的 off 处的 LogRecord, 同时返回其长度
func (dataFile *DataFile) ReadLogRecord(off int64) (*LogRecord, int64, error) {
	// 先计算 header 是否超过了文件大小，根据计算结果调整 header 的长度
	// 获取文件大小
	fileSize, err := dataFile.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}
	var headerSize int64 = maxLogRecordHeadSize
	if off > fileSize {
		return nil, 0, ErrInvalidOffset
	}
	if off+int64(headerSize) > fileSize {
		headerSize = fileSize - off
	}
	// 调用readNBytes读取data file的n byte，得到header
	headerBytes, err := dataFile.readNBytes(headerSize, off)
	if err != nil {
		return nil, 0, err
	}
	// 调用decode获取LogRecordHeader，得到key size与value size
	logRecordHeader, headerSize := decodeLogRecordHeader(headerBytes)
	if logRecordHeader == nil {
		return nil, 0, io.EOF
	}
	// TODO:是否有必要加上这个条件？
	if logRecordHeader.crc == 0 && logRecordHeader.keySize == 0 && logRecordHeader.valueSize == 0 {
		return nil, 0, io.EOF
	}
	keySize, valueSize := int64(logRecordHeader.keySize), int64(logRecordHeader.valueSize)
	if keySize == 0 {
		return nil, 0, ErrEmptyKey
	}
	recordSize := headerSize + keySize + valueSize
	// 构造LogRecord
	logRecord := &LogRecord{Typ: logRecordHeader.logRecordType}
	// 继续调用readNBytes读取key与value
	kvBuf, err := dataFile.readNBytes(keySize+valueSize, off+headerSize)
	if err != nil {
		return nil, 0, err
	}
	logRecord.Key = kvBuf[:keySize]
	logRecord.Value = kvBuf[keySize:]
	// 最后验证数据有效性
	crc := getLogRecordCRC(logRecord, headerBytes[crc32.Size:headerSize])
	if crc != logRecordHeader.crc {
		return nil, 0, ErrInvalidCrc
	}
	return logRecord, recordSize, nil
}

// 从文件的 off 开始读取 n byte
func (dataFile *DataFile) readNBytes(n int64, off int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := dataFile.IOManager.Read(b, off)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// 重置data file的IO类型
func (dataFile *DataFile) ResetIOManager(fileName string, ioType fio.IOType) error {
	// 需要先关闭原来的IO
	if err := dataFile.IOManager.Close(); err != nil {
		return err
	}
	ioManager, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return err
	}
	dataFile.IOManager = ioManager
	return nil
}