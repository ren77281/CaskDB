package data

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"kv-go/fio"
	"path/filepath"
)

const DataFileNameSuffix string = ".data"

var (
	ErrInvalidCrc    = errors.New("invalid crc, log record is not right")
	ErrEmptyKey      = errors.New("empty key in the data file")
	ErrInvalidOffset = errors.New("invalid offset, large than file size")
)

type DataFile struct {
	FileId    uint32        // 与fd类似，用来唯一标识数据库中的数据文件
	WriteOff  int64         // 当前文件数据的偏移量
	IoManager fio.IoManager // 提供IO方法的接口
}

// 打开文件，并保存其IO方法到DataFile中
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
	iomanager, err := fio.NewIoManager(fileName, fio.FileIoType)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: iomanager,
	}, nil
}

// 往文件末尾追加 datas
func (dataFile *DataFile) Append(datas []byte) error {
	// 调用文件的Write方法
	n, err := dataFile.IoManager.Write(datas)
	if err != nil {
		return err
	}
	dataFile.WriteOff += int64(n)
	return nil
}

func (dataFile *DataFile) Sync() error {
	return dataFile.IoManager.Sync()
}

func (dataFile *DataFile) Close() error {
	return dataFile.IoManager.Close()
}

// 读取文件的 off 处的 LogRecord, 同时返回其长度
func (dataFile *DataFile) ReadLogRecord(off int64) (*LogRecord, int64, error) {
	// 先计算 header 是否超过了文件大小，根据计算结果调整 header 的长度
	// 获取文件大小
	fileSize, err := dataFile.IoManager.Size()
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
	logRecord := &LogRecord{LogRecordType: logRecordHeader.logRecordType}
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
	_, err := dataFile.IoManager.Read(b, off)
	if err != nil {
		return nil, err
	}
	return b, nil
}
