package fio

type IOType = byte

const (
	FileIOType = iota
	MMapIOType
)

type IOManager interface {
	Read(b []byte, off int64) (int, error)
	Write(b []byte) (int, error)
	Sync() error
	Close() error
	Size() (int64, error)
}

func NewIOManager(fileName string, ioType IOType) (IOManager, error) {
	switch ioType {
	case FileIOType:
		return NewFileIOManager(fileName)
	case MMapIOType:
		return NewMMapIOManager(fileName)
	default:
		panic("IO unsupport")
	}
}