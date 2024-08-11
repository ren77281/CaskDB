package fio

type IoType = byte

const (
	FileIoType = iota
)

type IoManager interface {
	Read(b []byte, off int64) (int, error)
	Write(b []byte) (int, error)
	Sync() error
	Close() error
	Size() (int64, error)
}

func NewIoManager(name string, ioType IoType) (IoManager, error) {
	switch ioType {
	case FileIoType:
		return NewFileIoManager(name)
	default:
		panic("unsupport")
	}
}