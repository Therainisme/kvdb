package kvdb

import "sync"

type PositionMap struct {
	data  map[string]*Position
	mutex sync.Mutex
}

type Position struct {
	FileId    int64
	TimeStamp uint32
	EntrySize int64
	Offset    int64
}

func (kd *PositionMap) Set(key []byte, pos *Position) {
	kd.mutex.Lock()

	kd.data[string(key)] = pos

	kd.mutex.Unlock()
}

func (kd *PositionMap) Get(key []byte) (pos *Position) {
	kd.mutex.Lock()

	pos = kd.data[string(key)]

	kd.mutex.Unlock()

	return
}

func (kd *PositionMap) PutPosition(entry *Entry, fileId int64, offset int64) error {
	pos := &Position{
		FileId:    fileId,
		EntrySize: entry.GetSize(),
		Offset:    offset,
		TimeStamp: entry.timeStamp,
	}

	kd.Set(entry.Key, pos)

	return nil
}

func (kd *PositionMap) GetPosition(key []byte) (pos *Position, err error) {
	pos = kd.Get(key)
	err = nil
	return
}
