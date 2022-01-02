package kvdb

import "sync"

type PositionMap map[string]*Position

type Position struct {
	FileId    int64
	TimeStamp uint32
	EntrySize int64
	Offset    int64
}

var Keydir = make(PositionMap)
var mutex sync.Mutex

func (kd *PositionMap) Set(key []byte, pos *Position) {
	mutex.Lock()

	Keydir[string(key)] = pos

	mutex.Unlock()
}

func (kd *PositionMap) Get(key []byte) (pos *Position) {
	mutex.Lock()

	pos = Keydir[string(key)]

	mutex.Unlock()

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
