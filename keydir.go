package kvdb

import (
	"fmt"
	"io"
	"sync"
)

type PositionMap struct {
	data  map[string]*Position
	mutex sync.Mutex
}

type Position struct {
	FileId    int64
	ValueSize uint32
	Offset    int64
	TimeStamp uint32
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

func (kd *PositionMap) PutPosition(key []byte, entryHeader *EntryHeader, fileId int64, offset int64) error {
	pos := &Position{
		FileId:    fileId,
		ValueSize: entryHeader.valueSize,
		Offset:    offset,
		TimeStamp: entryHeader.timeStamp,
	}

	kd.Set(key, pos)

	return nil
}

func (kd *PositionMap) GetPosition(key []byte) (pos *Position, err error) {
	pos = kd.Get(key)
	err = nil
	return
}

// Rebuild keydir of a data file
func (kd *PositionMap) Update(kvdbFile *KvdbFile) {
	headerBuf := make([]byte, entryHeaderSize)
	offset := int64(0)

	for {
		_, err := kvdbFile.File.ReadAt(headerBuf, offset)
		if err != nil {
			if err == io.EOF {
				return
			} else {
				panic(fmt.Sprintf("Update keydir failed, fileId:%d, err:%v", kvdbFile.FileId, err))
			}
		}

		entryHeader, _ := EntryHeaderDecode(headerBuf)
		keyBuf := make([]byte, entryHeader.keySize)
		_, _ = kvdbFile.File.ReadAt(keyBuf, offset+entryHeaderSize)

		oldPos, _ := kd.GetPosition(keyBuf)
		if oldPos == nil || oldPos.TimeStamp < entryHeader.timeStamp {
			kd.PutPosition(keyBuf, entryHeader, kvdbFile.FileId, offset)
		}

		offset += entryHeader.GetSize()
	}
}
