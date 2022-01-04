package kvdb

import (
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

	// Keydir ensure that the latest index is stored.
	oldPos := kd.data[string(key)]
	if oldPos == nil || oldPos.TimeStamp <= pos.TimeStamp {
		kd.data[string(key)] = pos
	}

	kd.mutex.Unlock()
}

func (kd *PositionMap) Get(key []byte) (pos *Position) {
	kd.mutex.Lock()

	pos = kd.data[string(key)]

	kd.mutex.Unlock()

	return
}

func (kd *PositionMap) Delete(key []byte) {
	kd.mutex.Lock()

	delete(kd.data, string(key))

	kd.mutex.Unlock()
}

func (kd *PositionMap) PutPosition(key []byte, entryHeader *EntryHeader, fileId int64, offset int64) error {
	pos := &Position{
		FileId:    fileId,
		ValueSize: entryHeader.valueSize,
		Offset:    offset,
		TimeStamp: entryHeader.timeStamp,
	}

	if pos.ValueSize == 0 {
		kd.Delete(key)
	} else {
		kd.Set(key, pos)
	}

	return nil
}

func (kd *PositionMap) GetPosition(key []byte) (pos *Position, err error) {
	pos = kd.Get(key)
	err = nil
	return
}

// Rebuild keydir of a data file
func (kd *PositionMap) Update(kvdbFile *KvdbFile) {
	offset := int64(0)

	// Read the header of each entry
	for {
		// Read the header
		headerBuf, err := kvdbFile.ReadBuf(entryHeaderSize, offset)
		if err != nil && err == io.EOF {
			return
		}

		entryHeader, _ := DecodeEntryHeader(headerBuf)

		// Read the key
		keyBuf, _ := kvdbFile.ReadBuf(int64(entryHeader.keySize), offset+entryHeaderSize)

		kd.PutPosition(keyBuf, entryHeader, kvdbFile.FileId, offset)

		// Skip to the beginning of the next entry
		offset += entryHeader.GetSize()
	}
}
