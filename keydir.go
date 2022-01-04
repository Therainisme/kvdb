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
func (kd *PositionMap) Update(dataFile *DataFile) {
	offset := int64(0)

	if !dataFile.IsExistHintFile() {
		// Read the header of each entry
		for {
			// Read the header
			headerBuf, err := dataFile.ReadBuf(entryHeaderSize, offset)
			if err != nil && err == io.EOF {
				return
			}

			entryHeader, _ := DecodeEntryHeader(headerBuf)

			// Read the key
			key, _ := dataFile.ReadBuf(int64(entryHeader.keySize), offset+entryHeaderSize)

			kd.PutPosition(key, entryHeader, dataFile.FileId, offset)

			// Skip to the beginning of the next entry
			offset += entryHeader.GetSize()
		}
	} else {
		filePath := dataFile.File.Name()
		dbDir := filePath[0 : len(filePath)-21]
		hintFile := OpenHintFile(dataFile.FileId, dbDir)
		defer hintFile.File.Close()

		// Read the header of each hint item
		for {
			// Read the header
			headerBuf, err := hintFile.ReadBuf(HintItemHeaderSize, offset)
			if err != nil && err == io.EOF {
				return
			}

			hintItemHeader, _ := DecodeHintItemHeader(headerBuf)

			// Read the key
			key, _ := hintFile.ReadBuf(int64(hintItemHeader.KeySize), offset+HintItemHeaderSize)

			kd.PutPosition(
				key,
				&EntryHeader{
					crc:       0,
					timeStamp: hintItemHeader.TimeStamp,
					keySize:   hintItemHeader.KeySize,
					valueSize: hintItemHeader.ValueSize,
				},
				dataFile.FileId,
				hintItemHeader.Offset,
			)

			// Skip to the beginning of the next hint item
			offset += hintItemHeader.GetSize()
		}
	}
}
