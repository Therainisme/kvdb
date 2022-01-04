package kvdb

import (
	"io"
	"sync"
)

type Keydir struct {
	sm *sync.Map
}

type KeydirItem struct {
	FileId    int64
	ValueSize uint32
	Offset    int64
	TimeStamp uint32
}

func (kd *Keydir) Set(key []byte, item *KeydirItem) {
	// Keydir ensure that the latest index is stored.
	oldItem := kd.Get(key)
	if oldItem == nil || oldItem.TimeStamp <= item.TimeStamp {
		kd.sm.Store(string(key), item)
	}
}

func (kd *Keydir) Get(key []byte) (item *KeydirItem) {

	loadRes, ok := kd.sm.Load(string(key))
	if !ok {
		item = nil
	} else {
		item = loadRes.(*KeydirItem)
	}

	return
}

func (kd *Keydir) Delete(key []byte) {
	kd.sm.Delete(string(key))
}

func (kd *Keydir) PutItem(key []byte, entryHeader *EntryHeader, fileId int64, offset int64) error {
	pos := &KeydirItem{
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

func (kd *Keydir) GetItem(key []byte) (item *KeydirItem, err error) {
	item = kd.Get(key)
	err = nil
	return
}

// Rebuild keydir of a data file
func (kd *Keydir) Update(dataFile *DataFile) {
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

			kd.PutItem(key, entryHeader, dataFile.FileId, offset)

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

			kd.PutItem(
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
