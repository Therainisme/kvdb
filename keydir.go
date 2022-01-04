package kvdb

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
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

	filePath := kvdbFile.File.Name()
	hintFilePath := filePath[0:len(filePath)-4] + "hint"
	_, err := os.Stat(hintFilePath)

	if err != nil && os.IsNotExist(err) {
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
	} else {
		// todo change function
		hintFile := OpenHintFile(kvdbFile.FileId, hintFilePath[0:len(hintFilePath)-21])
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
			keyBuf, _ := hintFile.ReadBuf(int64(hintItemHeader.KeySize), offset+HintItemHeaderSize)

			kd.PutPosition(
				keyBuf,
				&EntryHeader{
					crc:       0,
					timeStamp: hintItemHeader.TimeStamp,
					keySize:   hintItemHeader.KeySize,
					valueSize: hintItemHeader.ValueSize,
				},
				kvdbFile.FileId,
				hintItemHeader.Offset,
			)

			println(hintItemHeader.KeySize)

			// Skip to the beginning of the next entry
			offset += hintItemHeader.GetSize()
		}
	}
}

type HintItemHeader struct {
	TimeStamp uint32
	KeySize   uint32
	ValueSize uint32
	Offset    int64
}

type HintItem struct {
	*HintItemHeader
	Key []byte
}

const HintItemHeaderSize = 20

func (h *HintItemHeader) GetSize() int64 {
	return int64(HintItemHeaderSize + h.KeySize)
}

func (h *HintItem) EncodeHintItem() []byte {
	buf := make([]byte, h.GetSize())

	// Header
	binary.BigEndian.PutUint32(buf[0:4], h.TimeStamp)
	binary.BigEndian.PutUint32(buf[4:8], h.KeySize)
	binary.BigEndian.PutUint32(buf[8:12], h.ValueSize)
	binary.BigEndian.PutUint64(buf[12:20], uint64(h.Offset))

	// Key
	copy(buf[20:], h.Key)

	return buf
}

func DecodeHintItemHeader(buf []byte) (hth *HintItemHeader, err error) {
	if len(buf) != HintItemHeaderSize {
		err = errors.New("Hint item header length doesn't match")
		return
	}

	// Header
	timeStamp := binary.BigEndian.Uint32(buf[0:4])
	keySize := binary.BigEndian.Uint32(buf[4:8])
	valueSize := binary.BigEndian.Uint32(buf[8:12])
	offset := binary.BigEndian.Uint64(buf[12:20])

	hth = &HintItemHeader{
		TimeStamp: timeStamp,
		KeySize:   keySize,
		ValueSize: valueSize,
		Offset:    int64(offset),
	}

	return
}

func DecodeHintItem(buf []byte) *HintItem {
	// Header
	timeStamp := binary.BigEndian.Uint32(buf[0:4])
	keySize := binary.BigEndian.Uint32(buf[4:8])
	valueSize := binary.BigEndian.Uint32(buf[8:12])
	offset := binary.BigEndian.Uint64(buf[12:20])

	// Key
	key := make([]byte, keySize)

	return &HintItem{
		HintItemHeader: &HintItemHeader{
			TimeStamp: timeStamp,
			KeySize:   keySize,
			ValueSize: valueSize,
			Offset:    int64(offset),
		},
		Key: key,
	}
}
