package kvdb

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"time"
)

type Entry struct {
	*EntryHeader
	*EntryData
}

type EntryHeader struct {
	crc       uint32
	timeStamp uint32
	keySize   uint32
	valueSize uint32
}

type EntryData struct {
	Key   []byte
	Value []byte
}

// CRC + TimeStamp + KeySize + ValueSize
const entryHeaderSize = 16

// HeaderSize + KeySize + ValueSize
func (eh *EntryHeader) GetSize() int64 {
	return int64(entryHeaderSize + eh.keySize + eh.valueSize)
}

func NewEntry(key []byte, value []byte) *Entry {
	entry := &Entry{
		EntryHeader: &EntryHeader{
			crc:       0,
			timeStamp: uint32(time.Now().Unix()),
			keySize:   uint32(len(key)),
			valueSize: uint32(len(value)),
		},
		EntryData: &EntryData{
			Key:   key,
			Value: value,
		},
	}
	return entry
}

/*
	EncodeEntry function will generate CRC-32 checksum
*/
func (e *Entry) EncodeEntry() []byte {
	buf := make([]byte, e.GetSize())

	// Header
	binary.BigEndian.PutUint32(buf[4:8], e.timeStamp)
	binary.BigEndian.PutUint32(buf[8:12], e.keySize)
	binary.BigEndian.PutUint32(buf[12:16], e.valueSize)

	// Data
	copy(buf[entryHeaderSize:entryHeaderSize+e.keySize], e.Key)
	copy(buf[entryHeaderSize+e.keySize:], e.Value)

	// CRC
	binary.BigEndian.PutUint32(buf[0:4], crc32.ChecksumIEEE(buf[4:]))
	return buf
}

/*
	Decode function will verify CRC-32 checksum
*/
func DecodeEntry(buf []byte) (entry *Entry, err error) {
	// Header
	crc := binary.BigEndian.Uint32(buf[0:4])
	timeStamp := binary.BigEndian.Uint32(buf[4:8])
	keySize := binary.BigEndian.Uint32(buf[8:12])
	valueSize := binary.BigEndian.Uint32(buf[12:16])

	// Data
	key := make([]byte, keySize)
	value := make([]byte, valueSize)
	copy(key, buf[entryHeaderSize:entryHeaderSize+keySize])
	copy(value, buf[entryHeaderSize+keySize:])

	entry = &Entry{
		EntryHeader: &EntryHeader{
			crc:       crc,
			timeStamp: timeStamp,
			keySize:   keySize,
			valueSize: valueSize,
		},
		EntryData: &EntryData{
			Key:   key,
			Value: value,
		},
	}

	if crc != crc32.ChecksumIEEE(buf[4:]) {
		err = errors.New("CRC-32 checksum is different")
	}

	return
}

func DecodeEntryHeader(buf []byte) (entryHeader *EntryHeader, err error) {
	if len(buf) != 16 {
		err = errors.New("Entry header length doesn't match")
		return
	}

	// Header
	crc := binary.BigEndian.Uint32(buf[0:4])
	timeStamp := binary.BigEndian.Uint32(buf[4:8])
	keySize := binary.BigEndian.Uint32(buf[8:12])
	valueSize := binary.BigEndian.Uint32(buf[12:16])

	entryHeader = &EntryHeader{
		crc:       crc,
		timeStamp: timeStamp,
		keySize:   keySize,
		valueSize: valueSize,
	}

	return
}
