package kvdb

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

// CRC + TimeStamp + KeySize + ValueSize
const entryHeaderSize = 16

type Entry struct {
	CRC       uint32
	TimeStamp uint32
	KeySize   uint32
	ValueSize uint32
	Key       []byte
	Value     []byte
}

func (entry *Entry) GetSize() uint64 {
	return uint64(entryHeaderSize + entry.KeySize + entry.ValueSize)
}

func NewEntry(key []byte, value []byte) *Entry {
	entry := &Entry{
		CRC:       0,
		TimeStamp: uint32(time.Now().Unix()),
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
		Key:       key,
		Value:     value,
	}
	return entry
}

func (entry *Entry) Encode() []byte {
	buf := make([]byte, entry.GetSize())
	binary.BigEndian.PutUint32(buf[4:8], entry.TimeStamp)
	binary.BigEndian.PutUint32(buf[8:12], entry.KeySize)
	binary.BigEndian.PutUint32(buf[12:16], entry.ValueSize)
	copy(buf[entryHeaderSize:entryHeaderSize+entry.KeySize], entry.Key)
	copy(buf[entryHeaderSize+entry.KeySize:], entry.Value)
	binary.BigEndian.PutUint32(buf[0:4], crc32.ChecksumIEEE(buf[4:]))
	return buf
}
