package kvdb

import (
	"encoding/binary"
	"errors"
)

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
		err = errors.New("hint item header length doesn't match")
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
