package kvdb

import (
	"fmt"
	"os"
	"strconv"
	"sync"
)

const (
	DataFileSuffix       = ".kvdb.data"
	ActiveDataFileSuffix = ".kvdb.data.active"
)

const (
	ActiveType = "active"
	OlderType  = "older"
	HintType   = "hint"
)

type KvdbFile struct {
	File   *os.File
	FileId int64
	offset int64
	Type   string // older active hint
	mutex  sync.Mutex
}

func CreateActiveDataFile(fileId int64) *KvdbFile {
	fileName := strconv.FormatInt(fileId, 10)
	file, err := os.OpenFile(fileName+ActiveDataFileSuffix, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		panic(err.Error())
	}
	return &KvdbFile{
		File:   file,
		FileId: fileId,
		Type:   ActiveType,
		offset: 0,
	}
}

func (kf *KvdbFile) AppendEntry(entry *Entry) error {
	buf := entry.Encode()
	kf.mutex.Lock()

	kf.File.WriteAt(buf, kf.offset)
	Keydir.PutPosition(entry, kf.FileId, kf.offset)
	kf.offset += int64(len(buf))

	kf.mutex.Unlock()
	return nil
}

func (kf *KvdbFile) ReadEntry(pos *Position) (entry *Entry, err error) {
	buf := make([]byte, pos.EntrySize)
	_, err = kf.File.ReadAt(buf, pos.Offset)
	if err != nil {
		fmt.Printf("Read entry failed, err:%v", err)
		return
	}

	entry, err = EntryDecode(buf)
	return
}
