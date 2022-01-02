package kvdb

import (
	"os"
	"sync"
)

const (
	DataFileSuffix       = "kvdb.data"
	ActiveDataFileSuffix = "kvdb.data.active"
)

const (
	ActiveType = "active"
	OlderType  = "older"
	HintType   = "hint"
)

type KvdbFile struct {
	File   *os.File
	Type   string // older active hint
	offset int64
	mutex  sync.Mutex
}

func CreateActiveDataFile(fileName string) *KvdbFile {
	file, err := os.OpenFile(fileName+ActiveDataFileSuffix, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		panic(err.Error())
	}
	return &KvdbFile{
		File:   file,
		Type:   ActiveType,
		offset: 0,
	}
}

func (kf *KvdbFile) AppendEntry(entry *Entry) error {
	buf := entry.Encode()
	kf.mutex.Lock()

	kf.File.WriteAt(buf, kf.offset)
	kf.offset += int64(len(buf))

	kf.mutex.Unlock()
	return nil
}
