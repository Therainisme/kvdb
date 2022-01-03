package kvdb

import (
	"fmt"
	"os"
	"strconv"
	"sync"
)

const (
	DataFileSuffix = ".kvdb.data"
)

const (
	ActiveType = "active"
	OlderType  = "older"
	HintType   = "hint"
)

type KvdbFile struct {
	File   *os.File
	FileId int64  // It is usually the timestamp of the creation.
	offset int64  // The starting offset when writing
	Type   string // older active hint
	mutex  sync.Mutex
}

func CreateActiveDataFile(fileId int64, directoryPath string) *KvdbFile {
	fileName := strconv.FormatInt(fileId, 10)
	file, err := os.OpenFile(directoryPath+fileName+DataFileSuffix, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
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

func OpenOlderDataFile(fileId int64, directoryPath string) *KvdbFile {
	fileName := strconv.FormatInt(fileId, 10)
	// read only
	file, err := os.OpenFile(directoryPath+fileName+DataFileSuffix, os.O_RDONLY, 0666)
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
	// Keydir.PutPosition(entry, kf.FileId, kf.offset)
	// todo PutPostition
	kf.offset += int64(len(buf))

	kf.mutex.Unlock()
	return nil
}

// deprecated
func (kf *KvdbFile) ReadEntry(pos *Position) (entry *Entry, err error) {
	buf := make([]byte, pos.ValueSize)
	_, err = kf.File.ReadAt(buf, pos.Offset)
	if err != nil {
		fmt.Printf("Read entry failed, err:%v", err)
		return
	}

	entry, err = EntryDecode(buf)
	return
}

// Stored in a Kbdb instance
type KvdbFileMap struct {
	data  map[int64]*KvdbFile
	mutex sync.Mutex
}

func (kfMap *KvdbFileMap) Get(fileId int64) (kvdbFile *KvdbFile) {
	kfMap.mutex.Lock()
	kvdbFile = kfMap.data[fileId]
	kfMap.mutex.Unlock()

	return
}

func (kfMap *KvdbFileMap) Set(fileId int64, kvdbFile *KvdbFile) {
	kfMap.mutex.Lock()
	kfMap.data[fileId] = kvdbFile
	kfMap.mutex.Unlock()
}
