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
	MergedType = "merged"
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
	file, err := os.OpenFile(directoryPath+"/"+fileName+DataFileSuffix, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
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

func CreateMergedDataFile(fileId int64, directoryPath string) *KvdbFile {
	fileName := strconv.FormatInt(fileId, 10)
	file, err := os.OpenFile(directoryPath+"/"+fileName+DataFileSuffix, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		panic(err.Error())
	}
	return &KvdbFile{
		File:   file,
		FileId: fileId,
		Type:   MergedType,
		offset: 0,
	}
}

func OpenOlderDataFile(fileId int64, directoryPath string) *KvdbFile {
	fileName := strconv.FormatInt(fileId, 10)
	// read only
	file, err := os.OpenFile(directoryPath+"/"+fileName+DataFileSuffix, os.O_RDONLY, 0666)
	if err != nil {
		panic(err.Error())
	}
	return &KvdbFile{
		File:   file,
		FileId: fileId,
		Type:   OlderType,
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

func (kf *KvdbFile) ReadEntry(key []byte, pos *Position) (entry *Entry, err error) {
	buf := make([]byte, entryHeaderSize+len(key)+int(pos.ValueSize))
	_, err = kf.File.ReadAt(buf, pos.Offset)
	if err != nil {
		err = fmt.Errorf("read entry failed, err:%v ", err)
		return
	}

	entry, err = EntryDecode(buf)
	return
}
