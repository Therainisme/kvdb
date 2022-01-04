package kvdb

import (
	"io"
	"io/fs"
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

func CreateActiveDataFile(fileId int64, dir string) *KvdbFile {
	file := openFile(fileId, dir, DataFileSuffix, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	return &KvdbFile{
		File:   file,
		FileId: fileId,
		Type:   ActiveType,
		offset: 0,
	}
}

func CreateMergedDataFile(fileId int64, dir string) *KvdbFile {
	file := openFile(fileId, dir, DataFileSuffix, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	return &KvdbFile{
		File:   file,
		FileId: fileId,
		Type:   MergedType,
		offset: 0,
	}
}

func OpenOlderDataFile(fileId int64, dir string) *KvdbFile {
	file := openFile(fileId, dir, DataFileSuffix, os.O_RDONLY, 0666)
	return &KvdbFile{
		File:   file,
		FileId: fileId,
		Type:   OlderType,
		offset: 0,
	}
}

func openFile(fileId int64, dir string, suffix string, flag int, perm fs.FileMode) *os.File {
	fileName := strconv.FormatInt(fileId, 10)
	file, err := os.OpenFile(dir+"/"+fileName+suffix, flag, perm)
	if err != nil {
		panic(err.Error())
	}
	return file
}

func (kf *KvdbFile) AppendEntry(entry *Entry) error {
	buf := entry.EncodeEntry()
	kf.mutex.Lock()

	kf.File.WriteAt(buf, kf.offset)
	kf.offset += int64(len(buf))

	kf.mutex.Unlock()
	return nil
}

func (kf *KvdbFile) ReadEntry(key []byte, pos *Position) (entry *Entry, err error) {
	targetEntrySize := entryHeaderSize + len(key) + int(pos.ValueSize)
	buf, _ := kf.ReadBuf(int64(targetEntrySize), pos.Offset)

	entry, err = DecodeEntry(buf)
	return
}

func (kf *KvdbFile) ReadBuf(bufSize int64, offset int64) (buf []byte, err error) {
	buf = make([]byte, bufSize)
	_, err = kf.File.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		panic(err.Error())
	}
	return buf, err
}
