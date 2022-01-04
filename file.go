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
	HintFileSuffix = ".kvdb.hint"
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

func openFile(fileId int64, dir string, suffix string, flag int, perm fs.FileMode) *os.File {
	fileName := strconv.FormatInt(fileId, 10)
	file, err := os.OpenFile(dir+"/"+fileName+suffix, flag, perm)
	if err != nil {
		panic(err.Error())
	}
	return file
}

func (kf *KvdbFile) ReadBuf(bufSize int64, offset int64) (buf []byte, err error) {
	buf = make([]byte, bufSize)
	_, err = kf.File.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		panic(err.Error())
	}
	return buf, err
}
