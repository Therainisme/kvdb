package kvdb

import "os"

type DataFile struct {
	*KvdbFile
}

func (kf *DataFile) AppendEntry(entry *Entry) error {
	buf := entry.EncodeEntry()
	kf.mutex.Lock()

	kf.File.WriteAt(buf, kf.offset)
	kf.offset += int64(len(buf))

	kf.mutex.Unlock()
	return nil
}

func (kf *DataFile) ReadEntry(key []byte, pos *Position) (entry *Entry, err error) {
	targetEntrySize := entryHeaderSize + len(key) + int(pos.ValueSize)
	buf, _ := kf.ReadBuf(int64(targetEntrySize), pos.Offset)

	entry, err = DecodeEntry(buf)
	return
}

func (kf *DataFile) IsExistHintFile() bool {
	filePath := kf.File.Name()
	hintFilePath := filePath[0:len(filePath)-4] + "hint"
	_, err := os.Stat(hintFilePath)
	if err != nil && os.IsNotExist(err) {
		return false
	}

	return true
}

func CreateActiveDataFile(fileId int64, dir string) *DataFile {
	file := openFile(fileId, dir, DataFileSuffix, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	return &DataFile{
		&KvdbFile{
			File:   file,
			FileId: fileId,
			Type:   ActiveType,
			offset: 0,
		},
	}
}

func CreateMergedDataFile(fileId int64, dir string) *DataFile {
	file := openFile(fileId, dir, DataFileSuffix, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	return &DataFile{
		&KvdbFile{
			File:   file,
			FileId: fileId,
			Type:   MergedType,
			offset: 0,
		},
	}
}

func OpenOlderDataFile(fileId int64, dir string) *DataFile {
	file := openFile(fileId, dir, DataFileSuffix, os.O_RDONLY, 0666)
	return &DataFile{
		&KvdbFile{
			File:   file,
			FileId: fileId,
			Type:   OlderType,
			offset: 0,
		},
	}
}
