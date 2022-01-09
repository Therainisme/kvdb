package kvdb

import "os"

type DataFile struct {
	*KvdbFile
}

func (df *DataFile) AppendEntry(entry *Entry) error {
	buf := entry.EncodeEntry()
	df.mutex.Lock()

	df.File.WriteAt(buf, df.offset)
	df.offset += int64(len(buf))

	df.mutex.Unlock()
	return nil
}

func (df *DataFile) ReadEntry(key []byte, keydirItem *KeydirItem) (entry *Entry, err error) {
	targetEntrySize := entryHeaderSize + len(key) + int(keydirItem.ValueSize)
	buf, _ := df.ReadBuf(int64(targetEntrySize), keydirItem.Offset)

	entry, err = DecodeEntry(buf)
	return
}

func DFPathToHFPath(dataFilePath string) (hintFilePath string) {
	return dataFilePath[0:len(dataFilePath)-4] + "hint"
}

func (df *DataFile) IsExistHintFile() bool {
	filePath := df.File.Name()
	hintFilePath := DFPathToHFPath(filePath)
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
