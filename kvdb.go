package kvdb

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

func Open(directoryPath string) *KvdbHandle {
	directory, err := os.Open(directoryPath)
	if err != nil {
		panic(err.Error())
	}

	directoryInfo, _ := directory.Stat()
	if !directoryInfo.IsDir() {
		panic(fmt.Sprintf("\"%s\" is not a directory\n", directoryPath))
	}

	dfIdArray := GetAllDataFileId(directory)
	fileMap, keydir := InitIndex(dfIdArray, directoryPath)

	return &KvdbHandle{
		DirectoryPath: directoryPath,
		Keydir:        keydir,
		DataFileMap:   fileMap,
	}
}

type KvdbHandle struct {
	DirectoryPath  string
	Keydir         *PositionMap
	DataFileMap    *DataFileMap
	ActiveDataFile *DataFile
}

type Keys = []string

func (db *KvdbHandle) Get(key []byte) ([]byte, error) {
	pos := db.Keydir.Get(key)
	if pos == nil {
		return []byte(""), nil
	}

	kf := db.DataFileMap.Get(pos.FileId)
	entry, err := kf.ReadEntry(key, pos)

	return entry.Value, err
}

func (db *KvdbHandle) Put(key []byte, value []byte) error {
	entry := NewEntry(key, value)

	// The DB must have an active file to write entries to.
	if db.ActiveDataFile == nil {
		fileId := time.Now().Unix()
		db.ActiveDataFile = CreateActiveDataFile(fileId, db.DirectoryPath)
		db.DataFileMap.Set(fileId, db.ActiveDataFile)
	}

	// Update keydir
	db.Keydir.PutPosition(
		key,
		entry.EntryHeader,
		db.ActiveDataFile.FileId,
		db.ActiveDataFile.offset,
	)

	// Write to active file
	err := db.ActiveDataFile.AppendEntry(entry)

	return err
}

func (db *KvdbHandle) Delete(key []byte) error {
	db.Put(key, []byte(""))
	return nil
}

func (handle *KvdbHandle) ListKeys() Keys {
	// todo
	return nil
}

func (db *KvdbHandle) Merge() error {
	mergedFile := CreateMergedDataFile(time.Now().Unix(), db.DirectoryPath)

	redundantEntryMap := &EntryMap{}

	// Iterate over all keys
	db.DataFileMap.sm.Range(func(key, value interface{}) bool {
		dataFile := value.(*DataFile)
		if dataFile.Type != OlderType {
			return true
		}

		offset := int64(0)

		for {
			// Read the header
			headerBuf, err := dataFile.ReadBuf(entryHeaderSize, offset)
			if err != nil && err == io.EOF {
				break
			}
			entryHeader, _ := DecodeEntryHeader(headerBuf)

			// Read the entry
			entryBuf, _ := dataFile.ReadBuf(entryHeader.GetSize(), offset)
			entry, _ := DecodeEntry(entryBuf)

			redundantEntryMap.Set(entry.Key, entry)

			offset += entry.GetSize()
		}

		return true
	})

	hintFile := CreateHintFile(mergedFile.FileId, db.DirectoryPath)

	// Write to merged file
	redundantEntryMap.sm.Range(func(key, value interface{}) bool {
		entry := value.(*Entry)
		// Update keydir
		db.Keydir.PutPosition(entry.Key, entry.EntryHeader, mergedFile.FileId, mergedFile.offset)

		// Write index to hint file
		hintFile.AppendHintItem(&HintItem{
			HintItemHeader: &HintItemHeader{
				TimeStamp: entry.timeStamp,
				KeySize:   entry.keySize,
				ValueSize: entry.valueSize,
				Offset:    mergedFile.offset,
			},
			Key: entry.Key,
		})

		// Write to merged file
		// Can update merged file offset
		mergedFile.AppendEntry(entry)
		return true
	})

	// Remove old data file
	db.DataFileMap.sm.Range(func(key, value interface{}) bool {
		kvdbFile := value.(*DataFile)
		if kvdbFile.Type != OlderType {
			return true
		}
		kvdbFile.File.Close()
		db.DataFileMap.Delete(kvdbFile.FileId)

		os.Remove(kvdbFile.File.Name())
		return true
	})

	// After merge process, merged file will be as a ordinary data file
	mergedFile.Type = OlderType
	db.DataFileMap.Set(mergedFile.FileId, mergedFile)
	return nil
}

func (handle *KvdbHandle) Sync() error {
	return nil
}

func (handle *KvdbHandle) Close() error {
	return nil
}

func InitIndex(dfIdArray []int64, directoryPath string) (*DataFileMap, *PositionMap) {
	var kvdbFileMap DataFileMap = DataFileMap{
		sm: sync.Map{},
	}
	var keydir PositionMap = PositionMap{
		data:  make(map[string]*Position),
		mutex: sync.Mutex{},
	}

	for _, dfId := range dfIdArray {
		kvdbFile := OpenOlderDataFile(dfId, directoryPath)
		kvdbFileMap.Set(dfId, kvdbFile)
		keydir.Update(kvdbFile)
	}

	return &kvdbFileMap, &keydir
}
