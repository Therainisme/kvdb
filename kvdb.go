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

	dfIdArray := ReadDataFileId(directory)
	fileMap, keydir := InitIndex(dfIdArray, directoryPath)

	return &KvdbHandle{
		DirectoryPath: directoryPath,
		Keydir:        keydir,
		FileMap:       fileMap,
	}
}

type KvdbHandle struct {
	DirectoryPath  string
	Keydir         *PositionMap
	FileMap        *KvdbFileMap
	ActiveDataFile *KvdbFile
}

type Keys = []string

func (db *KvdbHandle) Get(key []byte) ([]byte, error) {
	pos := db.Keydir.Get(key)
	if pos == nil {
		return []byte(""), nil
	}

	kf := db.FileMap.Get(pos.FileId)
	entry, err := kf.ReadEntry(key, pos)

	return entry.Value, err
}

func (db *KvdbHandle) Put(key []byte, value []byte) error {
	entry := NewEntry(key, value)

	// The DB must have an active file to write entries to.
	if db.ActiveDataFile == nil {
		fileId := time.Now().Unix()
		db.ActiveDataFile = CreateActiveDataFile(fileId, db.DirectoryPath)
		db.FileMap.Set(fileId, db.ActiveDataFile)
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
	db.FileMap.Set(mergedFile.FileId, mergedFile)

	redundantEntryMap := &EntryMap{}

	// Iterate over all keys
	db.FileMap.sm.Range(func(key, value interface{}) bool {
		kvdbFile := value.(*KvdbFile)
		if kvdbFile.Type == ActiveType || kvdbFile.Type == MergedType {
			return true
		}

		headerBuf := make([]byte, 16)
		offset := int64(0)

		for {
			_, err := kvdbFile.File.ReadAt(headerBuf, offset)
			if err != nil {
				if err == io.EOF {
					break
				} else {
					panic(fmt.Sprintf("Merged data file failed, fileId:%d, err:%v", kvdbFile.FileId, err))
				}
			}

			entryHeader, _ := EntryHeaderDecode(headerBuf)

			entryBuf := make([]byte, entryHeader.GetSize())
			_, _ = kvdbFile.File.ReadAt(entryBuf, offset)
			entry, _ := EntryDecode(entryBuf)

			oldEntry := redundantEntryMap.Get(entry.Key)
			if oldEntry == nil || oldEntry.timeStamp <= entry.timeStamp {
				redundantEntryMap.Set(entry.Key, entry)
			}

			offset += entry.GetSize()
		}

		return true
	})

	// Write to merged file
	redundantEntryMap.sm.Range(func(key, value interface{}) bool {
		entry := value.(*Entry)
		db.Keydir.PutPosition(entry.Key, entry.EntryHeader, mergedFile.FileId, mergedFile.offset)
		mergedFile.AppendEntry(entry)
		return true
	})

	// todo generate hint file

	// Remove old file
	db.FileMap.sm.Range(func(key, value interface{}) bool {
		kvdbFile := value.(*KvdbFile)
		if kvdbFile.Type == ActiveType || kvdbFile.Type == MergedType {
			return true
		}
		kvdbFile.File.Close()
		db.FileMap.Delete(kvdbFile.FileId)

		os.Remove(kvdbFile.File.Name())
		return true
	})

	return nil
}

func (handle *KvdbHandle) Sync() error {
	return nil
}

func (handle *KvdbHandle) Close() error {
	return nil
}

func InitIndex(dfIdArray []int64, directoryPath string) (*KvdbFileMap, *PositionMap) {
	var kvdbFileMap KvdbFileMap = KvdbFileMap{
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
