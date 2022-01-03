package kvdb

import (
	"fmt"
	"os"
	"sync"
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
	DirectoryPath string
	Keydir        *PositionMap
	FileMap       *KvdbFileMap
}

type Keys = []string

func (db *KvdbHandle) Get(key []byte) ([]byte, error) {
	pos := db.Keydir.Get(key)
	if pos == nil {
		return nil, nil
	}
	kf := db.FileMap.Get(pos.FileId)
	entry, err := kf.ReadEntry(key, pos)

	return entry.Value, err
}

func (handle *KvdbHandle) Put(key []byte, value []byte) error {
	// todo
	return nil
}

func (handle *KvdbHandle) Delete(key []byte) error {
	// todo
	return nil
}

func (handle *KvdbHandle) ListKeys() Keys {
	// todo
	return nil
}

func (handle *KvdbHandle) Merge(directoryName string) error {
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
		data:  make(map[int64]*KvdbFile),
		mutex: sync.Mutex{},
	}
	var keydir PositionMap = PositionMap{
		data:  make(map[string]*Position),
		mutex: sync.Mutex{},
	}

	for _, dfId := range dfIdArray {
		kvdbFile := OpenOlderDataFile(dfId, directoryPath+"/")
		kvdbFileMap.Set(dfId, kvdbFile)
		keydir.Update(kvdbFile)
	}

	return &kvdbFileMap, &keydir
}
