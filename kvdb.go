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
		err, _ := fmt.Printf("\"%s\" is not a directory\n", directoryPath)
		panic(err)
	}

	dfIdArray := ReadDataFileId(directory)
	keydir, fileMap := InitIndex(dfIdArray, directoryPath)

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

func (handle *KvdbHandle) Get(key string) (interface{}, error) {
	// todo
	return 0, nil
}

func (handle *KvdbHandle) Put(key string, value interface{}) error {
	// todo
	return nil
}

func (handle *KvdbHandle) Delete(key string) error {
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

func InitIndex(dfIdArray []int64, directoryPath string) (*PositionMap, *KvdbFileMap) {
	var keydir PositionMap = PositionMap{
		data:  make(map[string]*Position),
		mutex: sync.Mutex{},
	}
	var kvdbFileMap KvdbFileMap = KvdbFileMap{
		data:  make(map[int64]*KvdbFile),
		mutex: sync.Mutex{},
	}

	for _, dfId := range dfIdArray {
		kvdbFile := OpenOlderDataFile(dfId, directoryPath+"/")
		kvdbFileMap.Set(dfId, kvdbFile)
		keydir.Update(kvdbFile)
	}

	return &keydir, &kvdbFileMap
}
