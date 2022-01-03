package kvdb

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

func ReadDataFileId(directory *os.File) []int64 {
	fileIdArray := make([]int64, 0)

	memberNames, _ := directory.Readdirnames(-1)
	for _, v := range memberNames {
		dataFileRegexp := regexp.MustCompile(`\d+\` + DataFileSuffix)
		dataFileName := dataFileRegexp.FindStringSubmatch(v)
		for _, dfName := range dataFileName {
			fileIdStr := strings.ReplaceAll(dfName, DataFileSuffix, "")
			fileId, _ := strconv.ParseInt(fileIdStr, 10, 64)
			fileIdArray = append(fileIdArray, fileId)
		}
	}

	return fileIdArray
}

// Stored in a Kbdb instance
type KvdbFileMap struct {
	sm sync.Map
}

func (kfMap *KvdbFileMap) Get(fileId int64) (kvdbFile *KvdbFile) {

	loadRes, ok := kfMap.sm.Load(fileId)
	if !ok {
		kvdbFile = nil
	} else {
		kvdbFile = loadRes.(*KvdbFile)
	}

	return
}

func (kfMap *KvdbFileMap) Set(fileId int64, kvdbFile *KvdbFile) {
	kfMap.sm.Store(fileId, kvdbFile)
}

type EntryMap struct {
	sm sync.Map
}

func (em *EntryMap) Get(key []byte) (entry *Entry) {

	loadRes, ok := em.sm.Load(string(key))
	if !ok {
		entry = nil
	} else {
		entry = loadRes.(*Entry)
	}

	return
}

func (em *EntryMap) Set(key []byte, entry *Entry) {
	em.sm.Store(string(key), entry)
}
