package kvdb

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

func GetAllDataFileId(directory *os.File) []int64 {
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
type DataFileMap struct {
	sm sync.Map
}

func (kfMap *DataFileMap) Get(fileId int64) (dataFile *DataFile) {

	loadRes, ok := kfMap.sm.Load(fileId)
	if !ok {
		dataFile = nil
	} else {
		dataFile = loadRes.(*DataFile)
	}

	return
}

func (kfMap *DataFileMap) Set(fileId int64, dataFile *DataFile) {
	kfMap.sm.Store(fileId, dataFile)
}

func (kfMap *DataFileMap) Delete(fileId int64) {
	kfMap.sm.Delete(fileId)
}

// Using in merge process
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
	oldEntry := em.Get(entry.Key)
	if oldEntry == nil || oldEntry.timeStamp <= entry.timeStamp {
		em.sm.Store(string(key), entry)
	}
}
