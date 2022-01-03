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
	data  map[int64]*KvdbFile
	mutex sync.Mutex
}

func (kfMap *KvdbFileMap) Get(fileId int64) (kvdbFile *KvdbFile) {
	kfMap.mutex.Lock()
	kvdbFile = kfMap.data[fileId]
	kfMap.mutex.Unlock()

	return
}

func (kfMap *KvdbFileMap) Set(fileId int64, kvdbFile *KvdbFile) {
	kfMap.mutex.Lock()
	kfMap.data[fileId] = kvdbFile
	kfMap.mutex.Unlock()
}
