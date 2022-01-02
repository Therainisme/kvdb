package kvdb

import (
	"os"
	"regexp"
	"strconv"
	"strings"
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
