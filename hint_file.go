package kvdb

import "os"

type HintFile struct {
	*KvdbFile
}

func (hf *HintFile) AppendHintItem(hintItem *HintItem) error {
	buf := hintItem.EncodeHintItem()
	hf.mutex.Lock()

	hf.File.WriteAt(buf, hf.offset)
	hf.offset += int64(len(buf))

	hf.mutex.Unlock()
	return nil
}

func CreateHintFile(fileId int64, dir string) *HintFile {
	file := openFile(fileId, dir, HintFileSuffix, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	return &HintFile{
		&KvdbFile{
			File:   file,
			FileId: fileId,
			Type:   HintType,
			offset: 0,
		},
	}
}

func OpenHintFile(fileId int64, dir string) *HintFile {
	file := openFile(fileId, dir, HintFileSuffix, os.O_RDONLY, 0666)
	return &HintFile{
		&KvdbFile{
			File:   file,
			FileId: fileId,
			Type:   HintType,
			offset: 0,
		},
	}
}
