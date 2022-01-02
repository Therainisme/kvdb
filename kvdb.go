package kvdb

func Open(directoryName string) *KvdbHandle {
	// todo
	return &KvdbHandle{}
}

type KvdbHandle struct {
	DirectoryName string
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
