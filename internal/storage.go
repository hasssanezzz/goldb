package internal

type DiskStorageManager struct {
	source string
}

func NewDiskStorageManager(source string) (*DiskStorageManager, error) {
	sm := &DiskStorageManager{
		source: source,
	}

	return sm, nil
}
