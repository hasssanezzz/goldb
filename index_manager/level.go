package index_manager

import (
	"fmt"
	"os"
)

type Level struct {
	Meta TableMetadata
	file *os.File
}

func NewLevel(path string, serial int) *Level {
	lvl := &Level{}
	lvl.Meta.Serial = uint32(serial)
	lvl.Meta.Path = path
	return lvl
}

func (l *Level) Open() error {
	file, err := os.Open(l.Meta.Path)
	if err != nil {
		return fmt.Errorf("can not open level %q: %v", l.Meta.Path, err)
	}
	l.file = file
	// l.ParseMetadata()
	return nil
}
