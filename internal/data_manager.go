package internal

import (
	"fmt"
	"io"
	"os"

	"github.com/hasssanezzz/goldb/shared"
)

// TODO: make this is a pair value manager not a storage manager, and create
// a seperate actuall storage manager which concerns itself with dealing
// with disk operations, the value manager will depend on the storage manager.

type DiskDataManager struct {
	writer   WriteSeekCloser
	reader   io.ReadSeekCloser
	filename string
}

func NewDiskDataManager(filename string) (DataManager, error) {
	sm := &DiskDataManager{filename: filename}
	return sm, sm.Open()
}

func (s *DiskDataManager) Open() error {
	wfile, err := os.OpenFile(s.filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("storage manager can not open file for appending %q: %v", s.filename, err)
	}
	rfile, err := os.Open(s.filename)
	if err != nil {
		return fmt.Errorf("storage manager can not open file for reading %q: %v", s.filename, err)
	}
	s.writer = wfile
	s.reader = rfile
	return nil
}

func (s *DiskDataManager) Store(value []byte) (Position, error) {
	offset, err := s.writer.Seek(0, io.SeekEnd)
	if err != nil {
		return Position{}, fmt.Errorf("storage manager can not seek to end: %v", err)
	}

	_, err = s.writer.Write(value)
	if err != nil {
		return Position{}, fmt.Errorf("storage manager can not write value %q: %v", value, err)
	}
	return Position{uint32(offset), uint32(len(value))}, err
}

// Retrieve gets a value based on node position
func (s *DiskDataManager) Retrieve(position Position) ([]byte, error) {
	if position.Size == 0 {
		return nil, &shared.ErrKeyNotFound{}
	}

	_, err := s.reader.Seek(int64(position.Offset), io.SeekStart)
	if err != nil {
		return []byte{}, fmt.Errorf("storage manager can not read (%d, %d): %v", position.Offset, position.Size, err)
	}
	buf := make([]byte, position.Size)
	_, err = s.reader.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// Compact deletes all unused values
func (s *DiskDataManager) Compact() error {
	panic("unimplemented")
}

func (s *DiskDataManager) Close() error {
	err := s.writer.Close()
	if err != nil {
		return err
	}
	err = s.reader.Close()
	return err
}
