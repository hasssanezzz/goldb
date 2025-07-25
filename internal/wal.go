package internal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/hasssanezzz/goldb/shared"
)

type DiskWAL struct {
	keySize uint32
	source  string
	writer  *os.File
}

func NewDiskWAL(source string, keySize uint32) (WAL, error) {
	w := &DiskWAL{source: source, keySize: keySize}
	return w, w.Open()
}

func (w *DiskWAL) Open() error {
	wfile, err := os.OpenFile(w.source, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("WAL %q can not open file: %v", w.source, err)
	}
	w.writer = wfile
	return nil
}

func (w *DiskWAL) Append(entry WALEntry) error {
	bytesToWrite, err := shared.KeyToBytes(entry.Key, w.keySize)
	if err != nil {
		return err
	}

	valueLengthBuff := make([]byte, 4)
	valueLength := uint32(len(entry.Value))
	binary.LittleEndian.PutUint32(valueLengthBuff, valueLength)
	bytesToWrite = append(bytesToWrite, valueLengthBuff...)

	// if len(value) == 0 then this is a delete operation
	// if not, this is a set/put operation
	if len(entry.Value) > 0 {
		bytesToWrite = append(bytesToWrite, entry.Value...)
	}

	_, err = w.writer.Write(bytesToWrite)
	if err != nil {
		return fmt.Errorf("WAL %q can not write log: %v", w.source, err)
	}

	return nil
}

func (w *DiskWAL) Retrieve() ([]WALEntry, error) {
	rfile, err := os.Open(w.source)
	if err != nil {
		return nil, fmt.Errorf("WAL %q can not be opened: %v", w.source, err)
	}
	defer rfile.Close()

	buf := bytes.NewBuffer(nil)
	_, err = io.Copy(buf, rfile)
	if err != nil {
		return nil, fmt.Errorf("WAL %q can not be read: %v", w.source, err)
	}

	pairs := []WALEntry{}
	mp := map[string][]byte{}

	for {
		keyBytes, vlength := make([]byte, w.keySize), make([]byte, 4)
		_, err = buf.Read(keyBytes)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, fmt.Errorf("WAL %q can not be parsed: %v", w.source, err)
			}
		}

		_, err = buf.Read(vlength)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, fmt.Errorf("WAL %q can not be parsed: %v", w.source, err)
			}
		}

		value := make([]byte, binary.LittleEndian.Uint32(vlength))
		_, err = buf.Read(value)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, fmt.Errorf("WAL %q can not be parsed: %v", w.source, err)
			}
		}

		// add to the to map not the pairs array for compaction
		mp[shared.TrimPaddedKey(string(keyBytes))] = value
	}

	for key, value := range mp {
		pairs = append(pairs, WALEntry{Key: key, Value: value})
	}

	return pairs, nil
}

func (w *DiskWAL) Clear() error {
	return os.Truncate(w.source, 0)
}

func (w *DiskWAL) Close() error {
	return w.writer.Close()
}
