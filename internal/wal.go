package internal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/hasssanezzz/goldb/shared"
)

type DiskWAL struct {
	source string
	writer io.WriteCloser
	mu     sync.Mutex
}

func NewDiskWAL(source string) (WAL, error) {
	w := &DiskWAL{source: source}
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
	w.mu.Lock()
	defer w.mu.Unlock()

	buffer := make([]byte, 0, shared.KeySize+shared.UintSize+len(entry.Value))

	// Key (256 bytes)
	buffer = append(buffer, shared.KeyToBytes(entry.Key)...)

	// Value size (4 bytes)
	binary.LittleEndian.AppendUint32(buffer, uint32(len(entry.Value)))

	// Value (variable length)
	if len(entry.Value) > 0 {
		buffer = append(buffer, entry.Value...)
	}

	_, err := w.writer.Write(buffer)
	if err != nil {
		return fmt.Errorf("WAL %q can not write log: %v", w.source, err)
	}

	return nil
}

func (w *DiskWAL) Retrieve() ([]WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// TODO: seperate decoding binary objects logic to a specialized component
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
	mp := map[string][]byte{} // to get the latest values of duplicate keys

	for {
		keyBytes, vlength := make([]byte, shared.KeySize), make([]byte, 4)

		// Read key
		_, err = buf.Read(keyBytes)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, fmt.Errorf("WAL %q can not be parsed: %v", w.source, err)
			}
		}

		// Read value length
		_, err = buf.Read(vlength)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, fmt.Errorf("WAL %q can not be parsed: %v", w.source, err)
			}
		}

		// Read value
		value := make([]byte, binary.LittleEndian.Uint32(vlength))
		_, err = buf.Read(value)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, fmt.Errorf("WAL %q can not be parsed: %v", w.source, err)
			}
		}

		mp[shared.TrimPaddedKey(string(keyBytes))] = value
	}

	for key, value := range mp {
		pairs = append(pairs, WALEntry{Key: key, Value: value})
	}

	return pairs, nil
}

func (w *DiskWAL) Clear() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return os.Truncate(w.source, 0)
}

func (w *DiskWAL) Close() error {
	return w.writer.Close()
}
