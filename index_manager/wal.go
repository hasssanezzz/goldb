package index_manager

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
)

type WALOperation string

const WALOPSet WALOperation = "WALOPSet"
const WALOPDelete WALOperation = "WALOPDelete"

type WALEntry struct {
	Key   string
	Value []byte
}

type WAL struct {
	source string
	writer *os.File
}

func NewWAL(source string) (*WAL, error) {
	w := &WAL{source: source}
	return w, w.Open()
}

func (w *WAL) Open() error {
	wfile, err := os.OpenFile(w.source, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("WAL %q can not open file: %v", w.source, err)
	}
	w.writer = wfile
	return nil
}

func (w *WAL) Log(key string, value []byte) error {
	bytesToWrite := keyToBytes(key)
	valueLengthBuff := make([]byte, 4)
	valueLength := uint32(len(value))
	binary.LittleEndian.PutUint32(valueLengthBuff, valueLength)
	bytesToWrite = append(bytesToWrite, valueLengthBuff...)

	// if len(value) == 0 then this is a delete operation
	// if not, this is a set/put operation
	if len(value) > 0 {
		bytesToWrite = append(bytesToWrite, value...)
	}

	_, err := w.writer.Write(bytesToWrite)
	if err != nil {
		return fmt.Errorf("WAL %q can not write log: %v", w.source, err)
	}

	return nil
}

func (w *WAL) ParseLogs() ([]WALEntry, error) {
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

	for {
		keyBytes, vlength := make([]byte, 256), make([]byte, 4)
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

		pairs = append(pairs, WALEntry{
			Key:   strings.TrimRight(string(keyBytes), "\x00"),
			Value: value,
		})
	}

	return pairs, nil
}

func (w *WAL) Clear() error {
	return os.Truncate(w.source, 0)
}

func (w *WAL) Close() {
	w.Close()
}
