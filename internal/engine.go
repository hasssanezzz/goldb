package internal

import (
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"github.com/hasssanezzz/goldb/shared"
)

type Engine struct {
	Config         shared.EngineConfig
	indexManager   *IndexManager
	storageManager DataManager
	wal            WAL
}

func NewEngine(homepath string, configs ...shared.EngineConfig) (*Engine, error) {
	e := &Engine{}

	config := shared.DefaultConfig
	if len(configs) > 0 {
		config = configs[0]
	}
	config.Homepath = homepath
	e.Config = config

	indexManager, err := NewIndexManager(&config)
	if err != nil {
		return nil, err
	}

	storageManager, err := NewDiskDataManager(filepath.Join(homepath, "data.bin"))
	if err != nil {
		return nil, err
	}

	wal, err := NewDiskWAL(filepath.Join(homepath, "wal.log.bin"))
	if err != nil {
		return nil, err
	}

	e.indexManager = indexManager
	e.storageManager = storageManager
	e.wal = wal

	return e, e.setEntriesFromWAL()
}

func (e *Engine) setEntriesFromWAL() error {
	entries, err := e.wal.Retrieve()
	if err != nil {
		println("error parsing the logs")
		return err
	}

	for _, entry := range entries {
		if len(entry.Value) > 0 {
			// TODO - make logging conditional
			// log.Printf("[WAL:SET] %q %X\n", entry.Key, entry.Value)
			if err := e.Set(entry.Key, entry.Value, true); err != nil {
				return err
			}
		} else {
			// TODO - make logging conditional
			// log.Printf("[WAL:DEL] %q\n", entry.Key)
			if err := e.Delete(entry.Key, true); err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *Engine) Scan(pattern string) ([]string, error) {
	keys, err := e.indexManager.Keys()
	if err != nil {
		return nil, err
	}

	// if not pattern exists, return all the keys
	if len(pattern) == 0 {
		return keys, nil
	}

	results := []string{}
	for _, key := range keys {
		if strings.HasPrefix(key, pattern) {
			results = append(results, key)
		}
	}

	return results, nil
}

func (e *Engine) Get(key string) ([]byte, error) {
	// make sure key size is valid
	if len([]byte(key)) > int(e.Config.KeySize) {
		return nil, &shared.ErrKeyTooLong{Key: key, KeySize: e.Config.KeySize}
	}

	indexNode, err := e.indexManager.Get(key)
	if err != nil {
		if _, ok := err.(*shared.ErrKeyNotFound); ok {
			return nil, err
		}
		return nil, fmt.Errorf("db engine can not locate key (%q): %v", key, err)
	}

	data, err := e.storageManager.Retrieve(indexNode)
	if err != nil {
		if e, ok := err.(*shared.ErrKeyNotFound); ok {
			e.Key = key
			return nil, err
		}
		return nil, fmt.Errorf("db engine can not read key (%q): %v", key, err)
	}

	return data, nil
}

func (e *Engine) Set(key string, value []byte, ignoreWAL ...bool) error {
	// make sure key size is valid
	if len([]byte(key)) > int(e.Config.KeySize) {
		return &shared.ErrKeyTooLong{Key: key, KeySize: e.Config.KeySize}
	}

	// first of all after validating the key size, write the pair to the WAL if not ingored.
	if len(ignoreWAL) == 0 {
		// when would I ignore writing to the WAL?
		// when the I am setting KV pairs from the WAL I don't want to rewrite
		// the pairs coming from the WAL to the WAL again.
		if err := e.wal.Append(WALEntry{key, value}); err != nil {
			return err
		}
	}

	// Flush if the memtable exceeds its threshold
	if e.indexManager.memtable.Size() >= e.Config.MemtableSizeThreshold {
		// TODO: add latches to avoid concurrency issues.
		// NOTE: I temporary removed the `go` keyword
		func() {
			err := e.indexManager.Flush()
			if err != nil {
				log.Println("engine periodic flush error: ", err)
			}

			// if the flush was successful, clear the WAL
			e.wal.Clear()
		}()

		err := e.indexManager.CompactionCheck()
		if err != nil {
			panic(err)
		}
	}

	position, err := e.storageManager.Store(value)
	if err != nil {
		return fmt.Errorf("engine failed to write (%q, %x): %v", key, value, err)
	}

	e.indexManager.memtable.Set(KVPair{
		Key:   key,
		Value: position,
	})

	return nil
}

func (e *Engine) Delete(key string, ignoreWAL ...bool) error {
	// make sure key size is valid
	if len([]byte(key)) > int(e.Config.KeySize) {
		return &shared.ErrKeyTooLong{Key: key, KeySize: e.Config.KeySize}
	}

	// first of all after validating the key size
	// write the pair (with empty value) to the WAL if not ingored.
	if len(ignoreWAL) == 0 {
		// when would I ignore writing to the WAL?
		// when the I am setting KV pairs from the WAL I don't want to rewrite
		// the pairs coming from the WAL to the WAL again.
		if err := e.wal.Append(WALEntry{key, []byte{}}); err != nil {
			return err
		}
	}

	e.indexManager.Delete(key)
	return nil
}

func (e *Engine) Close() error {
	if err := e.indexManager.Close(); err != nil {
		return err
	}
	if err := e.storageManager.Close(); err != nil {
		return err
	}
	return nil
}
