package engine

import (
	"fmt"
	"log"
	"path/filepath"

	"github.com/hasssanezzz/goldb-engine/index_manager"
	"github.com/hasssanezzz/goldb-engine/memtable"
	"github.com/hasssanezzz/goldb-engine/storage_manager"
)

const MemtableSizeThreshold = 500_000 // about 126MB of memory

type Engine struct {
	indexMangaer   *index_manager.IndexManager
	storageManager *storage_manager.StorageManager
	wal            *index_manager.WAL
}

func New(homepath string) (*Engine, error) {
	e := &Engine{}

	indexMangaer, err := index_manager.New(homepath)
	if err != nil {
		return nil, err
	}

	storageManager, err := storage_manager.New(filepath.Join(homepath, "data.bin"))
	if err != nil {
		return nil, err
	}

	wal, err := index_manager.NewWAL(filepath.Join(homepath, "wal.log.bin"))
	if err != nil {
		return nil, err
	}

	e.indexMangaer = indexMangaer
	e.storageManager = storageManager
	e.wal = wal

	return e, e.setEntriesFromWAL()
}

func (e *Engine) setEntriesFromWAL() error {
	entries, err := e.wal.ParseLogs()
	if err != nil {
		println("error parsing the logs")
		return err
	}

	for _, entry := range entries {
		if len(entry.Value) > 0 {
			// TODO - make logging conditional
			log.Printf("[WAL:SET] %q %X\n", entry.Key, entry.Value)
			if err := e.Set(entry.Key, entry.Value, true); err != nil {
				return err
			}
		} else {
			// TODO - make logging conditional
			log.Printf("[WAL:DEL] %q\n", entry.Key)
			if err := e.Delete(entry.Key, true); err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *Engine) Set(key string, value []byte, ignoreWAL ...bool) error {
	// first of all, write the pair to the WAL if not ingored.
	if len(ignoreWAL) == 0 {
		// TODO - make logging conditional
		log.Printf("[SET] %q %X\n", key, value)

		// when would I ignore writing to the WAL?
		// when the I am setting KV pairs from the WAL I don't want to rewrite
		// the pairs coming from the WAL to the WAL again.
		if err := e.wal.Log(key, value); err != nil {
			return err
		}
	}

	// periodic flush, after the memtable hits its threshold
	if e.indexMangaer.Memtable.Size >= MemtableSizeThreshold {
		// TODO - add locks to avoid concurrency issues.
		// NOTE - I temporary removed the `go` keyword
		func() {
			err := e.indexMangaer.Flush()
			if err != nil {
				log.Println("engine periodic flush error: ", err)
			}

			// if the flush was successful, clear the WAL
			e.wal.Clear()
		}()
	}

	offset, err := e.storageManager.WriteValue(value)
	if err != nil {
		return fmt.Errorf("db engine can not write (%q, %x): %v", key, value, err)
	}
	e.indexMangaer.Memtable.Set(key, memtable.IndexNode{
		Offset: offset,
		Size:   uint32(len(value)),
	})
	return nil
}

func (e *Engine) Get(key string) ([]byte, error) {
	// TODO - make logging conditional
	log.Printf("[GET] %q\n", key)

	indexNode, err := e.indexMangaer.Get(key)
	if err != nil {
		if _, ok := err.(*index_manager.ErrKeyNotFound); ok {
			return nil, err
		}
		return nil, fmt.Errorf("db engine can not locate key (%q): %v", key, err)
	}

	data, err := e.storageManager.ReadValue(indexNode)
	if err != nil {
		if e, ok := err.(*index_manager.ErrKeyNotFound); ok {
			e.Key = key
			return nil, err
		}
		return nil, fmt.Errorf("db engine can not read key (%q): %v", key, err)
	}

	return data, nil
}

func (e *Engine) Delete(key string, ignoreWAL ...bool) error {
	// first of all, write the pair to the WAL if not ingored.
	if len(ignoreWAL) == 0 {
		// TODO - make logging conditional
		log.Printf("[DEL] %q\n", key)

		// when would I ignore writing to the WAL?
		// when the I am setting KV pairs from the WAL I don't want to rewrite
		// the pairs coming from the WAL to the WAL again.
		if err := e.wal.Log(key, []byte{}); err != nil {
			return err
		}
	}

	e.indexMangaer.Delete(key)
	return nil
}

func (e *Engine) Close() {
	if e.indexMangaer.Memtable.Size > 0 {
		e.indexMangaer.Flush()
	}
	e.indexMangaer.Close()
	e.storageManager.Close()
}
