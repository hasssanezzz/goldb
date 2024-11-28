package engine

import (
	"fmt"
	"path/filepath"

	"github.com/hasssanezzz/goldb-engine/index_manager"
	"github.com/hasssanezzz/goldb-engine/memtable"
	"github.com/hasssanezzz/goldb-engine/storage_manager"
)

type Engine struct {
	indexMangaer   *index_manager.IndexManager
	storageManager *storage_manager.StorageManager
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

	e.indexMangaer = indexMangaer
	e.storageManager = storageManager

	return e, nil
}

func (e *Engine) Set(key string, value []byte) error {
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

func (e *Engine) Delete(key string) {
	e.indexMangaer.Delete(key)
}

func (e *Engine) Close() {
	if e.indexMangaer.Memtable.Size > 0 {
		e.indexMangaer.Flush()
	}
	e.indexMangaer.Close()
	e.storageManager.Close()
}
