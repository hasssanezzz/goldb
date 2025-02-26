package shared

import (
	"fmt"
)

const UintSize = 4

type ErrKeyTooLong struct {
	Key     string
	KeySize uint32
}

func (e *ErrKeyTooLong) Error() string {
	return fmt.Sprintf("key %q exceeded max key size %d", e.Key, e.KeySize)
}

type ErrKeyNotFound struct{ Key string }

func (e *ErrKeyNotFound) Error() string {
	return fmt.Sprintf("key %q can not be found", e.Key)
}

type ErrKeyRemoved struct{ Key string }

func (e *ErrKeyRemoved) Error() string {
	return fmt.Sprintf("key %q is deleted", e.Key)
}
