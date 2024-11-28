package index_manager

import "fmt"

type ErrKeyRemoved struct{ Key string }
type ErrKeyNotFound struct{ Key string }

func (e *ErrKeyNotFound) Error() string {
	return fmt.Sprintf("key %q can not be found", e.Key)
}

func (e *ErrKeyRemoved) Error() string {
	return fmt.Sprintf("key %q is deleted", e.Key)
}
