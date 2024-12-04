package shared

import (
	"strings"
)

func KeyToBytes(key string) ([]byte, error) {
	keyByteLength := len([]byte(key))
	paddedKey := key + strings.Repeat(string("\x00"), 256-keyByteLength)
	results := []byte(paddedKey)
	if len(results) != 256 {
		return nil, &ErrKeyTooLong{key}
	}
	return results, nil
}
