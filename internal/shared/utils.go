package shared

import (
	"strings"
)

func KeyToBytes(key string, keySize uint32) ([]byte, error) {
	keyByteLength := len([]byte(key))
	paddedKey := key + strings.Repeat(string("\x00"), int(keySize)-keyByteLength)
	results := []byte(paddedKey)
	if len(results) != int(keySize) {
		return nil, &ErrKeyTooLong{key, keySize}
	}
	return results, nil
}

// TrimPaddedKey removes the null bytes from the end of a string.
func TrimPaddedKey(key string) string {
	return strings.TrimRight(key, "\x00")
}
