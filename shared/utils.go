package shared

import (
	"strings"
)

// TODO: disallow \x00 in keys
const KeySize = 256

func KeyToBytes(key string) []byte {
	keyBytes := []byte(key)
	if len(keyBytes) > KeySize {
		return keyBytes[:KeySize] // truncate
	}

	// Pad with null bytes
	padded := make([]byte, KeySize)
	copy(padded, keyBytes)
	return padded
}

// TrimPaddedKey removes the null bytes from the end of a string.
func TrimPaddedKey(key string) string {
	return strings.TrimRight(key, "\x00")
}
