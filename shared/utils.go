package shared

import (
	"strings"
)

// TODO: disallow \x00 in keys
const KeySize = 256

func KeyToBytes(key string) []byte {
	keyBytes := []byte(key)
	if len(keyBytes) > 256 {
		return keyBytes[:256] // truncate
	}

	// Pad with null bytes
	padded := make([]byte, 256)
	copy(padded, keyBytes)
	return padded
}

// TrimPaddedKey removes the null bytes from the end of a string.
func TrimPaddedKey(key string) string {
	return strings.TrimRight(key, "\x00")
}
