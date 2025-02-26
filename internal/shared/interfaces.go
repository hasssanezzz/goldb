package shared

import "io"

type WriteSeekCloser interface {
	io.Writer
	io.Seeker
	io.Closer
}
