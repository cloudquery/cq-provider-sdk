//go:build freebsd

package limit

import "errors"

func calculateFileLimit() (uint64, error) {
	return 0, errors.New("file descriptors limiter not supported on this platform")
}
