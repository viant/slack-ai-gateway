//go:build !darwin && !linux

package sqlite

import (
	"fmt"
	"os"
)

func acquireLock(string) (*os.File, error) {
	return nil, fmt.Errorf("stage A SQLite locking supports macOS and Linux only")
}
