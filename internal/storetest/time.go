package storetest

import "time"

// Time gives fixture epochs the same native UTC millisecond type as storage.
func Time(milliseconds int64) time.Time { return time.UnixMilli(milliseconds).UTC() }

// Pointer preserves the difference between a fixture's explicit value and NULL.
func Pointer[T any](value T) *T { return &value }
