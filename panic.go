package main

import "fmt"

// die returns a value of type T but always panics.
// Use it as: `return die[bool]("message: %s", x)` to satisfy typecheck without unreachable code.
func die[T any](format string, args ...any) T {
	panic(fmt.Sprintf(format, args...))
}
