package utils

import "fmt"

const (
	ERROR = "\033[1;31;40m[ERROR] %s\033[0m"
	WARN  = "\033[1;33;40m[WARN] %s\033[0m"
	INFO  = "\033[1;34;40m[INFO] %s\033[0m"
)

func WrapError(format string, args ...any) string {
	return fmt.Sprintf(ERROR, fmt.Sprintf(format, args...))
}

func WrapWarn(format string, args ...any) string {
	return fmt.Sprintf(WARN, fmt.Sprintf(format, args...))
}
func WrapInfo(format string, args ...any) string {
	return fmt.Sprintf(INFO, fmt.Sprintf(format, args...))
}
