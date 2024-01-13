package utils

import "sync"

func WrapLock(lock *sync.Mutex, fn func()) {
	lock.Lock()
	defer lock.Unlock()

	fn()
}
