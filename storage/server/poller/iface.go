package poller

type Pevent struct {
	ConnFd    uint64
	Operation int64
	Flag      int64
	UserData  *byte
}

type Poller interface {
	Register(changes []Pevent) error
	Wait(events []Pevent) (int, error)
	Close() error
}
