package poller

import "syscall"

type KqueuePoller struct {
	kq *int
}

func NewKqueuePoller() (*KqueuePoller, error) {
	kqFd, err := syscall.Kqueue()
	if err != nil {
		return nil, err
	}

	return &KqueuePoller{
		kq: &kqFd,
	}, nil
}

func (kp *KqueuePoller) Register(changes []Pevent) error {
	kchanges := kp.fromPevent(changes)
	_, err := syscall.Kevent(*kp.kq, kchanges, nil, nil)
	return err
}

func (kp *KqueuePoller) Wait(events []Pevent) (int, error) {
	kevents := kp.fromPevent(events)
	n, err := syscall.Kevent(*kp.kq, nil, kevents, nil)
	if err != nil {
		return 0, err
	}
	kp.toPevent(kevents, events)
	return n, nil
}

func (kp *KqueuePoller) fromPevent(events []Pevent) []syscall.Kevent_t {
	kevents := make([]syscall.Kevent_t, 0, len(events))
	for _, pevt := range events {
		kevents = append(kevents, syscall.Kevent_t{
			Ident:  pevt.ConnFd,
			Filter: int16(pevt.Operation),
			Flags:  uint16(pevt.Flag),
			Udata:  pevt.UserData,
		})
	}
	return kevents
}

func (kp *KqueuePoller) toPevent(kevents []syscall.Kevent_t, pevent []Pevent) {
	for idx, kevt := range kevents {
		pevent[idx].ConnFd = kevt.Ident
		pevent[idx].Operation = int64(kevt.Filter)
		pevent[idx].Flag = int64(kevt.Flags)
		pevent[idx].UserData = kevt.Udata
	}
}

func (kq *KqueuePoller) Close() error {
	var err error
	if kq.kq != nil {
		err = syscall.Close(*kq.kq)
	}
	return err
}
