package utils

type UnboundChan struct {
	in, out chan struct{}
	buffer  []struct{}
}

func NewUnboundChan() *UnboundChan {
	uc := &UnboundChan{
		in:     make(chan struct{}),
		out:    make(chan struct{}),
		buffer: make([]struct{}, 0, 10),
	}

	go func() {
		for range uc.in {
			select {
			case uc.out <- struct{}{}:
				continue
			default:
				// pass
			}

			uc.buffer = append(uc.buffer, struct{}{})
			for len(uc.buffer) > 0 {
				select {
				case _, ok := <-uc.in:
					if !ok {
						break
					}
					uc.buffer = append(uc.buffer, struct{}{})
				case uc.out <- struct{}{}:
					uc.buffer = uc.buffer[1:]
				}
			}
		}

		for len(uc.buffer) > 0 {
			uc.out <- struct{}{}
			uc.buffer = uc.buffer[1:]
		}

		close(uc.out)
	}()

	return uc
}

func (uc *UnboundChan) In() {
	uc.in <- struct{}{}
}

func (uc *UnboundChan) Out() bool {
	_, ok := <-uc.out
	return ok
}

func (uc *UnboundChan) Len() int64 {
	return int64(len(uc.buffer))
}

func (uc *UnboundChan) Close() {
	close(uc.in)
}
