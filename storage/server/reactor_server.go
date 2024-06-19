package server

import (
	"context"
	"github.com/bytedance/gopkg/util/gopool"
	"github.com/pkg/errors"
	"log"
	"math/rand"
	"net"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

type ReactorServer struct {
	mutex           sync.Mutex
	serverTransport net.Listener
	bizHandler      simpleHandler
	pool            gopool.Pool
	dp              *dispatcher
	reactors        map[int64]*reactor
	stop            chan struct{}
	done            sync.WaitGroup
}

type connWrapper struct {
	tcpConn *net.TCPConn
	fd      *uintptr
}

type reactor struct {
	id       int64
	connects chan *connWrapper
	srv      *ReactorServer
	w        *waiter
	poller   poller
}

func newReactor(id int64, srv *ReactorServer, poller poller) *reactor {
	r := &reactor{
		srv:      srv,
		id:       id,
		connects: make(chan *connWrapper),
		poller:   poller,
		w: &waiter{
			events: make(chan pevent),
			poller: poller,
		},
	}
	r.w.parent = r
	return r
}

func (r *reactor) run() {
	defer r.srv.done.Done()
	log.Printf("reactor #%d start", r.id)

	connects := r.connects
	r.srv.done.Add(1)
	r.srv.pool.Go(r.w.run)
	for {
		select {
		case wrapper, ok := <-connects:
			// output been close by dispatcher
			if !ok {
				log.Printf("reactor #%d ready to closes kqfd", r.id)
				if e := r.poller.close(); e != nil {
					log.Printf("reactor #%d close poller failed. err: %v", r.id, e)
				}
				connects = nil
				log.Printf("reactor #%d output set to nil", r.id)
				continue
			}
			log.Printf("reactor #%d receive output connection", r.id)
			// get wrapper file descriptor
			if wrapper.fd == nil {
				file, err := wrapper.tcpConn.File()
				if err != nil {
					log.Println(err)
					continue
				}
				fd := file.Fd()
				wrapper.fd = &fd
			}

			log.Printf("reactor #%d ready to register event, wrapper fd: %v", r.id, uint64(*wrapper.fd))

			changes := []pevent{{
				connFd:    uint64(*wrapper.fd),
				operation: syscall.EVFILT_READ,
				flag:      syscall.EV_ADD | syscall.EV_ENABLE | syscall.EV_ONESHOT, // edge trigger mode
				userData:  (*byte)(unsafe.Pointer(wrapper)),
			}}
			// register event to kqueue
			if err := r.poller.register(changes); err != nil {
				e := wrapper.tcpConn.Close()
				if e != nil {
					err = errors.Wrap(err, e.Error())
				}
				log.Println(err, changes, wrapper.tcpConn.RemoteAddr(), wrapper.tcpConn.LocalAddr())
				continue
			}

			log.Printf("reactor #%d register event success", r.id)
		case evt, ok := <-r.w.output():
			if !ok {
				log.Printf("reactor #%d stop", r.id)
				return
			}
			log.Printf("reactor #%d handle event", r.id)
			// in this case, close is already called.
			if r.srv.pool == nil {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), processTimeout)
			r.srv.done.Add(2)
			r.srv.pool.Go(func() {
				wrapper := (*connWrapper)(unsafe.Pointer(evt.userData))
				r.srv.handler(ctx, cancel, wrapper)
			})
			r.srv.pool.Go(func() { r.srv.notifier(ctx, cancel) })
		}
	}
}

func (r *reactor) input() chan<- *connWrapper {
	return r.connects
}

type waiter struct {
	events chan pevent
	parent *reactor
	poller poller
}

func (w *waiter) run() {
	log.Printf("waiter #%d start", w.parent.id)
	defer w.parent.srv.done.Done()
	// event buf
	evts := make([]pevent, 10)
	for {
		log.Printf("waiter #%d ready to wait event trigger", w.parent.id)

		// wait for event to be trigger
		n, err := w.poller.wait(evts)
		if err != nil {
			log.Printf("waiter #%d stop, err: %v", w.parent.id, err)
			close(w.events)
			return
		}

		log.Printf("waiter #%d event trigger success, evts: %#v, n: %d", w.parent.id, evts, n)

		for i := 0; i < n; i++ {
			evt := evts[i]
			switch {
			// we do not care about eof
			case evt.flag&syscall.EV_EOF != 0:
				log.Printf("waiter #%d meet eof, skip", w.parent.id)
			default:
				w.events <- evt
				log.Printf("waiter #%d sent evt to reactor success", w.parent.id)
			}
		}
	}
}

func (w *waiter) output() <-chan pevent {
	return w.events
}

type pevent struct {
	connFd    uint64
	operation int64
	flag      int64
	userData  *byte
}

type poller interface {
	register(changes []pevent) error
	wait(events []pevent) (int, error)
	close() error
}

type kqueuePoller struct {
	kq *int
}

func newKqueuePoller() (*kqueuePoller, error) {
	kqFd, err := syscall.Kqueue()
	if err != nil {
		return nil, err
	}

	return &kqueuePoller{
		kq: &kqFd,
	}, nil
}

func (kp *kqueuePoller) register(changes []pevent) error {
	kchanges := kp.fromPevent(changes)
	_, err := syscall.Kevent(*kp.kq, kchanges, nil, nil)
	return err
}

func (kp *kqueuePoller) wait(events []pevent) (int, error) {
	kevents := kp.fromPevent(events)
	n, err := syscall.Kevent(*kp.kq, nil, kevents, nil)
	if err != nil {
		return 0, err
	}
	kp.toPevent(kevents, events)
	return n, nil
}

func (kp *kqueuePoller) fromPevent(events []pevent) []syscall.Kevent_t {
	kevents := make([]syscall.Kevent_t, 0, len(events))
	for _, pevt := range events {
		kevents = append(kevents, syscall.Kevent_t{
			Ident:  pevt.connFd,
			Filter: int16(pevt.operation),
			Flags:  uint16(pevt.flag),
			Udata:  pevt.userData,
		})
	}
	return kevents
}

func (kp *kqueuePoller) toPevent(kevents []syscall.Kevent_t, pevent []pevent) {
	for idx, kevt := range kevents {
		pevent[idx].connFd = kevt.Ident
		pevent[idx].operation = int64(kevt.Filter)
		pevent[idx].flag = int64(kevt.Flags)
		pevent[idx].userData = kevt.Udata
	}
}

func (kq *kqueuePoller) close() error {
	var err error
	if kq.kq != nil {
		err = syscall.Close(*kq.kq)
	}
	return err
}

// todo: extract to config
const (
	numReactor     = 3
	pollCapacity   = 1000
	processTimeout = 1 * time.Second
)

func NewReactorServer(addr string, handler simpleHandler) (*ReactorServer, error) {
	var err error
	srv := &ReactorServer{
		bizHandler: handler,
		reactors:   make(map[int64]*reactor),
		pool:       gopool.NewPool("handlers", pollCapacity, gopool.NewConfig()),
		stop:       make(chan struct{}),
	}

	// init dispatcher
	srv.dp = &dispatcher{
		connections: make(chan *connWrapper),
		parent:      srv,
	}

	// init reactors
	for i := 0; i < numReactor; i++ {
		tmpIdx := int64(i)
		kp, err := newKqueuePoller()
		if err != nil {
			return nil, err
		}
		srv.reactors[tmpIdx] = newReactor(tmpIdx, srv, kp)
	}

	// init server transport
	srv.serverTransport, err = net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return srv, nil
}

type dispatcher struct {
	connections chan *connWrapper
	parent      *ReactorServer
}

func (dp *dispatcher) run() {
	defer dp.parent.done.Done()
	log.Println("dispatcher start")

	for conn := range dp.connections {
		log.Println("dispatcher receive connection")
		// random load balance
		id := rand.Int63n(numReactor)
		if reactor, exist := dp.parent.reactors[id]; exist {
			reactor.input() <- conn
			log.Printf("dispatcher send connection to reactor #%v success", id)
		} else {
			log.Printf("dispatcher find reactor #%v not exist", id)
		}
	}

	for _, reactor := range dp.parent.reactors {
		if reactor != nil {
			close(reactor.input())
		}
	}

	log.Println("dispatcher stop")
}

func (dp *dispatcher) input() chan<- *connWrapper {
	return dp.connections
}

func (rs *ReactorServer) Serve() error {
	rs.done.Add(numReactor + 1)

	// start connection dispatcher
	rs.pool.Go(rs.dp.run)
	for _, reactor := range rs.reactors {
		rs.pool.Go(reactor.run)
	}

	// mainReactor & acceptor
	for {
		conn, err := rs.serverTransport.Accept()
		if err != nil {
			rs.mutex.Lock()
			close(rs.dp.input())
			e := rs.close()
			if e != nil {
				e = errors.Wrap(e, err.Error())
			}
			rs.done.Wait()
			rs.clearState()
			rs.mutex.Unlock()
			return e
		}
		rs.mutex.Lock()
		rs.dp.input() <- &connWrapper{
			tcpConn: conn.(*net.TCPConn),
			fd:      nil,
		}
		rs.mutex.Unlock()
	}
}

func (rs *ReactorServer) handler(ctx context.Context, cancel context.CancelFunc, wrapper *connWrapper) {
	defer func() {
		rs.done.Done()
		cancel()
	}()
	rs.bizHandler(ctx, wrapper.tcpConn)
	rs.mutex.Lock()
	select {
	case <-rs.stop:
		rs.mutex.Unlock()
		log.Printf("bizHandler ready to close connFd %d", *wrapper.fd)
		if e := wrapper.tcpConn.Close(); e != nil {
			log.Printf("bizHandler close connection failed. err: %v", e)
			return
		}
	default:
		rs.dp.input() <- wrapper // reuse long connection
		rs.mutex.Unlock()
	}
}

func (rs *ReactorServer) notifier(ctx context.Context, cancel context.CancelFunc) {
	defer rs.done.Done()
	select {
	case <-rs.stop:
		cancel()
	case <-ctx.Done():
		// do nothing
	}
}

func (rs *ReactorServer) Close() error {
	rs.mutex.Lock()
	if err := rs.close(); err != nil {
		return err
	}
	rs.mutex.Unlock()
	rs.done.Wait()
	rs.mutex.Lock()
	rs.clearState()
	rs.mutex.Unlock()
	return nil
}

func (rs *ReactorServer) close() error {
	select {
	case <-rs.stop:
		return nil
		// stop already closed
	default:
		close(rs.stop)
		return rs.serverTransport.Close()
	}
}

func (rs *ReactorServer) clearState() {
	rs.pool = nil
	rs.serverTransport = nil
	rs.reactors = nil
	rs.bizHandler = nil
	rs.reactors = nil
}
