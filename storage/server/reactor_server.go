package server

import (
	"context"
	"github.com/Trinoooo/eggie_kv/storage/server/poller"
	"github.com/bytedance/gopkg/util/gopool"
	"github.com/pkg/errors"
	"log"
	"math/rand"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

type ReactorServer struct {
	mutex           sync.Mutex
	serverTransport *Listener
	bizHandler      simpleHandler
	pool            gopool.Pool
	dp              *dispatcher
	reactors        map[int64]*reactor
	stop            chan struct{}
	done            sync.WaitGroup
	metricsHelper   *MetricsHelper
}

type reactor struct {
	srv      *ReactorServer
	id       int64
	connects chan *Conn
	w        *waiter
	p        poller.Poller
}

func newReactor(id int64, srv *ReactorServer, p poller.Poller) *reactor {
	r := &reactor{
		srv:      srv,
		id:       id,
		connects: make(chan *Conn, reactorInputBufferSize),
		p:        p,
		w: &waiter{
			events: make(chan poller.Pevent, waiterOutputBufferSize),
			p:      p,
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
		case conn, ok := <-connects:
			// output been close by dispatcher
			if !ok {
				log.Printf("reactor #%d ready to closes poller", r.id)
				if e := r.p.Close(); e != nil {
					log.Printf("reactor #%d close p failed. err: %v", r.id, e)
				}
				connects = nil
				log.Printf("reactor #%d output set to nil", r.id)
				continue
			}

			log.Printf("reactor #%d ready to register event, remote addr: %v, local addr: %v, fd: %v", r.id, conn.RemoteAddr(), conn.LocalAddr(), conn.fd)

			changes := []poller.Pevent{{
				ConnFd:    uint64(conn.fd),
				Operation: syscall.EVFILT_READ,
				Flag:      syscall.EV_ADD | syscall.EV_ENABLE | syscall.EV_ONESHOT, // edge trigger mode
				UserData:  *(**byte)(unsafe.Pointer(&conn)),                        // bugfix：conn和byte内存大小不同，直接指针转换会有gc marked free object in span 风险
			}}
			// register event to poller
			if err := r.p.Register(changes); err != nil && err != syscall.EINTR {
				e := conn.Close()
				if e != nil {
					err = errors.Wrap(err, e.Error())
				}
				log.Println(err, changes, conn, conn.RemoteAddr(), conn.LocalAddr())
				continue
			}
			log.Printf("reactor #%d register event success, evt remote addr: %v, local addr: %v, fd: %v", r.id, conn.RemoteAddr(), conn.LocalAddr(), conn.fd)
		case evt, ok := <-r.w.output():
			if !ok {
				log.Printf("reactor #%d stop", r.id)
				return
			}
			conn := *(**Conn)(unsafe.Pointer(&evt.UserData))
			log.Printf("reactor #%d handle event, event remote addr: %v, local addr: %v, fd: %v", r.id, conn.RemoteAddr(), conn.LocalAddr(), conn.fd)
			// in this case, close is already called.
			if r.srv.pool == nil {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), processTimeout)
			r.srv.done.Add(2)
			r.srv.pool.Go(func() {
				r.srv.handler(ctx, cancel, conn)
			})
			r.srv.pool.Go(func() { r.srv.notifier(ctx, cancel) })
		}
	}
}

func (r *reactor) input() chan<- *Conn {
	return r.connects
}

type waiter struct {
	events chan poller.Pevent
	parent *reactor
	p      poller.Poller
}

func (w *waiter) run() {
	log.Printf("waiter #%d start", w.parent.id)
	defer w.parent.srv.done.Done()
	// event buf
	evts := make([]poller.Pevent, 10)
	for {
		log.Printf("waiter #%d ready to wait event trigger", w.parent.id)

		// wait for event to be trigger
		n, err := w.p.Wait(evts)
		if err != nil && err != syscall.EINTR { // bugfix: ignore EINTR
			log.Printf("waiter #%d stop, err: %v", w.parent.id, err)
			close(w.events)
			return
		}

		log.Printf("waiter #%d event trigger success, evts: %#v, n: %d", w.parent.id, evts, n)

		for i := 0; i < n; i++ {
			evt := evts[i]
			conn := *(**Conn)(unsafe.Pointer(&evt.UserData))
			switch {
			case evt.Flag&syscall.EV_EOF != 0:
				log.Printf("waiter #%d meet eof, remote addr: %v, local addr: %v, fd: %v", w.parent.id, conn.RemoteAddr(), conn.LocalAddr(), conn.fd)
				log.Printf("waiter #%d close server connection, err: %v", w.parent.id, conn.Close())
			default:
				log.Printf("waiter #%d ready to send evt to reactor, evt remote addr: %v, local addr: %v, fd: %v", w.parent.id, conn.RemoteAddr(), conn.LocalAddr(), conn.fd)
				w.events <- evt
				log.Printf("waiter #%d sent evt to reactor success, evt remote addr: %v, local addr: %v, fd: %v", w.parent.id, conn.RemoteAddr(), conn.LocalAddr(), conn.fd)
			}
		}
	}
}

func (w *waiter) output() <-chan poller.Pevent {
	return w.events
}

// todo: extract to config
const (
	numReactor     = 3
	pollCapacity   = 1000
	processTimeout = 1 * time.Second

	dispatcherInputBufferSize = 10
	reactorInputBufferSize    = 10
	waiterOutputBufferSize    = 10
)

func NewReactorServer(addr [4]byte, port int, handler simpleHandler) (*ReactorServer, error) {
	var err error
	srv := &ReactorServer{
		bizHandler:    handler,
		reactors:      make(map[int64]*reactor),
		pool:          gopool.NewPool("handlers", pollCapacity, gopool.NewConfig()),
		stop:          make(chan struct{}),
		metricsHelper: NewMetricsHelper(),
	}

	// init dispatcher
	srv.dp = &dispatcher{
		connections: make(chan *Conn, dispatcherInputBufferSize),
		parent:      srv,
	}

	// init reactors
	for i := 0; i < numReactor; i++ {
		tmpIdx := int64(i)
		kp, err := poller.NewKqueuePoller()
		if err != nil {
			return nil, err
		}
		srv.reactors[tmpIdx] = newReactor(tmpIdx, srv, kp)
	}

	// init server transport
	srv.serverTransport, err = Listen(addr, port)
	if err != nil {
		return nil, err
	}
	return srv, nil
}

type dispatcher struct {
	connections chan *Conn
	parent      *ReactorServer
}

func (dp *dispatcher) run() {
	defer dp.parent.done.Done()
	log.Println("dispatcher start")

	for conn := range dp.connections {
		log.Printf("dispatcher receive connection, remote addr: %v, local addr: %v, fd: %v", conn.RemoteAddr(), conn.LocalAddr(), conn.fd)
		// random load balance
		id := rand.Int63n(numReactor)
		if reactor, exist := dp.parent.reactors[id]; exist {
			reactor.input() <- conn
			log.Printf("dispatcher send connection to reactor #%v success, remote addr: %v, local addr: %v, fd: %v", id, conn.RemoteAddr(), conn.LocalAddr(), conn.fd)
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

func (dp *dispatcher) input() chan<- *Conn {
	return dp.connections
}

func (rs *ReactorServer) Serve() error {
	rs.done.Add(numReactor + 1)

	// start connection dispatcher
	rs.pool.Go(rs.dp.run)
	for _, reactor := range rs.reactors {
		rs.pool.Go(reactor.run)
	}

	// acceptor
	for {
		log.Printf("acceptor ready to accept connections")
		conn, err := rs.serverTransport.Accept()
		if err != nil && err != syscall.EINTR {
			log.Printf("error occur when accept connection, err: %v", err)
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
		log.Printf("acceptor accept connection, remote addr: %v, local addr: %v, fd: %v", conn.RemoteAddr(), conn.LocalAddr(), conn.fd)
		rs.metricsHelper.ConnectionAcceptCounter.Inc()
		rs.mutex.Lock()
		log.Printf("acceptor ready to send connection, remote addr: %v, local addr: %v, fd: %v", conn.RemoteAddr(), conn.LocalAddr(), conn.fd)
		rs.dp.input() <- conn
		log.Printf("acceptor success send connection, remote addr: %v, local addr: %v, fd: %v", conn.RemoteAddr(), conn.LocalAddr(), conn.fd)
		rs.mutex.Unlock()
	}
}

func (rs *ReactorServer) handler(ctx context.Context, cancel context.CancelFunc, conn *Conn) {
	defer func() {
		rs.done.Done()
		cancel()
	}()
	log.Printf("bizHandler ready to process socket fd: %d, remote addr: %v, local addr: %v", conn.fd, conn.RemoteAddr(), conn.LocalAddr())
	rs.bizHandler(ctx, conn)
	log.Printf("bizHandler success process socket fd: %d, remote addr: %v, local addr: %v", conn.fd, conn.RemoteAddr(), conn.LocalAddr())
	rs.mutex.Lock()
	select {
	case <-rs.stop:
		rs.mutex.Unlock()
		log.Printf("bizHandler ready to close connFd %d", conn.fd)
		if e := conn.Close(); e != nil {
			log.Printf("bizHandler close connection failed. err: %v", e)
			return
		}
	default:
		log.Printf("bizHandler ready to send back socket fd: %d, remote addr: %v, local addr: %v", conn.fd, conn.RemoteAddr(), conn.LocalAddr())
		rs.dp.input() <- conn // reuse long connection
		log.Printf("bizHandler success send back socket fd: %d, remote addr: %v, local addr: %v", conn.fd, conn.RemoteAddr(), conn.LocalAddr())
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
