package server

import (
	"github.com/Trinoooo/eggie_kv/errs"
	"github.com/Trinoooo/eggie_kv/storage/server/connections"
	"github.com/Trinoooo/eggie_kv/storage/server/poller"
	"github.com/Trinoooo/eggie_kv/storage/server/protocol"
	"github.com/bytedance/gopkg/util/gopool"
	"github.com/pkg/errors"
	"log"
	"math/rand"
	"sync"
	"syscall"
	"time"
)

type ReactorServer struct {
	mutex           sync.Mutex
	serverTransport connections.IListener
	pool            gopool.Pool
	dp              *dispatcher
	reactors        map[int64]*reactor
	stop            chan struct{}
	done            sync.WaitGroup
	metricsHelper   *MetricsHelper
}

type reactor struct {
	srv        *ReactorServer
	id         int64
	connects   chan connections.IConnection
	processors sync.Map // to avoid using Kevent.Udata
	w          *waiter
	p          poller.Poller
}

func newReactor(id int64, srv *ReactorServer, p poller.Poller) *reactor {
	r := &reactor{
		srv:      srv,
		id:       id,
		connects: make(chan connections.IConnection, reactorInputBufferSize),
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

			log.Printf("reactor #%d ready to register event, remote addr: %v, local addr: %v, fd: %v", r.id, conn.RemoteAddr(), conn.LocalAddr(), conn.RawFd())

			processor := NewProcessor(r.srv, protocol.NewBinaryProtocol(conn), protocol.NewBinaryProtocol(conn))
			changes := []poller.Pevent{{
				ConnFd:    uint64(conn.RawFd()),
				Operation: syscall.EVFILT_READ,
				Flag:      syscall.EV_ADD | syscall.EV_ENABLE,
			}, {
				ConnFd:    uint64(conn.RawFd()),
				Operation: syscall.EVFILT_WRITE,
				Flag:      syscall.EV_ADD | syscall.EV_ENABLE,
			}}
			// register event to poller
			if err := r.p.Register(changes); err != nil && !errors.Is(err, syscall.EINTR) {
				e := conn.Close()
				if e != nil {
					err = errors.Wrap(err, e.Error())
				}
				log.Println(err, changes, conn, conn.RemoteAddr(), conn.LocalAddr())
				continue
			}
			r.processors.Store(conn.RawFd(), processor)
			log.Printf("reactor #%d register event success, evt remote addr: %v, local addr: %v, fd: %v", r.id, conn.RemoteAddr(), conn.LocalAddr(), conn.RawFd())
		case evt, ok := <-r.w.output():
			if !ok {
				log.Printf("reactor #%d stop", r.id)
				return
			}
			p, _ := r.processors.Load(int(evt.ConnFd))
			processor := p.(*Processor)
			conn := processor.GetInputProtocol().GetConnection()
			log.Printf("reactor #%d handle event. remote addr: %v, local addr: %v, fd: %v", r.id, conn.RemoteAddr(), conn.LocalAddr(), conn.RawFd())
			// in this case, close is already called.
			if r.srv.pool == nil {
				continue
			}

			err := processor.Process()
			if errs.GetCode(err) == errs.TaskNotFinishErrCode {
				// do nothing, wait for next event trigger
			} else if err != nil {
				e := conn.Close()
				if e != nil {
					err = errors.Wrap(err, e.Error())
				}
				log.Println(err, conn, conn.RemoteAddr(), conn.LocalAddr())
				continue
			}
		}
	}
}

func (r *reactor) input() chan<- connections.IConnection {
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
	evts := make([]poller.Pevent, 100)
	for {
		log.Printf("waiter #%d ready to wait event trigger", w.parent.id)

		// wait for event to be trigger
		n, err := w.p.Wait(evts)
		if err != nil && !errors.Is(err, syscall.EINTR) { // bugfix: ignore EINTR
			log.Printf("waiter #%d stop, err: %v", w.parent.id, err)
			close(w.events)
			return
		}

		log.Printf("waiter #%d event trigger success, evts: %#v, n: %d", w.parent.id, evts, n)

		for i := 0; i < n; i++ {
			evt := evts[i]
			p, _ := w.parent.processors.Load(int(evt.ConnFd))
			processor := p.(*Processor)
			conn := processor.GetInputProtocol().GetConnection()
			switch {
			case evt.Flag&syscall.EV_EOF != 0:
				log.Printf("waiter #%d meet eof, remote addr: %v, local addr: %v, fd: %v", w.parent.id, conn.RemoteAddr(), conn.LocalAddr(), conn.RawFd())
				log.Printf("waiter #%d close server connection, err: %v", w.parent.id, conn.Close())
			default:
				log.Printf("waiter #%d ready to send evt to reactor, evt remote addr: %v, local addr: %v, fd: %v", w.parent.id, conn.RemoteAddr(), conn.LocalAddr(), conn.RawFd())
				w.events <- evt
				log.Printf("waiter #%d sent evt to reactor success, evt remote addr: %v, local addr: %v, fd: %v", w.parent.id, conn.RemoteAddr(), conn.LocalAddr(), conn.RawFd())
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

	dispatcherInputBufferSize = 1 << 10
	reactorInputBufferSize    = 10
	waiterOutputBufferSize    = 10
)

func NewReactorServer(addr [4]byte, port int) (*ReactorServer, error) {
	var err error
	srv := &ReactorServer{
		reactors:      make(map[int64]*reactor),
		pool:          gopool.NewPool("handlers", pollCapacity, gopool.NewConfig()),
		stop:          make(chan struct{}),
		metricsHelper: NewMetricsHelper(),
	}

	// init dispatcher
	srv.dp = &dispatcher{
		connections: make(chan connections.IConnection, dispatcherInputBufferSize),
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
	srv.serverTransport, err = connections.Listen(addr, port)
	if err != nil {
		return nil, err
	}
	return srv, nil
}

type dispatcher struct {
	connections chan connections.IConnection
	parent      *ReactorServer
}

func (dp *dispatcher) run() {
	defer dp.parent.done.Done()
	log.Println("dispatcher start")

	for conn := range dp.connections {
		log.Printf("dispatcher receive connection, remote addr: %v, local addr: %v, fd: %v", conn.RemoteAddr(), conn.LocalAddr(), conn.RawFd())
		// random load balance
		id := rand.Int63n(numReactor)
		if reactor, exist := dp.parent.reactors[id]; exist {
			reactor.input() <- conn
			log.Printf("dispatcher send connection to reactor #%v success, remote addr: %v, local addr: %v, fd: %v", id, conn.RemoteAddr(), conn.LocalAddr(), conn.RawFd())
		} else {
			log.Printf("dispatcher find reactor #%v not exist", id)
		}
	}

	log.Println("dispatcher stop")
}

func (dp *dispatcher) input() chan<- connections.IConnection {
	return dp.connections
}

func (dp *dispatcher) close() {
	// lock free
	c := dp.connections
	dp.connections = nil
	close(c)

	for _, reactor := range dp.parent.reactors {
		if reactor != nil {
			close(reactor.input())
		}
	}
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
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				// pass
				continue
			} else if errors.Is(err, syscall.ECONNABORTED) {
				log.Printf("software caused connection abort, maybe a darwin/ios bug, ignore")
				continue
			} else if errors.Is(err, syscall.EBADF) {
				log.Printf("Close called. exit gracefully")
				return nil
			} else {
				log.Printf("error occur when accept connection, err: %v", err)
				rs.mutex.Lock()
				rs.dp.close()
				if e := rs.close(); e != nil {
					e = errors.Wrap(err, e.Error())
				}
				rs.done.Wait()
				rs.clearState()
				rs.mutex.Unlock()
				return err
			}
		}
		log.Printf("acceptor accept connection, remote addr: %v, local addr: %v, fd: %v", conn.RemoteAddr(), conn.LocalAddr(), conn.RawFd())
		rs.metricsHelper.ConnectionAcceptCounter.Inc()
		log.Printf("acceptor ready to send connection, remote addr: %v, local addr: %v, fd: %v", conn.RemoteAddr(), conn.LocalAddr(), conn.RawFd())
		rs.dp.input() <- conn
		log.Printf("acceptor success send connection, remote addr: %v, local addr: %v, fd: %v", conn.RemoteAddr(), conn.LocalAddr(), conn.RawFd())
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
	rs.reactors = nil
	rs.dp = nil
	rs.metricsHelper = nil
}
