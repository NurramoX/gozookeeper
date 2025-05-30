// Package zk is a native Go client library for the ZooKeeper orchestration service.
package zk

/*
TODO:
* make sure a ping response comes back in a reasonable time

Possible watcher events:
* Event{Type: EventNotWatching, State: StateDisconnected, Path: Path, Err: err}
*/

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrNoServer indicates that an operation cannot be completed
// because attempts to connect to all servers in the list failed.
var ErrNoServer = errors.New("zk: could not connect to a server")

// ErrInvalidPath indicates that an operation was being attempted on
// an invalid Path. (e.g. empty Path).
var ErrInvalidPath = errors.New("zk: invalid Path")

// DefaultLogger uses the stdlib log package for logging.
var DefaultLogger Logger = defaultLogger{}

const (
	BufferSize      = 1536 * 1024
	EventChanSize   = 6
	SendChanSize    = 16
	ProtectedPrefix = "_c_"
)

type WatchType int

const (
	WatchTypeData WatchType = iota
	WatchTypeExist
	WatchTypeChild
)

type WatchPathType struct {
	Path  string
	WType WatchType
}

// Dialer is a function to be used to establish a connection to a single host.
type Dialer func(network, address string, timeout time.Duration) (net.Conn, error)

// Logger is an interface that can be implemented to provide custom log output.
type Logger interface {
	Printf(string, ...interface{})
}

type authCreds struct {
	scheme string
	auth   []byte
}

// Conn is the client connection and tracks all details for communication with the server.
type Conn struct {
	sessionID int64

	lastZxid         int64
	state            State // must be 32-bit aligned
	xid              uint32
	sessionTimeoutMs int32 // session timeout in milliseconds
	passwd           []byte

	dialer         Dialer
	hostProvider   HostProvider
	serverMu       sync.Mutex // protects server
	server         string     // remember the address/port of the current server
	conn           net.Conn
	eventChan      chan Event
	eventCallback  EventCallback // may be nil
	ShouldQuit     chan struct{}
	shouldQuitOnce sync.Once
	pingInterval   time.Duration
	recvTimeout    time.Duration
	ConnectTimeout time.Duration
	maxBufferSize  int

	creds   []authCreds
	credsMu sync.Mutex // protects server

	SendChan     chan *Request
	requests     map[int32]*Request // Xid -> pending request
	requestsLock sync.Mutex
	Watchers     map[WatchPathType][]chan Event
	watchersLock sync.Mutex
	closeChan    chan struct{} // channel to tell send loop stop

	// Debug (used by unit tests)
	reconnectLatch   chan struct{}
	setWatchLimit    int
	setWatchCallback func([]*SetWatchesRequest)

	// Debug (for recurring re-Auth hang)
	debugCloseRecvLoop bool
	resendZkAuthFn     func(context.Context, *Conn) error

	Logger  Logger
	logInfo bool // true if information messages are logged; false if only errors are logged

	buf []byte
}

func (c *Conn) ReconnectLatch() chan struct{} {
	return c.reconnectLatch
}

// SetResendZkAuthFn only for testing!
func (c *Conn) SetResendZkAuthFn(fn func(context.Context, *Conn) error) {
	// Note: Consider if locking is needed here based on how Conn is used concurrently.
	// If resendZkAuthFn can be called by one goroutine while being set by another,
	// you might need a mutex to protect this assignment. Often, such functions
	// are set once during setup or testing, making locking less critical.
	c.resendZkAuthFn = fn
}

func (c *Conn) SetSessionId(sessionId int64) {
	c.sessionID = sessionId
}

// SetDebugCloseRecvLoop only for testing!
func (c *Conn) SetDebugCloseRecvLoop(debug bool) {
	// Note: Similar to above, consider if locking (e.g., using atomic operations
	// or a mutex) is needed if this boolean Flag is read and written by different
	// goroutines simultaneously. For simple debug flags toggled in tests, direct
	// assignment is often sufficient.
	c.debugCloseRecvLoop = debug
}

func (c *Conn) SetReconnectLatch(reconnectLatch chan struct{}) {
	c.reconnectLatch = reconnectLatch
}
func (c *Conn) SetWatchLimit(setWatchLimit int) {
	c.setWatchLimit = setWatchLimit
}
func (c *Conn) SetWatchCallback(setWatchCallback func([]*SetWatchesRequest)) {
	c.setWatchCallback = setWatchCallback
}

// ConnOption represents a connection option.
type ConnOption func(c *Conn)

type Request struct {
	xid        int32
	opcode     int32
	pkt        interface{}
	recvStruct interface{}
	recvChan   chan response

	// Because sending and receiving happen in separate go routines, there's
	// a possible race condition when creating watches from outside the read
	// loop. We must ensure that a watcher gets added to the list synchronously
	// with the response from the server on any Request that creates a watch.
	// In order to not hard code the watch logic for each opcode in the recv
	// loop the caller can use recvFunc to insert some synchronously code
	// after a response.
	recvFunc func(*Request, *ResponseHeader, error)
}

type response struct {
	zxid int64
	err  error
}

// Event is an Znode event sent by the server.
// Refer to EventType for more details.
type Event struct {
	Type   EventType
	State  State
	Path   string // For non-session events, the Path of the watched node.
	Err    error
	Server string // For connection events
}

// HostProvider is used to represent a set of hosts a ZooKeeper client should connect to.
// It is an analog of the Java equivalent:
// http://svn.apache.org/viewvc/zookeeper/trunk/src/java/main/org/apache/zookeeper/client/HostProvider.java?view=markup
type HostProvider interface {
	// Init is called first, with the servers specified in the connection string.
	Init(servers []string) error
	// Len returns the number of servers.
	Len() int
	// Next returns the next server to connect to. retryStart will be true if we've looped through
	// all known servers without Connected() being called.
	Next() (server string, retryStart bool)
	// Notify the HostProvider of a successful connection.
	Connected()
}

// ConnectWithDialer establishes a new connection to a pool of zookeeper servers
// using a custom Dialer. See Connect for further information about session timeout.
// This method is deprecated and provided for compatibility: use the WithDialer option instead.
func ConnectWithDialer(servers []string, sessionTimeout time.Duration, dialer Dialer) (*Conn, <-chan Event, error) {
	return Connect(servers, sessionTimeout, WithDialer(dialer))
}

// Connect establishes a new connection to a pool of zookeeper
// servers. The provided session timeout sets the amount of time for which
// a session is considered valid after losing connection to a server. Within
// the session timeout it's possible to reestablish a connection to a different
// server and keep the same session. This is means any ephemeral nodes and
// watches are maintained.
func Connect(servers []string, sessionTimeout time.Duration, options ...ConnOption) (*Conn, <-chan Event, error) {
	if len(servers) == 0 {
		return nil, nil, errors.New("zk: server list must not be empty")
	}

	srvs := FormatServers(servers)

	// Randomize the order of the servers to avoid creating hotspots
	stringShuffle(srvs)

	ec := make(chan Event, EventChanSize)
	conn := &Conn{
		dialer:         net.DialTimeout,
		hostProvider:   NewDNSHostProvider(),
		conn:           nil,
		state:          StateDisconnected,
		eventChan:      ec,
		ShouldQuit:     make(chan struct{}),
		ConnectTimeout: 1 * time.Second,
		SendChan:       make(chan *Request, SendChanSize),
		requests:       make(map[int32]*Request),
		Watchers:       make(map[WatchPathType][]chan Event),
		passwd:         emptyPassword,
		Logger:         DefaultLogger,
		logInfo:        true, // default is true for backwards compatability
		buf:            make([]byte, BufferSize),
		resendZkAuthFn: ResendZkAuth,
	}

	// Set provided options.
	for _, option := range options {
		option(conn)
	}

	if err := conn.hostProvider.Init(srvs); err != nil {
		return nil, nil, err
	}

	conn.setTimeouts(int32(sessionTimeout / time.Millisecond))
	// TODO: This context should be passed in by the caller to be the connection lifecycle context.
	ctx := context.Background()

	go func() {
		conn.loop(ctx)
		conn.flushRequests(ErrClosing)
		conn.invalidateWatches(ErrClosing)
		close(conn.eventChan)
	}()
	return conn, ec, nil
}

// WithDialer returns a connection option specifying a non-default Dialer.
func WithDialer(dialer Dialer) ConnOption {
	return func(c *Conn) {
		c.dialer = dialer
	}
}

// WithHostProvider returns a connection option specifying a non-default HostProvider.
func WithHostProvider(hostProvider HostProvider) ConnOption {
	return func(c *Conn) {
		c.hostProvider = hostProvider
	}
}

// WithLogger returns a connection option specifying a non-default Logger.
func WithLogger(logger Logger) ConnOption {
	return func(c *Conn) {
		c.Logger = logger
	}
}

// WithLogInfo returns a connection option specifying whether or not information messages
// should be logged.
func WithLogInfo(logInfo bool) ConnOption {
	return func(c *Conn) {
		c.logInfo = logInfo
	}
}

// EventCallback is a function that is called when an Event occurs.
type EventCallback func(Event)

// WithEventCallback returns a connection option that specifies an event
// callback.
// The callback must not block - doing so would delay the ZK go routines.
func WithEventCallback(cb EventCallback) ConnOption {
	return func(c *Conn) {
		c.eventCallback = cb
	}
}

// WithMaxBufferSize sets the maximum buffer size used to read and decode
// packets received from the Zookeeper server. The standard Zookeeper client for
// Java defaults to a limit of 1mb. For backwards compatibility, this Go client
// defaults to unbounded unless overridden via this option. A value that is zero
// or negative indicates that no limit is enforced.
//
// This is meant to prevent resource exhaustion in the face of potentially
// malicious data in ZK. It should generally match the server setting (which
// also defaults ot 1mb) so that clients and servers agree on the limits for
// things like the size of data in an individual znode and the total size of a
// transaction.
//
// For production systems, this should be set to a reasonable value (ideally
// that matches the server configuration). For ops tooling, it is handy to use a
// much larger limit, in order to do things like clean-up problematic state in
// the ZK tree. For example, if a single znode has a huge number of children, it
// is possible for the response to a "list children" operation to exceed this
// buffer size and cause errors in clients. The only way to subsequently clean
// up the tree (by removing superfluous children) is to use a client configured
// with a larger buffer size that can successfully query for all of the child
// names and then remove them. (Note there are other tools that can list all of
// the child names without an increased buffer size in the client, but they work
// by inspecting the servers' transaction logs to enumerate children instead of
// sending an online Request to a server.
func WithMaxBufferSize(maxBufferSize int) ConnOption {
	return func(c *Conn) {
		c.maxBufferSize = maxBufferSize
	}
}

// WithMaxConnBufferSize sets maximum buffer size used to send and encode
// packets to Zookeeper server. The standard Zookeeper client for java defaults
// to a limit of 1mb. This option should be used for non-standard server setup
// where znode is bigger than default 1mb.
func WithMaxConnBufferSize(maxBufferSize int) ConnOption {
	return func(c *Conn) {
		c.buf = make([]byte, maxBufferSize)
	}
}

// Close will submit a close Request with ZK and signal the connection to stop
// sending and receiving packets.
func (c *Conn) Close() {
	c.shouldQuitOnce.Do(func() {
		close(c.ShouldQuit)

		select {
		case <-c.queueRequest(OpClose, &closeRequest{}, &closeResponse{}, nil):
		case <-time.After(time.Second):
		}
	})
}

// State returns the current state of the connection.
func (c *Conn) State() State {
	return State(atomic.LoadInt32((*int32)(&c.state)))
}

func (c *Conn) SessionID() int64 {
	return atomic.LoadInt64(&c.sessionID)
}

func (c *Conn) EventChan() chan Event {
	return c.eventChan
}

func (c *Conn) SessionTimeoutMs() int32 {
	return c.sessionTimeoutMs
}

func (c *Conn) Conn() net.Conn {
	return c.conn
}

// SetLogger sets the Logger to be used for printing errors.
// Logger is an interface provided by this package.
func (c *Conn) SetLogger(l Logger) {
	c.Logger = l
}

func (c *Conn) setTimeouts(sessionTimeoutMs int32) {
	c.sessionTimeoutMs = sessionTimeoutMs
	sessionTimeout := time.Duration(sessionTimeoutMs) * time.Millisecond
	c.recvTimeout = sessionTimeout * 2 / 3
	c.pingInterval = c.recvTimeout / 2
}

func (c *Conn) setState(state State) {
	atomic.StoreInt32((*int32)(&c.state), int32(state))
	c.sendEvent(Event{Type: EventSession, State: state, Server: c.Server()})
}

func (c *Conn) sendEvent(evt Event) {
	if c.eventCallback != nil {
		c.eventCallback(evt)
	}

	select {
	case c.eventChan <- evt:
	default:
		// panic("zk: event channel full - it must be monitored and never allowed to be full")
	}
}

func (c *Conn) connect() error {
	var retryStart bool
	for {
		c.serverMu.Lock()
		c.server, retryStart = c.hostProvider.Next()
		c.serverMu.Unlock()

		c.setState(StateConnecting)

		if retryStart {
			c.flushUnsentRequests(ErrNoServer)
			select {
			case <-time.After(time.Second):
				// pass
			case <-c.ShouldQuit:
				c.setState(StateDisconnected)
				c.flushUnsentRequests(ErrClosing)
				return ErrClosing
			}
		}

		zkConn, err := c.dialer("tcp", c.Server(), c.ConnectTimeout)
		if err == nil {
			c.conn = zkConn
			c.setState(StateConnected)
			if c.logInfo {
				c.Logger.Printf("connected to %s", c.Server())
			}
			return nil
		}

		c.Logger.Printf("failed to connect to %s: %v", c.Server(), err)
	}
}

func (c *Conn) sendRequest(
	opcode int32,
	req interface{},
	res interface{},
	recvFunc func(*Request, *ResponseHeader, error),
) (
	<-chan response,
	error,
) {
	rq := &Request{
		xid:        c.nextXid(),
		opcode:     opcode,
		pkt:        req,
		recvStruct: res,
		recvChan:   make(chan response, 1),
		recvFunc:   recvFunc,
	}

	if err := c.sendData(rq); err != nil {
		return nil, err
	}

	return rq.recvChan, nil
}

func (c *Conn) loop(ctx context.Context) {
	for {
		if err := c.connect(); err != nil {
			// c.Close() was called
			return
		}

		err := c.authenticate()
		switch {
		case err == ErrSessionExpired:
			c.Logger.Printf("authentication failed: %s", err)
			c.invalidateWatches(err)
		case err != nil && c.conn != nil:
			c.Logger.Printf("authentication failed: %s", err)
			c.conn.Close()
		case err == nil:
			if c.logInfo {
				c.Logger.Printf("authenticated: id=%d, timeout=%d", c.SessionID(), c.sessionTimeoutMs)
			}
			c.hostProvider.Connected()        // mark success
			c.closeChan = make(chan struct{}) // channel to tell send loop stop

			var wg sync.WaitGroup

			wg.Add(1)
			go func() {
				defer c.conn.Close() // causes recv loop to EOF/exit
				defer wg.Done()

				if err := c.resendZkAuthFn(ctx, c); err != nil {
					c.Logger.Printf("error in resending Auth creds: %v", err)
					return
				}

				if err := c.sendLoop(); err != nil || c.logInfo {
					c.Logger.Printf("send loop terminated: %v", err)
				}
			}()

			wg.Add(1)
			go func() {
				defer close(c.closeChan) // tell send loop to exit
				defer wg.Done()

				var err error
				if c.debugCloseRecvLoop {
					err = errors.New("DEBUG: close recv loop")
				} else {
					err = c.recvLoop(c.conn)
				}
				if err != io.EOF || c.logInfo {
					c.Logger.Printf("recv loop terminated: %v", err)
				}
				if err == nil {
					panic("zk: recvLoop should never return nil error")
				}
			}()

			c.sendSetWatches()
			wg.Wait()
		}

		c.setState(StateDisconnected)

		select {
		case <-c.ShouldQuit:
			c.flushRequests(ErrClosing)
			return
		default:
		}

		if err != ErrSessionExpired {
			err = ErrConnectionClosed
		}
		c.flushRequests(err)

		if c.reconnectLatch != nil {
			select {
			case <-c.ShouldQuit:
				return
			case <-c.reconnectLatch:
			}
		}
	}
}

func (c *Conn) flushUnsentRequests(err error) {
	for {
		select {
		default:
			return
		case req := <-c.SendChan:
			req.recvChan <- response{-1, err}
		}
	}
}

// Send error to all pending requests and clear Request map
func (c *Conn) flushRequests(err error) {
	c.requestsLock.Lock()
	for _, req := range c.requests {
		req.recvChan <- response{-1, err}
	}
	c.requests = make(map[int32]*Request)
	c.requestsLock.Unlock()
}

// Send event to all interested Watchers
func (c *Conn) NotifyWatches(ev Event) {
	var wTypes []WatchType
	switch ev.Type {
	case EventNodeCreated:
		wTypes = []WatchType{WatchTypeExist}
	case EventNodeDataChanged:
		wTypes = []WatchType{WatchTypeExist, WatchTypeData}
	case EventNodeChildrenChanged:
		wTypes = []WatchType{WatchTypeChild}
	case EventNodeDeleted:
		wTypes = []WatchType{WatchTypeExist, WatchTypeData, WatchTypeChild}
	}
	c.watchersLock.Lock()
	defer c.watchersLock.Unlock()
	for _, t := range wTypes {
		wpt := WatchPathType{ev.Path, t}
		if watchers := c.Watchers[wpt]; len(watchers) > 0 {
			for _, ch := range watchers {
				ch <- ev
				close(ch)
			}
			delete(c.Watchers, wpt)
		}
	}
}

// Send error to all Watchers and clear Watchers map
func (c *Conn) invalidateWatches(err error) {
	c.watchersLock.Lock()
	defer c.watchersLock.Unlock()

	if len(c.Watchers) >= 0 {
		for pathType, watchers := range c.Watchers {
			ev := Event{Type: EventNotWatching, State: StateDisconnected, Path: pathType.Path, Err: err}
			c.sendEvent(ev) // also publish globally
			for _, ch := range watchers {
				ch <- ev
				close(ch)
			}
		}
		c.Watchers = make(map[WatchPathType][]chan Event)
	}
}

func (c *Conn) sendSetWatches() {
	c.watchersLock.Lock()
	defer c.watchersLock.Unlock()

	if len(c.Watchers) == 0 {
		return
	}

	// NB: A ZK server, by default, rejects packets >1mb. So, if we have too
	// many watches to reset, we need to break this up into multiple packets
	// to avoid hitting that limit. Mirroring the Java client behavior: we are
	// conservative in that we limit requests to 128kb (since server limit is
	// is actually configurable and could conceivably be configured smaller
	// than default of 1mb).
	limit := 128 * 1024
	if c.setWatchLimit > 0 {
		limit = c.setWatchLimit
	}

	var reqs []*SetWatchesRequest
	var req *SetWatchesRequest
	var sizeSoFar int

	n := 0
	for pathType, watchers := range c.Watchers {
		if len(watchers) == 0 {
			continue
		}
		addlLen := 4 + len(pathType.Path)
		if req == nil || sizeSoFar+addlLen > limit {
			if req != nil {
				// add to set of requests that we'll send
				reqs = append(reqs, req)
			}
			sizeSoFar = 28 // fixed overhead of a set-watches packet
			req = &SetWatchesRequest{
				RelativeZxid: c.lastZxid,
				DataWatches:  make([]string, 0),
				ExistWatches: make([]string, 0),
				ChildWatches: make([]string, 0),
			}
		}
		sizeSoFar += addlLen
		switch pathType.WType {
		case WatchTypeData:
			req.DataWatches = append(req.DataWatches, pathType.Path)
		case WatchTypeExist:
			req.ExistWatches = append(req.ExistWatches, pathType.Path)
		case WatchTypeChild:
			req.ChildWatches = append(req.ChildWatches, pathType.Path)
		}
		n++
	}
	if n == 0 {
		return
	}
	if req != nil { // don't forget any trailing packet we were building
		reqs = append(reqs, req)
	}

	if c.setWatchCallback != nil {
		c.setWatchCallback(reqs)
	}

	go func() {
		res := &setWatchesResponse{}
		// TODO: Pipeline these so queue all of them up before waiting on any
		// response. That will require some investigation to make sure there
		// aren't failure modes where a blocking write to the channel of requests
		// could hang indefinitely and cause this goroutine to leak...
		for _, req := range reqs {
			_, err := c.request(OpSetWatches, req, res, nil)
			if err != nil {
				c.Logger.Printf("Failed to set previous watches: %v", err)
				break
			}
		}
	}()
}

func (c *Conn) authenticate() error {
	buf := make([]byte, 256)

	// Encode and send a connect Request.
	n, err := EncodePacket(buf[4:], &ConnectRequest{
		ProtocolVersion: protocolVersion,
		LastZxidSeen:    c.lastZxid,
		TimeOut:         c.sessionTimeoutMs,
		SessionID:       c.SessionID(),
		Passwd:          c.passwd,
	})
	if err != nil {
		return err
	}

	binary.BigEndian.PutUint32(buf[:4], uint32(n))

	c.conn.SetWriteDeadline(time.Now().Add(c.recvTimeout * 10))
	_, err = c.conn.Write(buf[:n+4])
	c.conn.SetWriteDeadline(time.Time{})
	if err != nil {
		return err
	}

	// Receive and decode a connect response.
	c.conn.SetReadDeadline(time.Now().Add(c.recvTimeout * 10))
	_, err = io.ReadFull(c.conn, buf[:4])
	c.conn.SetReadDeadline(time.Time{})
	if err != nil {
		return err
	}

	blen := int(binary.BigEndian.Uint32(buf[:4]))
	if cap(buf) < blen {
		buf = make([]byte, blen)
	}

	_, err = io.ReadFull(c.conn, buf[:blen])
	if err != nil {
		return err
	}

	r := ConnectResponse{}
	_, err = DecodePacket(buf[:blen], &r)
	if err != nil {
		return err
	}
	if r.SessionID == 0 {
		atomic.StoreInt64(&c.sessionID, int64(0))
		c.passwd = emptyPassword
		c.lastZxid = 0
		c.setState(StateExpired)
		return ErrSessionExpired
	}

	atomic.StoreInt64(&c.sessionID, r.SessionID)
	c.setTimeouts(r.TimeOut)
	c.passwd = r.Passwd
	c.setState(StateHasSession)

	return nil
}

func (c *Conn) sendData(req *Request) error {
	header := &RequestHeader{req.xid, req.opcode}
	n, err := EncodePacket(c.buf[4:], header)
	if err != nil {
		req.recvChan <- response{-1, err}
		return nil
	}

	n2, err := EncodePacket(c.buf[4+n:], req.pkt)
	if err != nil {
		req.recvChan <- response{-1, err}
		return nil
	}

	n += n2

	binary.BigEndian.PutUint32(c.buf[:4], uint32(n))

	c.requestsLock.Lock()
	select {
	case <-c.closeChan:
		req.recvChan <- response{-1, ErrConnectionClosed}
		c.requestsLock.Unlock()
		return ErrConnectionClosed
	default:
	}
	c.requests[req.xid] = req
	c.requestsLock.Unlock()

	c.conn.SetWriteDeadline(time.Now().Add(c.recvTimeout))
	_, err = c.conn.Write(c.buf[:n+4])
	c.conn.SetWriteDeadline(time.Time{})
	if err != nil {
		req.recvChan <- response{-1, err}
		c.conn.Close()
		return err
	}

	return nil
}

func (c *Conn) sendLoop() error {
	pingTicker := time.NewTicker(c.pingInterval)
	defer pingTicker.Stop()

	for {
		select {
		case req := <-c.SendChan:
			if err := c.sendData(req); err != nil {
				return err
			}
		case <-pingTicker.C:
			n, err := EncodePacket(c.buf[4:], &RequestHeader{Xid: -2, Opcode: OpPing})
			if err != nil {
				panic("zk: OpPing should never fail to serialize")
			}

			binary.BigEndian.PutUint32(c.buf[:4], uint32(n))

			c.conn.SetWriteDeadline(time.Now().Add(c.recvTimeout))
			_, err = c.conn.Write(c.buf[:n+4])
			c.conn.SetWriteDeadline(time.Time{})
			if err != nil {
				c.conn.Close()
				return err
			}
		case <-c.closeChan:
			return nil
		}
	}
}

func (c *Conn) recvLoop(conn net.Conn) error {
	sz := BufferSize
	if c.maxBufferSize > 0 && sz > c.maxBufferSize {
		sz = c.maxBufferSize
	}
	buf := make([]byte, sz)
	for {
		// package length
		if err := conn.SetReadDeadline(time.Now().Add(c.recvTimeout)); err != nil {
			c.Logger.Printf("failed to set connection deadline: %v", err)
		}
		_, err := io.ReadFull(conn, buf[:4])
		if err != nil {
			return fmt.Errorf("failed to read from connection: %v", err)
		}

		blen := int(binary.BigEndian.Uint32(buf[:4]))
		if cap(buf) < blen {
			if c.maxBufferSize > 0 && blen > c.maxBufferSize {
				return fmt.Errorf("received packet from server with length %d, which exceeds max buffer size %d", blen, c.maxBufferSize)
			}
			buf = make([]byte, blen)
		}

		_, err = io.ReadFull(conn, buf[:blen])
		conn.SetReadDeadline(time.Time{})
		if err != nil {
			return err
		}

		res := ResponseHeader{}
		_, err = DecodePacket(buf[:16], &res)
		if err != nil {
			return err
		}

		if res.Xid == -1 {
			res := &watcherEvent{}
			_, err = DecodePacket(buf[16:blen], res)
			if err != nil {
				return err
			}
			ev := Event{
				Type:  res.Type,
				State: res.State,
				Path:  res.Path,
				Err:   nil,
			}
			c.sendEvent(ev)
			c.NotifyWatches(ev)
		} else if res.Xid == -2 {
			// Ping response. Ignore.
		} else if res.Xid < 0 {
			c.Logger.Printf("Xid < 0 (%d) but not ping or watcher event", res.Xid)
		} else {
			if res.Zxid > 0 {
				c.lastZxid = res.Zxid
			}

			c.requestsLock.Lock()
			req, ok := c.requests[res.Xid]
			if ok {
				delete(c.requests, res.Xid)
			}
			c.requestsLock.Unlock()

			if !ok {
				c.Logger.Printf("Response for unknown Request with xid %d", res.Xid)
			} else {
				if res.Err != 0 {
					err = res.Err.toError()
				} else {
					_, err = DecodePacket(buf[16:blen], req.recvStruct)
				}
				if req.recvFunc != nil {
					req.recvFunc(req, &res, err)
				}
				req.recvChan <- response{res.Zxid, err}
				if req.opcode == OpClose {
					return io.EOF
				}
			}
		}
	}
}

func (c *Conn) nextXid() int32 {
	return int32(atomic.AddUint32(&c.xid, 1) & 0x7fffffff)
}

func (c *Conn) AddWatcher(path string, watchType WatchType) <-chan Event {
	c.watchersLock.Lock()
	defer c.watchersLock.Unlock()

	ch := make(chan Event, 1)
	wpt := WatchPathType{path, watchType}
	c.Watchers[wpt] = append(c.Watchers[wpt], ch)
	return ch
}

func (c *Conn) queueRequest(opcode int32, req interface{}, res interface{}, recvFunc func(*Request, *ResponseHeader, error)) <-chan response {
	rq := &Request{
		xid:        c.nextXid(),
		opcode:     opcode,
		pkt:        req,
		recvStruct: res,
		recvChan:   make(chan response, 2),
		recvFunc:   recvFunc,
	}

	switch opcode {
	case OpClose:
		// always attempt to send close ops.
		select {
		case c.SendChan <- rq:
		case <-time.After(c.ConnectTimeout * 2):
			c.Logger.Printf("gave up trying to send OpClose to server")
			rq.recvChan <- response{-1, ErrConnectionClosed}
		}
	default:
		// otherwise avoid deadlocks for dumb clients who aren't aware that
		// the ZK connection is closed yet.
		select {
		case <-c.ShouldQuit:
			rq.recvChan <- response{-1, ErrConnectionClosed}
		case c.SendChan <- rq:
			// check for a tie
			select {
			case <-c.ShouldQuit:
				// maybe the caller gets this, maybe not- we tried.
				rq.recvChan <- response{-1, ErrConnectionClosed}
			default:
			}
		}
	}
	return rq.recvChan
}

func (c *Conn) request(opcode int32, req interface{}, res interface{}, recvFunc func(*Request, *ResponseHeader, error)) (int64, error) {
	recv := c.queueRequest(opcode, req, res, recvFunc)
	select {
	case r := <-recv:
		return r.zxid, r.err
	case <-c.ShouldQuit:
		// queueRequest() can be racy, double-check for the race here and avoid
		// a potential data-race. otherwise the client of this func may try to
		// access `res` fields concurrently w/ the async response processor.
		// NOTE: callers of this func should check for (at least) ErrConnectionClosed
		// and avoid accessing fields of the response object if such error is present.
		return -1, ErrConnectionClosed
	}
}

// AddAuth adds an authentication config to the connection.
func (c *Conn) AddAuth(scheme string, auth []byte) error {
	_, err := c.request(OpSetAuth, &setAuthRequest{Type: 0, Scheme: scheme, Auth: auth}, &setAuthResponse{}, nil)

	if err != nil {
		return err
	}

	// Remember authdata so that it can be re-submitted on reconnect
	//
	// FIXME(prozlach): For now we treat "userfoo:passbar" and "userfoo:passbar2"
	// as two different entries, which will be re-submitted on reconnect. Some
	// research is needed on how ZK treats these cases and
	// then maybe switch to something like "map[username] = password" to allow
	// only single password for given user with users being unique.
	obj := authCreds{
		scheme: scheme,
		auth:   auth,
	}

	c.credsMu.Lock()
	c.creds = append(c.creds, obj)
	c.credsMu.Unlock()

	return nil
}

// Children returns the children of a znode.
func (c *Conn) Children(path string) ([]string, *Stat, error) {
	if err := ValidatePath(path, false); err != nil {
		return nil, nil, err
	}

	res := &getChildren2Response{}
	_, err := c.request(OpGetChildren2, &getChildren2Request{Path: path, Watch: false}, res, nil)
	if err == ErrConnectionClosed {
		return nil, nil, err
	}
	return res.Children, &res.Stat, err
}

// ChildrenW returns the children of a znode and sets a watch.
func (c *Conn) ChildrenW(path string) ([]string, *Stat, <-chan Event, error) {
	if err := ValidatePath(path, false); err != nil {
		return nil, nil, nil, err
	}

	var ech <-chan Event
	res := &getChildren2Response{}
	_, err := c.request(OpGetChildren2, &getChildren2Request{Path: path, Watch: true}, res, func(req *Request, res *ResponseHeader, err error) {
		if err == nil {
			ech = c.AddWatcher(path, WatchTypeChild)
		}
	})
	if err != nil {
		return nil, nil, nil, err
	}
	return res.Children, &res.Stat, ech, err
}

// Get gets the contents of a znode.
func (c *Conn) Get(path string) ([]byte, *Stat, error) {
	if err := ValidatePath(path, false); err != nil {
		return nil, nil, err
	}

	res := &getDataResponse{}
	_, err := c.request(OpGetData, &getDataRequest{Path: path, Watch: false}, res, nil)
	if err == ErrConnectionClosed {
		return nil, nil, err
	}
	return res.Data, &res.Stat, err
}

// GetW returns the contents of a znode and sets a watch
func (c *Conn) GetW(path string) ([]byte, *Stat, <-chan Event, error) {
	if err := ValidatePath(path, false); err != nil {
		return nil, nil, nil, err
	}

	var ech <-chan Event
	res := &getDataResponse{}
	_, err := c.request(OpGetData, &getDataRequest{Path: path, Watch: true}, res, func(req *Request, res *ResponseHeader, err error) {
		if err == nil {
			ech = c.AddWatcher(path, WatchTypeData)
		}
	})
	if err != nil {
		return nil, nil, nil, err
	}
	return res.Data, &res.Stat, ech, err
}

// Set updates the contents of a znode.
func (c *Conn) Set(path string, data []byte, version int32) (*Stat, error) {
	if err := ValidatePath(path, false); err != nil {
		return nil, err
	}

	res := &setDataResponse{}
	_, err := c.request(OpSetData, &SetDataRequest{path, data, version}, res, nil)
	if err == ErrConnectionClosed {
		return nil, err
	}
	return &res.Stat, err
}

// Create creates a znode.
// The returned Path is the new Path assigned by the server, it may not be the
// same as the input, for example when creating a sequence znode the returned Path
// will be the input Path with a sequence number appended.
func (c *Conn) Create(path string, data []byte, flags int32, acl []ACL) (string, error) {
	createMode, err := ParseCreateMode(flags)
	if err != nil {
		return "", err
	}

	if err := ValidatePath(path, createMode.isSequential); err != nil {
		return "", err
	}

	if createMode.isTTL {
		return "", fmt.Errorf("Create with TTL Flag disallowed: %w", ErrInvalidFlags)
	}

	res := &createResponse{}
	_, err = c.request(OpCreate, &CreateRequest{path, data, acl, createMode.Flag}, res, nil)
	if err == ErrConnectionClosed {
		return "", err
	}
	return res.Path, err
}

// CreateContainer creates a container znode and returns the Path.
//
// Containers cannot be ephemeral or sequential, or have TTLs.
// Ensure that we reject flags for TTL, Sequence, and Ephemeral.
func (c *Conn) CreateContainer(path string, data []byte, flag int32, acl []ACL) (string, error) {
	createMode, err := ParseCreateMode(flag)
	if err != nil {
		return "", err
	}

	if err := ValidatePath(path, createMode.isSequential); err != nil {
		return "", err
	}

	if !createMode.isContainer {
		return "", fmt.Errorf("CreateContainer requires container Flag: %w", ErrInvalidFlags)
	}

	res := &createResponse{}
	_, err = c.request(OpCreateContainer, &CreateRequest{path, data, acl, createMode.Flag}, res, nil)
	return res.Path, err
}

// CreateTTL creates a TTL znode, which will be automatically deleted by server after the TTL.
func (c *Conn) CreateTTL(path string, data []byte, flag int32, acl []ACL, ttl time.Duration) (string, error) {
	createMode, err := ParseCreateMode(flag)
	if err != nil {
		return "", err
	}

	if err := ValidatePath(path, createMode.isSequential); err != nil {
		return "", err
	}

	if !createMode.isTTL {
		return "", fmt.Errorf("CreateTTL requires TTL Flag: %w", ErrInvalidFlags)
	}

	res := &createResponse{}
	_, err = c.request(OpCreateTTL, &CreateTTLRequest{path, data, acl, createMode.Flag, ttl.Milliseconds()}, res, nil)
	return res.Path, err
}

// CreateProtectedEphemeralSequential fixes a race condition if the server crashes
// after it creates the node. On reconnect the session may still be valid so the
// ephemeral node still exists. Therefore, on reconnect we need to check if a node
// with a GUID generated on create exists.
func (c *Conn) CreateProtectedEphemeralSequential(path string, data []byte, acl []ACL) (string, error) {
	if err := ValidatePath(path, true); err != nil {
		return "", err
	}

	var guid [16]byte
	_, err := io.ReadFull(rand.Reader, guid[:16])
	if err != nil {
		return "", err
	}
	guidStr := fmt.Sprintf("%x", guid)

	parts := strings.Split(path, "/")
	parts[len(parts)-1] = fmt.Sprintf("%s%s-%s", ProtectedPrefix, guidStr, parts[len(parts)-1])
	rootPath := strings.Join(parts[:len(parts)-1], "/")
	protectedPath := strings.Join(parts, "/")

	var newPath string
	for i := 0; i < 3; i++ {
		newPath, err = c.Create(protectedPath, data, FlagEphemeral|FlagSequence, acl)
		switch err {
		case ErrSessionExpired:
			// No need to search for the node since it can't exist. Just try again.
		case ErrConnectionClosed:
			children, _, err := c.Children(rootPath)
			if err != nil {
				return "", err
			}
			for _, p := range children {
				parts := strings.Split(p, "/")
				if pth := parts[len(parts)-1]; strings.HasPrefix(pth, ProtectedPrefix) {
					if g := pth[len(ProtectedPrefix) : len(ProtectedPrefix)+32]; g == guidStr {
						return rootPath + "/" + p, nil
					}
				}
			}
		case nil:
			return newPath, nil
		default:
			return "", err
		}
	}
	return "", err
}

// Delete deletes a znode.
func (c *Conn) Delete(path string, version int32) error {
	if err := ValidatePath(path, false); err != nil {
		return err
	}

	_, err := c.request(OpDelete, &DeleteRequest{path, version}, &deleteResponse{}, nil)
	return err
}

// Exists tells the existence of a znode.
func (c *Conn) Exists(path string) (bool, *Stat, error) {
	if err := ValidatePath(path, false); err != nil {
		return false, nil, err
	}

	res := &existsResponse{}
	_, err := c.request(OpExists, &existsRequest{Path: path, Watch: false}, res, nil)
	if err == ErrConnectionClosed {
		return false, nil, err
	}
	exists := true
	if err == ErrNoNode {
		exists = false
		err = nil
	}
	return exists, &res.Stat, err
}

// ExistsW tells the existence of a znode and sets a watch.
func (c *Conn) ExistsW(path string) (bool, *Stat, <-chan Event, error) {
	if err := ValidatePath(path, false); err != nil {
		return false, nil, nil, err
	}

	var ech <-chan Event
	res := &existsResponse{}
	_, err := c.request(OpExists, &existsRequest{Path: path, Watch: true}, res, func(req *Request, res *ResponseHeader, err error) {
		if err == nil {
			ech = c.AddWatcher(path, WatchTypeData)
		} else if err == ErrNoNode {
			ech = c.AddWatcher(path, WatchTypeExist)
		}
	})
	exists := true
	if err == ErrNoNode {
		exists = false
		err = nil
	}
	if err != nil {
		return false, nil, nil, err
	}
	return exists, &res.Stat, ech, err
}

// GetACL gets the ACLs of a znode.
func (c *Conn) GetACL(path string) ([]ACL, *Stat, error) {
	if err := ValidatePath(path, false); err != nil {
		return nil, nil, err
	}

	res := &GetAclResponse{}
	_, err := c.request(OpGetAcl, &getAclRequest{Path: path}, res, nil)
	if err == ErrConnectionClosed {
		return nil, nil, err
	}
	return res.Acl, &res.Stat, err
}

// SetACL updates the ACLs of a znode.
func (c *Conn) SetACL(path string, acl []ACL, version int32) (*Stat, error) {
	if err := ValidatePath(path, false); err != nil {
		return nil, err
	}

	res := &setAclResponse{}
	_, err := c.request(OpSetAcl, &setAclRequest{Path: path, Acl: acl, Version: version}, res, nil)
	if err == ErrConnectionClosed {
		return nil, err
	}
	return &res.Stat, err
}

// Sync flushes the channel between process and the leader of a given znode,
// you may need it if you want identical views of ZooKeeper data for 2 client instances.
// Please refer to the "Consistency Guarantees" section of ZK document for more details.
func (c *Conn) Sync(path string) (string, error) {
	if err := ValidatePath(path, false); err != nil {
		return "", err
	}

	res := &syncResponse{}
	_, err := c.request(OpSync, &syncRequest{Path: path}, res, nil)
	if err == ErrConnectionClosed {
		return "", err
	}
	return res.Path, err
}

// MultiResponse is the result of a Multi call.
type MultiResponse struct {
	Stat   *Stat
	String string
	Error  error
}

// Multi executes multiple ZooKeeper operations or none of them. The provided
// ops must be one of *CreateRequest, *DeleteRequest, *SetDataRequest, or
// *CheckVersionRequest.
func (c *Conn) Multi(ops ...interface{}) ([]MultiResponse, error) {
	req := &MultiRequest{
		Ops:        make([]MultiRequestOp, 0, len(ops)),
		DoneHeader: MultiHeader{Type: -1, Done: true, Err: -1},
	}
	for _, op := range ops {
		var opCode int32
		switch op.(type) {
		case *CreateRequest:
			opCode = OpCreate
		case *SetDataRequest:
			opCode = OpSetData
		case *DeleteRequest:
			opCode = OpDelete
		case *CheckVersionRequest:
			opCode = OpCheck
		default:
			return nil, fmt.Errorf("unknown operation type %T", op)
		}
		req.Ops = append(req.Ops, MultiRequestOp{MultiHeader{opCode, false, -1}, op})
	}
	res := &multiResponse{}
	_, err := c.request(OpMulti, req, res, nil)
	if err == ErrConnectionClosed {
		return nil, err
	}
	mr := make([]MultiResponse, len(res.Ops))
	for i, op := range res.Ops {
		mr[i] = MultiResponse{Stat: op.Stat, String: op.String, Error: op.Err.toError()}
	}
	return mr, err
}

// IncrementalReconfig is the zookeeper reconfiguration api that allows adding and removing servers
// by lists of members. For more info refer to the ZK documentation.
//
// An optional version allows for conditional reconfigurations, -1 ignores the condition.
//
// Returns the new configuration znode stat.
func (c *Conn) IncrementalReconfig(joining, leaving []string, version int64) (*Stat, error) {
	// TODO: validate the shape of the member string to give early feedback.
	request := &ReconfigRequest{
		JoiningServers: []byte(strings.Join(joining, ",")),
		LeavingServers: []byte(strings.Join(leaving, ",")),
		CurConfigId:    version,
	}

	return c.internalReconfig(request)
}

// Reconfig is the non-incremental update functionality for Zookeeper where the list provided
// is the entire new member list. For more info refer to the ZK documentation.
//
// An optional version allows for conditional reconfigurations, -1 ignores the condition.
//
// Returns the new configuration znode stat.
func (c *Conn) Reconfig(members []string, version int64) (*Stat, error) {
	request := &ReconfigRequest{
		NewMembers:  []byte(strings.Join(members, ",")),
		CurConfigId: version,
	}

	return c.internalReconfig(request)
}

func (c *Conn) internalReconfig(request *ReconfigRequest) (*Stat, error) {
	response := &reconfigReponse{}
	_, err := c.request(OpReconfig, request, response, nil)
	return &response.Stat, err
}

// Server returns the current or last-connected server name.
func (c *Conn) Server() string {
	c.serverMu.Lock()
	defer c.serverMu.Unlock()
	return c.server
}

func ResendZkAuth(ctx context.Context, c *Conn) error {
	shouldCancel := func() bool {
		select {
		case <-c.ShouldQuit:
			return true
		case <-c.closeChan:
			return true
		default:
			return false
		}
	}

	c.credsMu.Lock()
	defer c.credsMu.Unlock()

	if c.logInfo {
		c.Logger.Printf("re-submitting `%d` credentials after reconnect", len(c.creds))
	}

	for _, cred := range c.creds {
		// return early before attempting to send Request.
		if shouldCancel() {
			return nil
		}
		// do not use the public API for Auth since it depends on the send/recv loops
		// that are waiting for this to return
		resChan, err := c.sendRequest(
			OpSetAuth,
			&setAuthRequest{Type: 0,
				Scheme: cred.scheme,
				Auth:   cred.auth,
			},
			&setAuthResponse{},
			nil, /* recvFunc*/
		)
		if err != nil {
			return fmt.Errorf("failed to send Auth Request: %v", err)
		}

		var res response
		select {
		case res = <-resChan:
		case <-c.closeChan:
			c.Logger.Printf("recv closed, cancel re-submitting credentials")
			return nil
		case <-c.ShouldQuit:
			c.Logger.Printf("should quit, cancel re-submitting credentials")
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
		if res.err != nil {
			return fmt.Errorf("failed connection setAuth Request: %v", res.err)
		}
	}

	return nil
}
