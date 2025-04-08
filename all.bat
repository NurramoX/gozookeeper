package zk

import (
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"unicode/utf8"
)

// AuthACL produces an ACL list containing a single ACL which uses the
// provided permissions, with the scheme "auth", and ID "", which is used
// by ZooKeeper to represent any authenticated user.
func AuthACL(perms int32) []ACL {
	return []ACL{{perms, "auth", ""}}
}

// WorldACL produces an ACL list containing a single ACL which uses the
// provided permissions, with the scheme "world", and ID "anyone", which
// is used by ZooKeeper to represent any user at all.
func WorldACL(perms int32) []ACL {
	return []ACL{{perms, "world", "anyone"}}
}

func DigestACL(perms int32, user, password string) []ACL {
	userPass := []byte(fmt.Sprintf("%s:%s", user, password))
	h := sha1.New()
	if n, err := h.Write(userPass); err != nil || n != len(userPass) {
		panic("SHA1 failed")
	}
	digest := base64.StdEncoding.EncodeToString(h.Sum(nil))
	return []ACL{{perms, "digest", fmt.Sprintf("%s:%s", user, digest)}}
}

// FormatServers takes a slice of addresses, and makes sure they are in a format
// that resembles <addr>:<port>. If the server has no port provided, the
// DefaultPort constant is added to the end.
func FormatServers(servers []string) []string {
	srvs := make([]string, len(servers))
	for i, addr := range servers {
		if strings.Contains(addr, ":") {
			srvs[i] = addr
		} else {
			srvs[i] = addr + ":" + strconv.Itoa(DefaultPort)
		}
	}
	return srvs
}

// stringShuffle performs a Fisher-Yates shuffle on a slice of strings
func stringShuffle(s []string) {
	for i := len(s) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		s[i], s[j] = s[j], s[i]
	}
}

// validatePath will make sure a path is valid before sending the request
func validatePath(path string, isSequential bool) error {
	if path == "" {
		return ErrInvalidPath
	}

	if path[0] != '/' {
		return ErrInvalidPath
	}

	n := len(path)
	if n == 1 {
		// path is just the root
		return nil
	}

	if !isSequential && path[n-1] == '/' {
		return ErrInvalidPath
	}

	// Start at rune 1 since we already know that the first character is
	// a '/'.
	for i, w := 1, 0; i < n; i += w {
		r, width := utf8.DecodeRuneInString(path[i:])
		switch {
		case r == '\u0000':
			return ErrInvalidPath
		case r == '/':
			last, _ := utf8.DecodeLastRuneInString(path[:i])
			if last == '/' {
				return ErrInvalidPath
			}
		case r == '.':
			last, lastWidth := utf8.DecodeLastRuneInString(path[:i])

			// Check for double dot
			if last == '.' {
				last, _ = utf8.DecodeLastRuneInString(path[:i-lastWidth])
			}

			if last == '/' {
				if i+1 == n {
					return ErrInvalidPath
				}

				next, _ := utf8.DecodeRuneInString(path[i+w:])
				if next == '/' {
					return ErrInvalidPath
				}
			}
		case r >= '\u0000' && r <= '\u001f',
			r >= '\u007f' && r <= '\u009f',
			r >= '\uf000' && r <= '\uf8ff',
			r >= '\ufff0' && r < '\uffff':
			return ErrInvalidPath
		}
		w = width
	}
	return nil
}
package main

import (
	"fmt"
	"time"

	"github.com/go-zookeeper/zk"
)

func main() {
	c, _, err := zk.Connect([]string{"127.0.0.1"}, time.Second) //*10)
	if err != nil {
		panic(err)
	}
	children, stat, ch, err := c.ChildrenW("/")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v %+v\n", children, stat)
	e := <-ch
	fmt.Printf("%+v\n", e)
}
package zk

import (
	"encoding/binary"
	"errors"
	"log"
	"reflect"
	"runtime"
	"strings"
	"time"
)

var (
	ErrUnhandledFieldType = errors.New("zk: unhandled field type")
	ErrPtrExpected        = errors.New("zk: encode/decode expect a non-nil pointer to struct")
	ErrShortBuffer        = errors.New("zk: buffer too small")
)

type defaultLogger struct{}

func (defaultLogger) Printf(format string, a ...interface{}) {
	log.Printf(format, a...)
}

type ACL struct {
	Perms  int32
	Scheme string
	ID     string
}

type Stat struct {
	Czxid          int64 // The zxid of the change that caused this znode to be created.
	Mzxid          int64 // The zxid of the change that last modified this znode.
	Ctime          int64 // The time in milliseconds from epoch when this znode was created.
	Mtime          int64 // The time in milliseconds from epoch when this znode was last modified.
	Version        int32 // The number of changes to the data of this znode.
	Cversion       int32 // The number of changes to the children of this znode.
	Aversion       int32 // The number of changes to the ACL of this znode.
	EphemeralOwner int64 // The session id of the owner of this znode if the znode is an ephemeral node. If it is not an ephemeral node, it will be zero.
	DataLength     int32 // The length of the data field of this znode.
	NumChildren    int32 // The number of children of this znode.
	Pzxid          int64 // last modified children
}

// ServerClient is the information for a single Zookeeper client and its session.
// This is used to parse/extract the output fo the `cons` command.
type ServerClient struct {
	Queued        int64
	Received      int64
	Sent          int64
	SessionID     int64
	Lcxid         int64
	Lzxid         int64
	Timeout       int32
	LastLatency   int32
	MinLatency    int32
	AvgLatency    int32
	MaxLatency    int32
	Established   time.Time
	LastResponse  time.Time
	Addr          string
	LastOperation string // maybe?
	Error         error
}

// ServerClients is a struct for the FLWCons() function. It's used to provide
// the list of Clients.
//
// This is needed because FLWCons() takes multiple servers.
type ServerClients struct {
	Clients []*ServerClient
	Error   error
}

// ServerStats is the information pulled from the Zookeeper `stat` command.
type ServerStats struct {
	Server      string
	Sent        int64
	Received    int64
	NodeCount   int64
	MinLatency  int64
	AvgLatency  float64
	MaxLatency  int64
	Connections int64
	Outstanding int64
	Epoch       int32
	Counter     int32
	BuildTime   time.Time
	Mode        Mode
	Version     string
	Error       error
}

type requestHeader struct {
	Xid    int32
	Opcode int32
}

type responseHeader struct {
	Xid  int32
	Zxid int64
	Err  ErrCode
}

type multiHeader struct {
	Type int32
	Done bool
	Err  ErrCode
}

type auth struct {
	Type   int32
	Scheme string
	Auth   []byte
}

// Generic request structs

type pathRequest struct {
	Path string
}

type PathVersionRequest struct {
	Path    string
	Version int32
}

type pathWatchRequest struct {
	Path  string
	Watch bool
}

type pathResponse struct {
	Path string
}

type statResponse struct {
	Stat Stat
}

//

type CheckVersionRequest PathVersionRequest
type closeRequest struct{}
type closeResponse struct{}

type connectRequest struct {
	ProtocolVersion int32
	LastZxidSeen    int64
	TimeOut         int32
	SessionID       int64
	Passwd          []byte
}

type connectResponse struct {
	ProtocolVersion int32
	TimeOut         int32
	SessionID       int64
	Passwd          []byte
}

type CreateRequest struct {
	Path  string
	Data  []byte
	Acl   []ACL
	Flags int32
}

type CreateTTLRequest struct {
	Path  string
	Data  []byte
	Acl   []ACL
	Flags int32
	Ttl   int64 // ms
}

type createResponse pathResponse
type DeleteRequest PathVersionRequest
type deleteResponse struct{}

type errorResponse struct {
	Err int32
}

type existsRequest pathWatchRequest
type existsResponse statResponse
type getAclRequest pathRequest

type getAclResponse struct {
	Acl  []ACL
	Stat Stat
}

type getChildrenRequest pathRequest

type getChildrenResponse struct {
	Children []string
}

type getChildren2Request pathWatchRequest

type getChildren2Response struct {
	Children []string
	Stat     Stat
}

type getDataRequest pathWatchRequest

type getDataResponse struct {
	Data []byte
	Stat Stat
}

type getMaxChildrenRequest pathRequest

type getMaxChildrenResponse struct {
	Max int32
}

type getSaslRequest struct {
	Token []byte
}

type pingRequest struct{}
type pingResponse struct{}

type setAclRequest struct {
	Path    string
	Acl     []ACL
	Version int32
}

type setAclResponse statResponse

type SetDataRequest struct {
	Path    string
	Data    []byte
	Version int32
}

type setDataResponse statResponse

type setMaxChildren struct {
	Path string
	Max  int32
}

type setSaslRequest struct {
	Token string
}

type setSaslResponse struct {
	Token string
}

type setWatchesRequest struct {
	RelativeZxid int64
	DataWatches  []string
	ExistWatches []string
	ChildWatches []string
}

type setWatchesResponse struct{}

type syncRequest pathRequest
type syncResponse pathResponse

type setAuthRequest auth
type setAuthResponse struct{}

type multiRequestOp struct {
	Header multiHeader
	Op     interface{}
}
type multiRequest struct {
	Ops        []multiRequestOp
	DoneHeader multiHeader
}
type multiResponseOp struct {
	Header multiHeader
	String string
	Stat   *Stat
	Err    ErrCode
}
type multiResponse struct {
	Ops        []multiResponseOp
	DoneHeader multiHeader
}

// zk version 3.5 reconfig API
type reconfigRequest struct {
	JoiningServers []byte
	LeavingServers []byte
	NewMembers     []byte
	// curConfigId version of the current configuration
	// optional - causes reconfiguration to return an error if configuration is no longer current
	CurConfigId int64
}

type reconfigReponse getDataResponse

func (r *multiRequest) Encode(buf []byte) (int, error) {
	total := 0
	for _, op := range r.Ops {
		op.Header.Done = false
		n, err := encodePacketValue(buf[total:], reflect.ValueOf(op))
		if err != nil {
			return total, err
		}
		total += n
	}
	r.DoneHeader.Done = true
	n, err := encodePacketValue(buf[total:], reflect.ValueOf(r.DoneHeader))
	if err != nil {
		return total, err
	}
	total += n

	return total, nil
}

func (r *multiRequest) Decode(buf []byte) (int, error) {
	r.Ops = make([]multiRequestOp, 0)
	r.DoneHeader = multiHeader{-1, true, -1}
	total := 0
	for {
		header := &multiHeader{}
		n, err := decodePacketValue(buf[total:], reflect.ValueOf(header))
		if err != nil {
			return total, err
		}
		total += n
		if header.Done {
			r.DoneHeader = *header
			break
		}

		req := requestStructForOp(header.Type)
		if req == nil {
			return total, ErrAPIError
		}
		n, err = decodePacketValue(buf[total:], reflect.ValueOf(req))
		if err != nil {
			return total, err
		}
		total += n
		r.Ops = append(r.Ops, multiRequestOp{*header, req})
	}
	return total, nil
}

func (r *multiResponse) Decode(buf []byte) (int, error) {
	var multiErr error

	r.Ops = make([]multiResponseOp, 0)
	r.DoneHeader = multiHeader{-1, true, -1}
	total := 0
	for {
		header := &multiHeader{}
		n, err := decodePacketValue(buf[total:], reflect.ValueOf(header))
		if err != nil {
			return total, err
		}
		total += n
		if header.Done {
			r.DoneHeader = *header
			break
		}

		res := multiResponseOp{Header: *header}
		var w reflect.Value
		switch header.Type {
		default:
			return total, ErrAPIError
		case opError:
			w = reflect.ValueOf(&res.Err)
		case opCreate:
			w = reflect.ValueOf(&res.String)
		case opSetData:
			res.Stat = new(Stat)
			w = reflect.ValueOf(res.Stat)
		case opCheck, opDelete:
		}
		if w.IsValid() {
			n, err := decodePacketValue(buf[total:], w)
			if err != nil {
				return total, err
			}
			total += n
		}
		r.Ops = append(r.Ops, res)
		if multiErr == nil && res.Err != errOk {
			// Use the first error as the error returned from Multi().
			multiErr = res.Err.toError()
		}
	}
	return total, multiErr
}

type watcherEvent struct {
	Type  EventType
	State State
	Path  string
}

type decoder interface {
	Decode(buf []byte) (int, error)
}

type encoder interface {
	Encode(buf []byte) (int, error)
}

func decodePacket(buf []byte, st interface{}) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(runtime.Error); ok && strings.HasPrefix(e.Error(), "runtime error: slice bounds out of range") {
				err = ErrShortBuffer
			} else {
				panic(r)
			}
		}
	}()

	v := reflect.ValueOf(st)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return 0, ErrPtrExpected
	}
	return decodePacketValue(buf, v)
}

func decodePacketValue(buf []byte, v reflect.Value) (int, error) {
	rv := v
	kind := v.Kind()
	if kind == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
		kind = v.Kind()
	}

	n := 0
	switch kind {
	default:
		return n, ErrUnhandledFieldType
	case reflect.Struct:
		if de, ok := rv.Interface().(decoder); ok {
			return de.Decode(buf)
		} else if de, ok := v.Interface().(decoder); ok {
			return de.Decode(buf)
		} else {
			for i := 0; i < v.NumField(); i++ {
				field := v.Field(i)
				n2, err := decodePacketValue(buf[n:], field)
				n += n2
				if err != nil {
					return n, err
				}
			}
		}
	case reflect.Bool:
		v.SetBool(buf[n] != 0)
		n++
	case reflect.Int32:
		v.SetInt(int64(binary.BigEndian.Uint32(buf[n : n+4])))
		n += 4
	case reflect.Int64:
		v.SetInt(int64(binary.BigEndian.Uint64(buf[n : n+8])))
		n += 8
	case reflect.String:
		ln := int(binary.BigEndian.Uint32(buf[n : n+4]))
		v.SetString(string(buf[n+4 : n+4+ln]))
		n += 4 + ln
	case reflect.Slice:
		switch v.Type().Elem().Kind() {
		default:
			count := int(binary.BigEndian.Uint32(buf[n : n+4]))
			n += 4
			values := reflect.MakeSlice(v.Type(), count, count)
			v.Set(values)
			for i := 0; i < count; i++ {
				n2, err := decodePacketValue(buf[n:], values.Index(i))
				n += n2
				if err != nil {
					return n, err
				}
			}
		case reflect.Uint8:
			ln := int(int32(binary.BigEndian.Uint32(buf[n : n+4])))
			if ln < 0 {
				n += 4
				v.SetBytes(nil)
			} else {
				bytes := make([]byte, ln)
				copy(bytes, buf[n+4:n+4+ln])
				v.SetBytes(bytes)
				n += 4 + ln
			}
		}
	}
	return n, nil
}

func encodePacket(buf []byte, st interface{}) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(runtime.Error); ok && strings.HasPrefix(e.Error(), "runtime error: slice bounds out of range") {
				err = ErrShortBuffer
			} else {
				panic(r)
			}
		}
	}()

	v := reflect.ValueOf(st)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return 0, ErrPtrExpected
	}
	return encodePacketValue(buf, v)
}

func encodePacketValue(buf []byte, v reflect.Value) (int, error) {
	rv := v
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		v = v.Elem()
	}

	n := 0
	switch v.Kind() {
	default:
		return n, ErrUnhandledFieldType
	case reflect.Struct:
		if en, ok := rv.Interface().(encoder); ok {
			return en.Encode(buf)
		} else if en, ok := v.Interface().(encoder); ok {
			return en.Encode(buf)
		} else {
			for i := 0; i < v.NumField(); i++ {
				field := v.Field(i)
				n2, err := encodePacketValue(buf[n:], field)
				n += n2
				if err != nil {
					return n, err
				}
			}
		}
	case reflect.Bool:
		if v.Bool() {
			buf[n] = 1
		} else {
			buf[n] = 0
		}
		n++
	case reflect.Int32:
		binary.BigEndian.PutUint32(buf[n:n+4], uint32(v.Int()))
		n += 4
	case reflect.Int64:
		binary.BigEndian.PutUint64(buf[n:n+8], uint64(v.Int()))
		n += 8
	case reflect.String:
		str := v.String()
		binary.BigEndian.PutUint32(buf[n:n+4], uint32(len(str)))
		copy(buf[n+4:n+4+len(str)], []byte(str))
		n += 4 + len(str)
	case reflect.Slice:
		switch v.Type().Elem().Kind() {
		default:
			count := v.Len()
			startN := n
			n += 4
			for i := 0; i < count; i++ {
				n2, err := encodePacketValue(buf[n:], v.Index(i))
				n += n2
				if err != nil {
					return n, err
				}
			}
			binary.BigEndian.PutUint32(buf[startN:startN+4], uint32(count))
		case reflect.Uint8:
			if v.IsNil() {
				binary.BigEndian.PutUint32(buf[n:n+4], uint32(0xffffffff))
				n += 4
			} else {
				bytes := v.Bytes()
				binary.BigEndian.PutUint32(buf[n:n+4], uint32(len(bytes)))
				copy(buf[n+4:n+4+len(bytes)], bytes)
				n += 4 + len(bytes)
			}
		}
	}
	return n, nil
}

func requestStructForOp(op int32) interface{} {
	switch op {
	case opClose:
		return &closeRequest{}
	case opCreate, opCreateContainer:
		return &CreateRequest{}
	case opCreateTTL:
		return &CreateTTLRequest{}
	case opDelete:
		return &DeleteRequest{}
	case opExists:
		return &existsRequest{}
	case opGetAcl:
		return &getAclRequest{}
	case opGetChildren:
		return &getChildrenRequest{}
	case opGetChildren2:
		return &getChildren2Request{}
	case opGetData:
		return &getDataRequest{}
	case opPing:
		return &pingRequest{}
	case opSetAcl:
		return &setAclRequest{}
	case opSetData:
		return &SetDataRequest{}
	case opSetWatches:
		return &setWatchesRequest{}
	case opSync:
		return &syncRequest{}
	case opSetAuth:
		return &setAuthRequest{}
	case opCheck:
		return &CheckVersionRequest{}
	case opMulti:
		return &multiRequest{}
	case opReconfig:
		return &reconfigRequest{}
	}
	return nil
}
package zk

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

const _defaultLookupTimeout = 3 * time.Second

type lookupHostFn func(context.Context, string) ([]string, error)

// DNSHostProviderOption is an option for the DNSHostProvider.
type DNSHostProviderOption interface {
	apply(*DNSHostProvider)
}

type lookupTimeoutOption struct {
	timeout time.Duration
}

// WithLookupTimeout returns a DNSHostProviderOption that sets the lookup timeout.
func WithLookupTimeout(timeout time.Duration) DNSHostProviderOption {
	return lookupTimeoutOption{
		timeout: timeout,
	}
}

func (o lookupTimeoutOption) apply(provider *DNSHostProvider) {
	provider.lookupTimeout = o.timeout
}

// DNSHostProvider is the default HostProvider. It currently matches
// the Java StaticHostProvider, resolving hosts from DNS once during
// the call to Init.  It could be easily extended to re-query DNS
// periodically or if there is trouble connecting.
type DNSHostProvider struct {
	mu            sync.Mutex // Protects everything, so we can add asynchronous updates later.
	servers       []string
	curr          int
	last          int
	lookupTimeout time.Duration
	lookupHost    lookupHostFn // Override of net.LookupHost, for testing.
}

// NewDNSHostProvider creates a new DNSHostProvider with the given options.
func NewDNSHostProvider(options ...DNSHostProviderOption) *DNSHostProvider {
	var provider DNSHostProvider
	for _, option := range options {
		option.apply(&provider)
	}
	return &provider
}

// Init is called first, with the servers specified in the connection
// string. It uses DNS to look up addresses for each server, then
// shuffles them all together.
func (hp *DNSHostProvider) Init(servers []string) error {
	hp.mu.Lock()
	defer hp.mu.Unlock()

	lookupHost := hp.lookupHost
	if lookupHost == nil {
		var resolver net.Resolver
		lookupHost = resolver.LookupHost
	}

	timeout := hp.lookupTimeout
	if timeout == 0 {
		timeout = _defaultLookupTimeout
	}

	// TODO: consider using a context from the caller.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	found := []string{}
	for _, server := range servers {
		host, port, err := net.SplitHostPort(server)
		if err != nil {
			return err
		}
		addrs, err := lookupHost(ctx, host)
		if err != nil {
			return err
		}
		for _, addr := range addrs {
			found = append(found, net.JoinHostPort(addr, port))
		}
	}

	if len(found) == 0 {
		return fmt.Errorf("No hosts found for addresses %q", servers)
	}

	// Randomize the order of the servers to avoid creating hotspots
	stringShuffle(found)

	hp.servers = found
	hp.curr = -1
	hp.last = -1

	return nil
}

// Len returns the number of servers available
func (hp *DNSHostProvider) Len() int {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	return len(hp.servers)
}

// Next returns the next server to connect to. retryStart will be true
// if we've looped through all known servers without Connected() being
// called.
func (hp *DNSHostProvider) Next() (server string, retryStart bool) {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	hp.curr = (hp.curr + 1) % len(hp.servers)
	retryStart = hp.curr == hp.last
	if hp.last == -1 {
		hp.last = 0
	}
	return hp.servers[hp.curr], retryStart
}

// Connected notifies the HostProvider of a successful connection.
func (hp *DNSHostProvider) Connected() {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	hp.last = hp.curr
}
package zk

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	// ErrDeadlock is returned by Lock when trying to lock twice without unlocking first
	ErrDeadlock = errors.New("zk: trying to acquire a lock twice")
	// ErrNotLocked is returned by Unlock when trying to release a lock that has not first be acquired.
	ErrNotLocked = errors.New("zk: not locked")
)

// Lock is a mutual exclusion lock.
type Lock struct {
	c        *Conn
	path     string
	acl      []ACL
	lockPath string
	seq      int
}

// NewLock creates a new lock instance using the provided connection, path, and acl.
// The path must be a node that is only used by this lock. A lock instances starts
// unlocked until Lock() is called.
func NewLock(c *Conn, path string, acl []ACL) *Lock {
	return &Lock{
		c:    c,
		path: path,
		acl:  acl,
	}
}

func parseSeq(path string) (int, error) {
	parts := strings.Split(path, "lock-")
	// python client uses a __LOCK__ prefix
	if len(parts) == 1 {
		parts = strings.Split(path, "__")
	}
	return strconv.Atoi(parts[len(parts)-1])
}

// Lock attempts to acquire the lock. It works like LockWithData, but it doesn't
// write any data to the lock node.
func (l *Lock) Lock() error {
	return l.LockWithData([]byte{})
}

// LockWithData attempts to acquire the lock, writing data into the lock node.
// It will wait to return until the lock is acquired or an error occurs. If
// this instance already has the lock then ErrDeadlock is returned.
func (l *Lock) LockWithData(data []byte) error {
	if l.lockPath != "" {
		return ErrDeadlock
	}

	prefix := fmt.Sprintf("%s/lock-", l.path)

	path := ""
	var err error
	for i := 0; i < 3; i++ {
		path, err = l.c.CreateProtectedEphemeralSequential(prefix, data, l.acl)
		if err == ErrNoNode {
			// Create parent node.
			parts := strings.Split(l.path, "/")
			pth := ""
			for _, p := range parts[1:] {
				var exists bool
				pth += "/" + p
				exists, _, err = l.c.Exists(pth)
				if err != nil {
					return err
				}
				if exists == true {
					continue
				}
				_, err = l.c.Create(pth, []byte{}, 0, l.acl)
				if err != nil && err != ErrNodeExists {
					return err
				}
			}
		} else if err == nil {
			break
		} else {
			return err
		}
	}
	if err != nil {
		return err
	}

	seq, err := parseSeq(path)
	if err != nil {
		return err
	}

	for {
		children, _, err := l.c.Children(l.path)
		if err != nil {
			return err
		}

		lowestSeq := seq
		prevSeq := -1
		prevSeqPath := ""
		for _, p := range children {
			s, err := parseSeq(p)
			if err != nil {
				return err
			}
			if s < lowestSeq {
				lowestSeq = s
			}
			if s < seq && s > prevSeq {
				prevSeq = s
				prevSeqPath = p
			}
		}

		if seq == lowestSeq {
			// Acquired the lock
			break
		}

		// Wait on the node next in line for the lock
		_, _, ch, err := l.c.GetW(l.path + "/" + prevSeqPath)
		if err != nil && err != ErrNoNode {
			return err
		} else if err != nil && err == ErrNoNode {
			// try again
			continue
		}

		ev := <-ch
		if ev.Err != nil {
			return ev.Err
		}
	}

	l.seq = seq
	l.lockPath = path
	return nil
}

// Unlock releases an acquired lock. If the lock is not currently acquired by
// this Lock instance than ErrNotLocked is returned.
func (l *Lock) Unlock() error {
	if l.lockPath == "" {
		return ErrNotLocked
	}
	if err := l.c.Delete(l.lockPath, -1); err != nil {
		return err
	}
	l.lockPath = ""
	l.seq = 0
	return nil
}
package zk

import (
	"errors"
	"fmt"
)

const (
	protocolVersion = 0
	// DefaultPort is the default port listened by server.
	DefaultPort = 2181
)

const (
	opNotify          = 0
	opCreate          = 1
	opDelete          = 2
	opExists          = 3
	opGetData         = 4
	opSetData         = 5
	opGetAcl          = 6
	opSetAcl          = 7
	opGetChildren     = 8
	opSync            = 9
	opPing            = 11
	opGetChildren2    = 12
	opCheck           = 13
	opMulti           = 14
	opReconfig        = 16
	opCreateContainer = 19
	opCreateTTL       = 21
	opClose           = -11
	opSetAuth         = 100
	opSetWatches      = 101
	opError           = -1
	// Not in protocol, used internally
	opWatcherEvent = -2
)

const (
	// EventNodeCreated represents a node is created.
	EventNodeCreated         EventType = 1
	EventNodeDeleted         EventType = 2
	EventNodeDataChanged     EventType = 3
	EventNodeChildrenChanged EventType = 4

	// EventSession represents a session event.
	EventSession     EventType = -1
	EventNotWatching EventType = -2
)

var (
	eventNames = map[EventType]string{
		EventNodeCreated:         "EventNodeCreated",
		EventNodeDeleted:         "EventNodeDeleted",
		EventNodeDataChanged:     "EventNodeDataChanged",
		EventNodeChildrenChanged: "EventNodeChildrenChanged",
		EventSession:             "EventSession",
		EventNotWatching:         "EventNotWatching",
	}
)

const (
	// StateUnknown means the session state is unknown.
	StateUnknown           State = -1
	StateDisconnected      State = 0
	StateConnecting        State = 1
	StateSyncConnected     State = 3
	StateAuthFailed        State = 4
	StateConnectedReadOnly State = 5
	StateSaslAuthenticated State = 6
	StateExpired           State = -112

	StateConnected  = State(100)
	StateHasSession = State(101)
)

var (
	stateNames = map[State]string{
		StateUnknown:           "StateUnknown",
		StateDisconnected:      "StateDisconnected",
		StateConnectedReadOnly: "StateConnectedReadOnly",
		StateSaslAuthenticated: "StateSaslAuthenticated",
		StateExpired:           "StateExpired",
		StateAuthFailed:        "StateAuthFailed",
		StateConnecting:        "StateConnecting",
		StateConnected:         "StateConnected",
		StateHasSession:        "StateHasSession",
		StateSyncConnected:     "StateSyncConnected",
	}
)

// State is the session state.
type State int32

// String converts State to a readable string.
func (s State) String() string {
	if name := stateNames[s]; name != "" {
		return name
	}
	return "Unknown"
}

// ErrCode is the error code defined by server. Refer to ZK documentations for more specifics.
type ErrCode int32

var (
	// ErrConnectionClosed means the connection has been closed.
	ErrConnectionClosed        = errors.New("zk: connection closed")
	ErrUnknown                 = errors.New("zk: unknown error")
	ErrAPIError                = errors.New("zk: api error")
	ErrNoNode                  = errors.New("zk: node does not exist")
	ErrNoAuth                  = errors.New("zk: not authenticated")
	ErrBadVersion              = errors.New("zk: version conflict")
	ErrNoChildrenForEphemerals = errors.New("zk: ephemeral nodes may not have children")
	ErrNodeExists              = errors.New("zk: node already exists")
	ErrNotEmpty                = errors.New("zk: node has children")
	ErrSessionExpired          = errors.New("zk: session has been expired by the server")
	ErrInvalidACL              = errors.New("zk: invalid ACL specified")
	ErrInvalidFlags            = errors.New("zk: invalid flags specified")
	ErrAuthFailed              = errors.New("zk: client authentication failed")
	ErrClosing                 = errors.New("zk: zookeeper is closing")
	ErrNothing                 = errors.New("zk: no server responses to process")
	ErrSessionMoved            = errors.New("zk: session moved to another server, so operation is ignored")
	ErrReconfigDisabled        = errors.New("attempts to perform a reconfiguration operation when reconfiguration feature is disabled")
	ErrBadArguments            = errors.New("invalid arguments")
	// ErrInvalidCallback         = errors.New("zk: invalid callback specified")

	errCodeToError = map[ErrCode]error{
		0:                          nil,
		errAPIError:                ErrAPIError,
		errNoNode:                  ErrNoNode,
		errNoAuth:                  ErrNoAuth,
		errBadVersion:              ErrBadVersion,
		errNoChildrenForEphemerals: ErrNoChildrenForEphemerals,
		errNodeExists:              ErrNodeExists,
		errNotEmpty:                ErrNotEmpty,
		errSessionExpired:          ErrSessionExpired,
		// errInvalidCallback:         ErrInvalidCallback,
		errInvalidAcl:        ErrInvalidACL,
		errAuthFailed:        ErrAuthFailed,
		errClosing:           ErrClosing,
		errNothing:           ErrNothing,
		errSessionMoved:      ErrSessionMoved,
		errZReconfigDisabled: ErrReconfigDisabled,
		errBadArguments:      ErrBadArguments,
	}
)

func (e ErrCode) toError() error {
	if err, ok := errCodeToError[e]; ok {
		return err
	}
	return fmt.Errorf("unknown error: %v", e)
}

const (
	errOk = 0
	// System and server-side errors
	errSystemError          = -1
	errRuntimeInconsistency = -2
	errDataInconsistency    = -3
	errConnectionLoss       = -4
	errMarshallingError     = -5
	errUnimplemented        = -6
	errOperationTimeout     = -7
	errBadArguments         = -8
	errInvalidState         = -9
	// API errors
	errAPIError                ErrCode = -100
	errNoNode                  ErrCode = -101 // *
	errNoAuth                  ErrCode = -102
	errBadVersion              ErrCode = -103 // *
	errNoChildrenForEphemerals ErrCode = -108
	errNodeExists              ErrCode = -110 // *
	errNotEmpty                ErrCode = -111
	errSessionExpired          ErrCode = -112
	errInvalidCallback         ErrCode = -113
	errInvalidAcl              ErrCode = -114
	errAuthFailed              ErrCode = -115
	errClosing                 ErrCode = -116
	errNothing                 ErrCode = -117
	errSessionMoved            ErrCode = -118
	// Attempts to perform a reconfiguration operation when reconfiguration feature is disabled
	errZReconfigDisabled ErrCode = -123
)

// Constants for ACL permissions
const (
	// PermRead represents the permission needed to read a znode.
	PermRead = 1 << iota
	PermWrite
	PermCreate
	PermDelete
	PermAdmin
	PermAll = 0x1f
)

var (
	emptyPassword = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	opNames       = map[int32]string{
		opNotify:          "notify",
		opCreate:          "create",
		opCreateContainer: "createContainer",
		opCreateTTL:       "createTTL",
		opDelete:          "delete",
		opExists:          "exists",
		opGetData:         "getData",
		opSetData:         "setData",
		opGetAcl:          "getACL",
		opSetAcl:          "setACL",
		opGetChildren:     "getChildren",
		opSync:            "sync",
		opPing:            "ping",
		opGetChildren2:    "getChildren2",
		opCheck:           "check",
		opMulti:           "multi",
		opReconfig:        "reconfig",
		opClose:           "close",
		opSetAuth:         "setAuth",
		opSetWatches:      "setWatches",

		opWatcherEvent: "watcherEvent",
	}
)

// EventType represents the event type sent by server.
type EventType int32

func (t EventType) String() string {
	if name := eventNames[t]; name != "" {
		return name
	}
	return "Unknown"
}

// Mode is used to build custom server modes (leader|follower|standalone).
type Mode uint8

func (m Mode) String() string {
	if name := modeNames[m]; name != "" {
		return name
	}
	return "unknown"
}

const (
	ModeUnknown    Mode = iota
	ModeLeader     Mode = iota
	ModeFollower   Mode = iota
	ModeStandalone Mode = iota
)

var (
	modeNames = map[Mode]string{
		ModeLeader:     "leader",
		ModeFollower:   "follower",
		ModeStandalone: "standalone",
	}
)
// Internal assertion library inspired by https://github.com/stretchr/testify.
// "A little copying is better than a little dependency." - https://go-proverbs.github.io.
// We don't want the library to have dependencies, so we write our own assertions.
package assert

import (
	"fmt"
	"reflect"
)

// TestingT is an interface wrapper around stdlib *testing.T.
type TestingT interface {
	Errorf(format string, args ...interface{})
	Helper()
}

// Equal asserts that expected is equal to actual.
func Equal(t TestingT, want, got interface{}, msgAndArgs ...interface{}) {
	if !reflect.DeepEqual(want, got) {
		fail(t, fmt.Sprintf("not equal: want: %+v, got: %+v", want, got), msgAndArgs)
	}
}

// NoError asserts that the error is nil.
func NoError(t TestingT, err error, msgAndArgs ...interface{}) {
	if err != nil {
		fail(t, fmt.Sprintf("unexpected error: %v", err), msgAndArgs)
	}
}

func fail(t TestingT, message string, msgAndArgs []interface{}) {
	t.Helper()
	userMessage := msgAndArgsToString(msgAndArgs)
	if userMessage != "" {
		message += ": " + userMessage
	}
	t.Errorf(message)
}

func msgAndArgsToString(msgAndArgs []interface{}) string {
	if len(msgAndArgs) == 0 {
		return ""
	}
	if len(msgAndArgs) == 1 {
		return fmt.Sprintf("%+v", msgAndArgs[0])
	}
	if format, ok := msgAndArgs[0].(string); ok {
		return fmt.Sprintf(format, msgAndArgs[1:]...)
	}
	return fmt.Sprintf("%+v", msgAndArgs)
}
// Package zk is a native Go client library for the ZooKeeper orchestration service.
package zk

/*
TODO:
* make sure a ping response comes back in a reasonable time

Possible watcher events:
* Event{Type: EventNotWatching, State: StateDisconnected, Path: path, Err: err}
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
// an invalid path. (e.g. empty path).
var ErrInvalidPath = errors.New("zk: invalid path")

// DefaultLogger uses the stdlib log package for logging.
var DefaultLogger Logger = defaultLogger{}

const (
	bufferSize      = 1536 * 1024
	eventChanSize   = 6
	sendChanSize    = 16
	protectedPrefix = "_c_"
)

type watchType int

const (
	watchTypeData watchType = iota
	watchTypeExist
	watchTypeChild
)

type watchPathType struct {
	path  string
	wType watchType
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
	lastZxid         int64
	sessionID        int64
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
	shouldQuit     chan struct{}
	shouldQuitOnce sync.Once
	pingInterval   time.Duration
	recvTimeout    time.Duration
	connectTimeout time.Duration
	maxBufferSize  int

	creds   []authCreds
	credsMu sync.Mutex // protects server

	sendChan     chan *request
	requests     map[int32]*request // Xid -> pending request
	requestsLock sync.Mutex
	watchers     map[watchPathType][]chan Event
	watchersLock sync.Mutex
	closeChan    chan struct{} // channel to tell send loop stop

	// Debug (used by unit tests)
	reconnectLatch   chan struct{}
	setWatchLimit    int
	setWatchCallback func([]*setWatchesRequest)

	// Debug (for recurring re-auth hang)
	debugCloseRecvLoop bool
	resendZkAuthFn     func(context.Context, *Conn) error

	logger  Logger
	logInfo bool // true if information messages are logged; false if only errors are logged

	buf []byte
}

// connOption represents a connection option.
type connOption func(c *Conn)

type request struct {
	xid        int32
	opcode     int32
	pkt        interface{}
	recvStruct interface{}
	recvChan   chan response

	// Because sending and receiving happen in separate go routines, there's
	// a possible race condition when creating watches from outside the read
	// loop. We must ensure that a watcher gets added to the list synchronously
	// with the response from the server on any request that creates a watch.
	// In order to not hard code the watch logic for each opcode in the recv
	// loop the caller can use recvFunc to insert some synchronously code
	// after a response.
	recvFunc func(*request, *responseHeader, error)
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
	Path   string // For non-session events, the path of the watched node.
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
func Connect(servers []string, sessionTimeout time.Duration, options ...connOption) (*Conn, <-chan Event, error) {
	if len(servers) == 0 {
		return nil, nil, errors.New("zk: server list must not be empty")
	}

	srvs := FormatServers(servers)

	// Randomize the order of the servers to avoid creating hotspots
	stringShuffle(srvs)

	ec := make(chan Event, eventChanSize)
	conn := &Conn{
		dialer:         net.DialTimeout,
		hostProvider:   NewDNSHostProvider(),
		conn:           nil,
		state:          StateDisconnected,
		eventChan:      ec,
		shouldQuit:     make(chan struct{}),
		connectTimeout: 1 * time.Second,
		sendChan:       make(chan *request, sendChanSize),
		requests:       make(map[int32]*request),
		watchers:       make(map[watchPathType][]chan Event),
		passwd:         emptyPassword,
		logger:         DefaultLogger,
		logInfo:        true, // default is true for backwards compatability
		buf:            make([]byte, bufferSize),
		resendZkAuthFn: resendZkAuth,
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
func WithDialer(dialer Dialer) connOption {
	return func(c *Conn) {
		c.dialer = dialer
	}
}

// WithHostProvider returns a connection option specifying a non-default HostProvider.
func WithHostProvider(hostProvider HostProvider) connOption {
	return func(c *Conn) {
		c.hostProvider = hostProvider
	}
}

// WithLogger returns a connection option specifying a non-default Logger.
func WithLogger(logger Logger) connOption {
	return func(c *Conn) {
		c.logger = logger
	}
}

// WithLogInfo returns a connection option specifying whether or not information messages
// should be logged.
func WithLogInfo(logInfo bool) connOption {
	return func(c *Conn) {
		c.logInfo = logInfo
	}
}

// EventCallback is a function that is called when an Event occurs.
type EventCallback func(Event)

// WithEventCallback returns a connection option that specifies an event
// callback.
// The callback must not block - doing so would delay the ZK go routines.
func WithEventCallback(cb EventCallback) connOption {
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
// sending an online request to a server.
func WithMaxBufferSize(maxBufferSize int) connOption {
	return func(c *Conn) {
		c.maxBufferSize = maxBufferSize
	}
}

// WithMaxConnBufferSize sets maximum buffer size used to send and encode
// packets to Zookeeper server. The standard Zookeeper client for java defaults
// to a limit of 1mb. This option should be used for non-standard server setup
// where znode is bigger than default 1mb.
func WithMaxConnBufferSize(maxBufferSize int) connOption {
	return func(c *Conn) {
		c.buf = make([]byte, maxBufferSize)
	}
}

// Close will submit a close request with ZK and signal the connection to stop
// sending and receiving packets.
func (c *Conn) Close() {
	c.shouldQuitOnce.Do(func() {
		close(c.shouldQuit)

		select {
		case <-c.queueRequest(opClose, &closeRequest{}, &closeResponse{}, nil):
		case <-time.After(time.Second):
		}
	})
}

// State returns the current state of the connection.
func (c *Conn) State() State {
	return State(atomic.LoadInt32((*int32)(&c.state)))
}

// SessionID returns the current session id of the connection.
func (c *Conn) SessionID() int64 {
	return atomic.LoadInt64(&c.sessionID)
}

// SetLogger sets the logger to be used for printing errors.
// Logger is an interface provided by this package.
func (c *Conn) SetLogger(l Logger) {
	c.logger = l
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
			case <-c.shouldQuit:
				c.setState(StateDisconnected)
				c.flushUnsentRequests(ErrClosing)
				return ErrClosing
			}
		}

		zkConn, err := c.dialer("tcp", c.Server(), c.connectTimeout)
		if err == nil {
			c.conn = zkConn
			c.setState(StateConnected)
			if c.logInfo {
				c.logger.Printf("connected to %s", c.Server())
			}
			return nil
		}

		c.logger.Printf("failed to connect to %s: %v", c.Server(), err)
	}
}

func (c *Conn) sendRequest(
	opcode int32,
	req interface{},
	res interface{},
	recvFunc func(*request, *responseHeader, error),
) (
	<-chan response,
	error,
) {
	rq := &request{
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
			c.logger.Printf("authentication failed: %s", err)
			c.invalidateWatches(err)
		case err != nil && c.conn != nil:
			c.logger.Printf("authentication failed: %s", err)
			c.conn.Close()
		case err == nil:
			if c.logInfo {
				c.logger.Printf("authenticated: id=%d, timeout=%d", c.SessionID(), c.sessionTimeoutMs)
			}
			c.hostProvider.Connected()        // mark success
			c.closeChan = make(chan struct{}) // channel to tell send loop stop

			var wg sync.WaitGroup

			wg.Add(1)
			go func() {
				defer c.conn.Close() // causes recv loop to EOF/exit
				defer wg.Done()

				if err := c.resendZkAuthFn(ctx, c); err != nil {
					c.logger.Printf("error in resending auth creds: %v", err)
					return
				}

				if err := c.sendLoop(); err != nil || c.logInfo {
					c.logger.Printf("send loop terminated: %v", err)
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
					c.logger.Printf("recv loop terminated: %v", err)
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
		case <-c.shouldQuit:
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
			case <-c.shouldQuit:
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
		case req := <-c.sendChan:
			req.recvChan <- response{-1, err}
		}
	}
}

// Send error to all pending requests and clear request map
func (c *Conn) flushRequests(err error) {
	c.requestsLock.Lock()
	for _, req := range c.requests {
		req.recvChan <- response{-1, err}
	}
	c.requests = make(map[int32]*request)
	c.requestsLock.Unlock()
}

// Send event to all interested watchers
func (c *Conn) notifyWatches(ev Event) {
	var wTypes []watchType
	switch ev.Type {
	case EventNodeCreated:
		wTypes = []watchType{watchTypeExist}
	case EventNodeDataChanged:
		wTypes = []watchType{watchTypeExist, watchTypeData}
	case EventNodeChildrenChanged:
		wTypes = []watchType{watchTypeChild}
	case EventNodeDeleted:
		wTypes = []watchType{watchTypeExist, watchTypeData, watchTypeChild}
	}
	c.watchersLock.Lock()
	defer c.watchersLock.Unlock()
	for _, t := range wTypes {
		wpt := watchPathType{ev.Path, t}
		if watchers := c.watchers[wpt]; len(watchers) > 0 {
			for _, ch := range watchers {
				ch <- ev
				close(ch)
			}
			delete(c.watchers, wpt)
		}
	}
}

// Send error to all watchers and clear watchers map
func (c *Conn) invalidateWatches(err error) {
	c.watchersLock.Lock()
	defer c.watchersLock.Unlock()

	if len(c.watchers) >= 0 {
		for pathType, watchers := range c.watchers {
			ev := Event{Type: EventNotWatching, State: StateDisconnected, Path: pathType.path, Err: err}
			c.sendEvent(ev) // also publish globally
			for _, ch := range watchers {
				ch <- ev
				close(ch)
			}
		}
		c.watchers = make(map[watchPathType][]chan Event)
	}
}

func (c *Conn) sendSetWatches() {
	c.watchersLock.Lock()
	defer c.watchersLock.Unlock()

	if len(c.watchers) == 0 {
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

	var reqs []*setWatchesRequest
	var req *setWatchesRequest
	var sizeSoFar int

	n := 0
	for pathType, watchers := range c.watchers {
		if len(watchers) == 0 {
			continue
		}
		addlLen := 4 + len(pathType.path)
		if req == nil || sizeSoFar+addlLen > limit {
			if req != nil {
				// add to set of requests that we'll send
				reqs = append(reqs, req)
			}
			sizeSoFar = 28 // fixed overhead of a set-watches packet
			req = &setWatchesRequest{
				RelativeZxid: c.lastZxid,
				DataWatches:  make([]string, 0),
				ExistWatches: make([]string, 0),
				ChildWatches: make([]string, 0),
			}
		}
		sizeSoFar += addlLen
		switch pathType.wType {
		case watchTypeData:
			req.DataWatches = append(req.DataWatches, pathType.path)
		case watchTypeExist:
			req.ExistWatches = append(req.ExistWatches, pathType.path)
		case watchTypeChild:
			req.ChildWatches = append(req.ChildWatches, pathType.path)
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
			_, err := c.request(opSetWatches, req, res, nil)
			if err != nil {
				c.logger.Printf("Failed to set previous watches: %v", err)
				break
			}
		}
	}()
}

func (c *Conn) authenticate() error {
	buf := make([]byte, 256)

	// Encode and send a connect request.
	n, err := encodePacket(buf[4:], &connectRequest{
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

	r := connectResponse{}
	_, err = decodePacket(buf[:blen], &r)
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

func (c *Conn) sendData(req *request) error {
	header := &requestHeader{req.xid, req.opcode}
	n, err := encodePacket(c.buf[4:], header)
	if err != nil {
		req.recvChan <- response{-1, err}
		return nil
	}

	n2, err := encodePacket(c.buf[4+n:], req.pkt)
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
		case req := <-c.sendChan:
			if err := c.sendData(req); err != nil {
				return err
			}
		case <-pingTicker.C:
			n, err := encodePacket(c.buf[4:], &requestHeader{Xid: -2, Opcode: opPing})
			if err != nil {
				panic("zk: opPing should never fail to serialize")
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
	sz := bufferSize
	if c.maxBufferSize > 0 && sz > c.maxBufferSize {
		sz = c.maxBufferSize
	}
	buf := make([]byte, sz)
	for {
		// package length
		if err := conn.SetReadDeadline(time.Now().Add(c.recvTimeout)); err != nil {
			c.logger.Printf("failed to set connection deadline: %v", err)
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

		res := responseHeader{}
		_, err = decodePacket(buf[:16], &res)
		if err != nil {
			return err
		}

		if res.Xid == -1 {
			res := &watcherEvent{}
			_, err = decodePacket(buf[16:blen], res)
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
			c.notifyWatches(ev)
		} else if res.Xid == -2 {
			// Ping response. Ignore.
		} else if res.Xid < 0 {
			c.logger.Printf("Xid < 0 (%d) but not ping or watcher event", res.Xid)
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
				c.logger.Printf("Response for unknown request with xid %d", res.Xid)
			} else {
				if res.Err != 0 {
					err = res.Err.toError()
				} else {
					_, err = decodePacket(buf[16:blen], req.recvStruct)
				}
				if req.recvFunc != nil {
					req.recvFunc(req, &res, err)
				}
				req.recvChan <- response{res.Zxid, err}
				if req.opcode == opClose {
					return io.EOF
				}
			}
		}
	}
}

func (c *Conn) nextXid() int32 {
	return int32(atomic.AddUint32(&c.xid, 1) & 0x7fffffff)
}

func (c *Conn) addWatcher(path string, watchType watchType) <-chan Event {
	c.watchersLock.Lock()
	defer c.watchersLock.Unlock()

	ch := make(chan Event, 1)
	wpt := watchPathType{path, watchType}
	c.watchers[wpt] = append(c.watchers[wpt], ch)
	return ch
}

func (c *Conn) queueRequest(opcode int32, req interface{}, res interface{}, recvFunc func(*request, *responseHeader, error)) <-chan response {
	rq := &request{
		xid:        c.nextXid(),
		opcode:     opcode,
		pkt:        req,
		recvStruct: res,
		recvChan:   make(chan response, 2),
		recvFunc:   recvFunc,
	}

	switch opcode {
	case opClose:
		// always attempt to send close ops.
		select {
		case c.sendChan <- rq:
		case <-time.After(c.connectTimeout * 2):
			c.logger.Printf("gave up trying to send opClose to server")
			rq.recvChan <- response{-1, ErrConnectionClosed}
		}
	default:
		// otherwise avoid deadlocks for dumb clients who aren't aware that
		// the ZK connection is closed yet.
		select {
		case <-c.shouldQuit:
			rq.recvChan <- response{-1, ErrConnectionClosed}
		case c.sendChan <- rq:
			// check for a tie
			select {
			case <-c.shouldQuit:
				// maybe the caller gets this, maybe not- we tried.
				rq.recvChan <- response{-1, ErrConnectionClosed}
			default:
			}
		}
	}
	return rq.recvChan
}

func (c *Conn) request(opcode int32, req interface{}, res interface{}, recvFunc func(*request, *responseHeader, error)) (int64, error) {
	recv := c.queueRequest(opcode, req, res, recvFunc)
	select {
	case r := <-recv:
		return r.zxid, r.err
	case <-c.shouldQuit:
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
	_, err := c.request(opSetAuth, &setAuthRequest{Type: 0, Scheme: scheme, Auth: auth}, &setAuthResponse{}, nil)

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
	if err := validatePath(path, false); err != nil {
		return nil, nil, err
	}

	res := &getChildren2Response{}
	_, err := c.request(opGetChildren2, &getChildren2Request{Path: path, Watch: false}, res, nil)
	if err == ErrConnectionClosed {
		return nil, nil, err
	}
	return res.Children, &res.Stat, err
}

// ChildrenW returns the children of a znode and sets a watch.
func (c *Conn) ChildrenW(path string) ([]string, *Stat, <-chan Event, error) {
	if err := validatePath(path, false); err != nil {
		return nil, nil, nil, err
	}

	var ech <-chan Event
	res := &getChildren2Response{}
	_, err := c.request(opGetChildren2, &getChildren2Request{Path: path, Watch: true}, res, func(req *request, res *responseHeader, err error) {
		if err == nil {
			ech = c.addWatcher(path, watchTypeChild)
		}
	})
	if err != nil {
		return nil, nil, nil, err
	}
	return res.Children, &res.Stat, ech, err
}

// Get gets the contents of a znode.
func (c *Conn) Get(path string) ([]byte, *Stat, error) {
	if err := validatePath(path, false); err != nil {
		return nil, nil, err
	}

	res := &getDataResponse{}
	_, err := c.request(opGetData, &getDataRequest{Path: path, Watch: false}, res, nil)
	if err == ErrConnectionClosed {
		return nil, nil, err
	}
	return res.Data, &res.Stat, err
}

// GetW returns the contents of a znode and sets a watch
func (c *Conn) GetW(path string) ([]byte, *Stat, <-chan Event, error) {
	if err := validatePath(path, false); err != nil {
		return nil, nil, nil, err
	}

	var ech <-chan Event
	res := &getDataResponse{}
	_, err := c.request(opGetData, &getDataRequest{Path: path, Watch: true}, res, func(req *request, res *responseHeader, err error) {
		if err == nil {
			ech = c.addWatcher(path, watchTypeData)
		}
	})
	if err != nil {
		return nil, nil, nil, err
	}
	return res.Data, &res.Stat, ech, err
}

// Set updates the contents of a znode.
func (c *Conn) Set(path string, data []byte, version int32) (*Stat, error) {
	if err := validatePath(path, false); err != nil {
		return nil, err
	}

	res := &setDataResponse{}
	_, err := c.request(opSetData, &SetDataRequest{path, data, version}, res, nil)
	if err == ErrConnectionClosed {
		return nil, err
	}
	return &res.Stat, err
}

// Create creates a znode.
// The returned path is the new path assigned by the server, it may not be the
// same as the input, for example when creating a sequence znode the returned path
// will be the input path with a sequence number appended.
func (c *Conn) Create(path string, data []byte, flags int32, acl []ACL) (string, error) {
	createMode, err := parseCreateMode(flags)
	if err != nil {
		return "", err
	}

	if err := validatePath(path, createMode.isSequential); err != nil {
		return "", err
	}

	if createMode.isTTL {
		return "", fmt.Errorf("Create with TTL flag disallowed: %w", ErrInvalidFlags)
	}

	res := &createResponse{}
	_, err = c.request(opCreate, &CreateRequest{path, data, acl, createMode.flag}, res, nil)
	if err == ErrConnectionClosed {
		return "", err
	}
	return res.Path, err
}

// CreateContainer creates a container znode and returns the path.
//
// Containers cannot be ephemeral or sequential, or have TTLs.
// Ensure that we reject flags for TTL, Sequence, and Ephemeral.
func (c *Conn) CreateContainer(path string, data []byte, flag int32, acl []ACL) (string, error) {
	createMode, err := parseCreateMode(flag)
	if err != nil {
		return "", err
	}

	if err := validatePath(path, createMode.isSequential); err != nil {
		return "", err
	}

	if !createMode.isContainer {
		return "", fmt.Errorf("CreateContainer requires container flag: %w", ErrInvalidFlags)
	}

	res := &createResponse{}
	_, err = c.request(opCreateContainer, &CreateRequest{path, data, acl, createMode.flag}, res, nil)
	return res.Path, err
}

// CreateTTL creates a TTL znode, which will be automatically deleted by server after the TTL.
func (c *Conn) CreateTTL(path string, data []byte, flag int32, acl []ACL, ttl time.Duration) (string, error) {
	createMode, err := parseCreateMode(flag)
	if err != nil {
		return "", err
	}

	if err := validatePath(path, createMode.isSequential); err != nil {
		return "", err
	}

	if !createMode.isTTL {
		return "", fmt.Errorf("CreateTTL requires TTL flag: %w", ErrInvalidFlags)
	}

	res := &createResponse{}
	_, err = c.request(opCreateTTL, &CreateTTLRequest{path, data, acl, createMode.flag, ttl.Milliseconds()}, res, nil)
	return res.Path, err
}

// CreateProtectedEphemeralSequential fixes a race condition if the server crashes
// after it creates the node. On reconnect the session may still be valid so the
// ephemeral node still exists. Therefore, on reconnect we need to check if a node
// with a GUID generated on create exists.
func (c *Conn) CreateProtectedEphemeralSequential(path string, data []byte, acl []ACL) (string, error) {
	if err := validatePath(path, true); err != nil {
		return "", err
	}

	var guid [16]byte
	_, err := io.ReadFull(rand.Reader, guid[:16])
	if err != nil {
		return "", err
	}
	guidStr := fmt.Sprintf("%x", guid)

	parts := strings.Split(path, "/")
	parts[len(parts)-1] = fmt.Sprintf("%s%s-%s", protectedPrefix, guidStr, parts[len(parts)-1])
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
				if pth := parts[len(parts)-1]; strings.HasPrefix(pth, protectedPrefix) {
					if g := pth[len(protectedPrefix) : len(protectedPrefix)+32]; g == guidStr {
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
	if err := validatePath(path, false); err != nil {
		return err
	}

	_, err := c.request(opDelete, &DeleteRequest{path, version}, &deleteResponse{}, nil)
	return err
}

// Exists tells the existence of a znode.
func (c *Conn) Exists(path string) (bool, *Stat, error) {
	if err := validatePath(path, false); err != nil {
		return false, nil, err
	}

	res := &existsResponse{}
	_, err := c.request(opExists, &existsRequest{Path: path, Watch: false}, res, nil)
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
	if err := validatePath(path, false); err != nil {
		return false, nil, nil, err
	}

	var ech <-chan Event
	res := &existsResponse{}
	_, err := c.request(opExists, &existsRequest{Path: path, Watch: true}, res, func(req *request, res *responseHeader, err error) {
		if err == nil {
			ech = c.addWatcher(path, watchTypeData)
		} else if err == ErrNoNode {
			ech = c.addWatcher(path, watchTypeExist)
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
	if err := validatePath(path, false); err != nil {
		return nil, nil, err
	}

	res := &getAclResponse{}
	_, err := c.request(opGetAcl, &getAclRequest{Path: path}, res, nil)
	if err == ErrConnectionClosed {
		return nil, nil, err
	}
	return res.Acl, &res.Stat, err
}

// SetACL updates the ACLs of a znode.
func (c *Conn) SetACL(path string, acl []ACL, version int32) (*Stat, error) {
	if err := validatePath(path, false); err != nil {
		return nil, err
	}

	res := &setAclResponse{}
	_, err := c.request(opSetAcl, &setAclRequest{Path: path, Acl: acl, Version: version}, res, nil)
	if err == ErrConnectionClosed {
		return nil, err
	}
	return &res.Stat, err
}

// Sync flushes the channel between process and the leader of a given znode,
// you may need it if you want identical views of ZooKeeper data for 2 client instances.
// Please refer to the "Consistency Guarantees" section of ZK document for more details.
func (c *Conn) Sync(path string) (string, error) {
	if err := validatePath(path, false); err != nil {
		return "", err
	}

	res := &syncResponse{}
	_, err := c.request(opSync, &syncRequest{Path: path}, res, nil)
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
	req := &multiRequest{
		Ops:        make([]multiRequestOp, 0, len(ops)),
		DoneHeader: multiHeader{Type: -1, Done: true, Err: -1},
	}
	for _, op := range ops {
		var opCode int32
		switch op.(type) {
		case *CreateRequest:
			opCode = opCreate
		case *SetDataRequest:
			opCode = opSetData
		case *DeleteRequest:
			opCode = opDelete
		case *CheckVersionRequest:
			opCode = opCheck
		default:
			return nil, fmt.Errorf("unknown operation type %T", op)
		}
		req.Ops = append(req.Ops, multiRequestOp{multiHeader{opCode, false, -1}, op})
	}
	res := &multiResponse{}
	_, err := c.request(opMulti, req, res, nil)
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
	request := &reconfigRequest{
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
	request := &reconfigRequest{
		NewMembers:  []byte(strings.Join(members, ",")),
		CurConfigId: version,
	}

	return c.internalReconfig(request)
}

func (c *Conn) internalReconfig(request *reconfigRequest) (*Stat, error) {
	response := &reconfigReponse{}
	_, err := c.request(opReconfig, request, response, nil)
	return &response.Stat, err
}

// Server returns the current or last-connected server name.
func (c *Conn) Server() string {
	c.serverMu.Lock()
	defer c.serverMu.Unlock()
	return c.server
}

func resendZkAuth(ctx context.Context, c *Conn) error {
	shouldCancel := func() bool {
		select {
		case <-c.shouldQuit:
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
		c.logger.Printf("re-submitting `%d` credentials after reconnect", len(c.creds))
	}

	for _, cred := range c.creds {
		// return early before attempting to send request.
		if shouldCancel() {
			return nil
		}
		// do not use the public API for auth since it depends on the send/recv loops
		// that are waiting for this to return
		resChan, err := c.sendRequest(
			opSetAuth,
			&setAuthRequest{Type: 0,
				Scheme: cred.scheme,
				Auth:   cred.auth,
			},
			&setAuthResponse{},
			nil, /* recvFunc*/
		)
		if err != nil {
			return fmt.Errorf("failed to send auth request: %v", err)
		}

		var res response
		select {
		case res = <-resChan:
		case <-c.closeChan:
			c.logger.Printf("recv closed, cancel re-submitting credentials")
			return nil
		case <-c.shouldQuit:
			c.logger.Printf("should quit, cancel re-submitting credentials")
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
		if res.err != nil {
			return fmt.Errorf("failed connection setAuth request: %v", res.err)
		}
	}

	return nil
}
package zk

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// FLWSrvr is a FourLetterWord helper function. In particular, this function pulls the srvr output
// from the zookeeper instances and parses the output. A slice of *ServerStats structs are returned
// as well as a boolean value to indicate whether this function processed successfully.
//
// If the boolean value is false there was a problem. If the *ServerStats slice is empty or nil,
// then the error happened before we started to obtain 'srvr' values. Otherwise, one of the
// servers had an issue and the "Error" value in the struct should be inspected to determine
// which server had the issue.
func FLWSrvr(servers []string, timeout time.Duration) ([]*ServerStats, bool) {
	// different parts of the regular expression that are required to parse the srvr output
	const (
		zrVer   = `^Zookeeper version: ([A-Za-z0-9\.\-]+), built on (\d\d/\d\d/\d\d\d\d \d\d:\d\d [A-Za-z0-9:\+\-]+)`
		zrLat   = `^Latency min/avg/max: (\d+)/([0-9.]+)/(\d+)`
		zrNet   = `^Received: (\d+).*\n^Sent: (\d+).*\n^Connections: (\d+).*\n^Outstanding: (\d+)`
		zrState = `^Zxid: (0x[A-Za-z0-9]+).*\n^Mode: (\w+).*\n^Node count: (\d+)`
	)

	// build the regex from the pieces above
	re, err := regexp.Compile(fmt.Sprintf(`(?m:\A%v.*\n%v.*\n%v.*\n%v)`, zrVer, zrLat, zrNet, zrState))
	if err != nil {
		return nil, false
	}

	imOk := true
	servers = FormatServers(servers)
	ss := make([]*ServerStats, len(servers))

	for i := range ss {
		response, err := fourLetterWord(servers[i], "srvr", timeout)

		if err != nil {
			ss[i] = &ServerStats{Server: servers[i], Error: err}
			imOk = false
			continue
		}

		matches := re.FindAllStringSubmatch(string(response), -1)

		if matches == nil {
			err := fmt.Errorf("unable to parse fields from zookeeper response (no regex matches)")
			ss[i] = &ServerStats{Server: servers[i], Error: err}
			imOk = false
			continue
		}

		match := matches[0][1:]

		// determine current server
		var srvrMode Mode
		switch match[10] {
		case "leader":
			srvrMode = ModeLeader
		case "follower":
			srvrMode = ModeFollower
		case "standalone":
			srvrMode = ModeStandalone
		default:
			srvrMode = ModeUnknown
		}

		buildTime, err := time.Parse("01/02/2006 15:04 MST", match[1])

		if err != nil {
			ss[i] = &ServerStats{Server: servers[i], Error: err}
			imOk = false
			continue
		}

		parsedInt, err := strconv.ParseInt(match[9], 0, 64)

		if err != nil {
			ss[i] = &ServerStats{Server: servers[i], Error: err}
			imOk = false
			continue
		}

		// the ZxID value is an int64 with two int32s packed inside
		// the high int32 is the epoch (i.e., number of leader elections)
		// the low int32 is the counter
		epoch := int32(parsedInt >> 32)
		counter := int32(parsedInt & 0xFFFFFFFF)

		// within the regex above, these values must be numerical
		// so we can avoid useless checking of the error return value
		minLatency, _ := strconv.ParseInt(match[2], 0, 64)
		avgLatency, _ := strconv.ParseFloat(match[3], 64)
		maxLatency, _ := strconv.ParseInt(match[4], 0, 64)
		recv, _ := strconv.ParseInt(match[5], 0, 64)
		sent, _ := strconv.ParseInt(match[6], 0, 64)
		cons, _ := strconv.ParseInt(match[7], 0, 64)
		outs, _ := strconv.ParseInt(match[8], 0, 64)
		ncnt, _ := strconv.ParseInt(match[11], 0, 64)

		ss[i] = &ServerStats{
			Server:      servers[i],
			Sent:        sent,
			Received:    recv,
			NodeCount:   ncnt,
			MinLatency:  minLatency,
			AvgLatency:  avgLatency,
			MaxLatency:  maxLatency,
			Connections: cons,
			Outstanding: outs,
			Epoch:       epoch,
			Counter:     counter,
			BuildTime:   buildTime,
			Mode:        srvrMode,
			Version:     match[0],
		}
	}

	return ss, imOk
}

// FLWRuok is a FourLetterWord helper function. In particular, this function
// pulls the ruok output from each server.
func FLWRuok(servers []string, timeout time.Duration) []bool {
	servers = FormatServers(servers)
	oks := make([]bool, len(servers))

	for i := range oks {
		response, err := fourLetterWord(servers[i], "ruok", timeout)

		if err != nil {
			continue
		}

		if string(response[:4]) == "imok" {
			oks[i] = true
		}
	}
	return oks
}

// FLWCons is a FourLetterWord helper function. In particular, this function
// pulls the ruok output from each server.
//
// As with FLWSrvr, the boolean value indicates whether one of the requests had
// an issue. The Clients struct has an Error value that can be checked.
func FLWCons(servers []string, timeout time.Duration) ([]*ServerClients, bool) {
	const (
		zrAddr = `^ /((?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?):(?:\d+))\[\d+\]`
		zrPac  = `\(queued=(\d+),recved=(\d+),sent=(\d+),sid=(0x[A-Za-z0-9]+),lop=(\w+),est=(\d+),to=(\d+),`
		zrSesh = `lcxid=(0x[A-Za-z0-9]+),lzxid=(0x[A-Za-z0-9]+),lresp=(\d+),llat=(\d+),minlat=(\d+),avglat=(\d+),maxlat=(\d+)\)`
	)

	re, err := regexp.Compile(fmt.Sprintf("%v%v%v", zrAddr, zrPac, zrSesh))
	if err != nil {
		return nil, false
	}

	servers = FormatServers(servers)
	sc := make([]*ServerClients, len(servers))
	imOk := true

	for i := range sc {
		response, err := fourLetterWord(servers[i], "cons", timeout)

		if err != nil {
			sc[i] = &ServerClients{Error: err}
			imOk = false
			continue
		}

		scan := bufio.NewScanner(bytes.NewReader(response))

		var clients []*ServerClient

		for scan.Scan() {
			line := scan.Bytes()

			if len(line) == 0 {
				continue
			}

			m := re.FindAllStringSubmatch(string(line), -1)

			if m == nil {
				err := fmt.Errorf("unable to parse fields from zookeeper response (no regex matches)")
				sc[i] = &ServerClients{Error: err}
				imOk = false
				continue
			}

			match := m[0][1:]

			queued, _ := strconv.ParseInt(match[1], 0, 64)
			recvd, _ := strconv.ParseInt(match[2], 0, 64)
			sent, _ := strconv.ParseInt(match[3], 0, 64)
			sid, _ := strconv.ParseInt(match[4], 0, 64)
			est, _ := strconv.ParseInt(match[6], 0, 64)
			timeout, _ := strconv.ParseInt(match[7], 0, 32)
			lcxid, _ := parseInt64(match[8])
			lzxid, _ := parseInt64(match[9])
			lresp, _ := strconv.ParseInt(match[10], 0, 64)
			llat, _ := strconv.ParseInt(match[11], 0, 32)
			minlat, _ := strconv.ParseInt(match[12], 0, 32)
			avglat, _ := strconv.ParseInt(match[13], 0, 32)
			maxlat, _ := strconv.ParseInt(match[14], 0, 32)

			clients = append(clients, &ServerClient{
				Queued:        queued,
				Received:      recvd,
				Sent:          sent,
				SessionID:     sid,
				Lcxid:         int64(lcxid),
				Lzxid:         int64(lzxid),
				Timeout:       int32(timeout),
				LastLatency:   int32(llat),
				MinLatency:    int32(minlat),
				AvgLatency:    int32(avglat),
				MaxLatency:    int32(maxlat),
				Established:   time.Unix(est, 0),
				LastResponse:  time.Unix(lresp, 0),
				Addr:          match[0],
				LastOperation: match[5],
			})
		}

		sc[i] = &ServerClients{Clients: clients}
	}

	return sc, imOk
}

// parseInt64 is similar to strconv.ParseInt, but it also handles hex values that represent negative numbers
func parseInt64(s string) (int64, error) {
	if strings.HasPrefix(s, "0x") {
		i, err := strconv.ParseUint(s, 0, 64)
		return int64(i), err
	}
	return strconv.ParseInt(s, 0, 64)
}

func fourLetterWord(server, command string, timeout time.Duration) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", server, timeout)
	if err != nil {
		return nil, err
	}

	// the zookeeper server should automatically close this socket
	// once the command has been processed, but better safe than sorry
	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(timeout))
	_, err = conn.Write([]byte(command))
	if err != nil {
		return nil, err
	}

	conn.SetReadDeadline(time.Now().Add(timeout))
	return ioutil.ReadAll(conn)
}
package zk

import "fmt"

// TODO: (v2) enum type for CreateMode API.
const (
	FlagPersistent                  = 0
	FlagEphemeral                   = 1
	FlagSequence                    = 2
	FlagEphemeralSequential         = 3
	FlagContainer                   = 4
	FlagTTL                         = 5
	FlagPersistentSequentialWithTTL = 6
)

type createMode struct {
	flag         int32
	isEphemeral  bool
	isSequential bool
	isContainer  bool
	isTTL        bool
}

// parsing a flag integer into the CreateMode needed to call the correct
// Create RPC to Zookeeper.
//
// NOTE: This parse method is designed to be able to copy and paste the same
// CreateMode ENUM constructors from Java:
// https://github.com/apache/zookeeper/blob/master/zookeeper-server/src/main/java/org/apache/zookeeper/CreateMode.java
func parseCreateMode(flag int32) (createMode, error) {
	switch flag {
	case FlagPersistent:
		return createMode{0, false, false, false, false}, nil
	case FlagEphemeral:
		return createMode{1, true, false, false, false}, nil
	case FlagSequence:
		return createMode{2, false, true, false, false}, nil
	case FlagEphemeralSequential:
		return createMode{3, true, true, false, false}, nil
	case FlagContainer:
		return createMode{4, false, false, true, false}, nil
	case FlagTTL:
		return createMode{5, false, false, false, true}, nil
	case FlagPersistentSequentialWithTTL:
		return createMode{6, false, true, false, true}, nil
	default:
		return createMode{}, fmt.Errorf("invalid flag value: [%v]", flag)
	}
}
