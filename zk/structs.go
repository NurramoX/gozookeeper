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

type RequestHeader struct {
	Xid    int32
	Opcode int32
}

type ResponseHeader struct {
	Xid  int32
	Zxid int64
	Err  ErrCode
}

type MultiHeader struct {
	Type int32
	Done bool
	Err  ErrCode
}

type Auth struct {
	Type   int32
	Scheme string
	Auth   []byte
}

// Generic Request structs

type PathRequest struct {
	Path string
}

type PathVersionRequest struct {
	Path    string
	Version int32
}

type PathWatchRequest struct {
	Path  string
	Watch bool
}

type PathResponse struct {
	Path string
}

type StatResponse struct {
	Stat Stat
}

//

type CheckVersionRequest PathVersionRequest
type closeRequest struct{}
type closeResponse struct{}

type ConnectRequest struct {
	ProtocolVersion int32
	LastZxidSeen    int64
	TimeOut         int32
	SessionID       int64
	Passwd          []byte
}

type ConnectResponse struct {
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

type createResponse PathResponse
type DeleteRequest PathVersionRequest
type deleteResponse struct{}

type errorResponse struct {
	Err int32
}

type existsRequest PathWatchRequest
type existsResponse StatResponse
type getAclRequest PathRequest

type GetAclResponse struct {
	Acl  []ACL
	Stat Stat
}

type getChildrenRequest PathRequest

type GetChildrenResponse struct {
	Children []string
}

type getChildren2Request PathWatchRequest

type getChildren2Response struct {
	Children []string
	Stat     Stat
}

type getDataRequest PathWatchRequest

type getDataResponse struct {
	Data []byte
	Stat Stat
}

type getMaxChildrenRequest PathRequest

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

type setAclResponse StatResponse

type SetDataRequest struct {
	Path    string
	Data    []byte
	Version int32
}

type setDataResponse StatResponse

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

type SetWatchesRequest struct {
	RelativeZxid int64
	DataWatches  []string
	ExistWatches []string
	ChildWatches []string
}

type setWatchesResponse struct{}

type syncRequest PathRequest
type syncResponse PathResponse

type setAuthRequest Auth
type setAuthResponse struct{}

type MultiRequestOp struct {
	Header MultiHeader
	Op     interface{}
}
type MultiRequest struct {
	Ops        []MultiRequestOp
	DoneHeader MultiHeader
}
type multiResponseOp struct {
	Header MultiHeader
	String string
	Stat   *Stat
	Err    ErrCode
}
type multiResponse struct {
	Ops        []multiResponseOp
	DoneHeader MultiHeader
}

// zk version 3.5 reconfig API
type ReconfigRequest struct {
	JoiningServers []byte
	LeavingServers []byte
	NewMembers     []byte
	// curConfigId version of the current configuration
	// optional - causes reconfiguration to return an error if configuration is no longer current
	CurConfigId int64
}

type reconfigReponse getDataResponse

func (r *MultiRequest) Encode(buf []byte) (int, error) {
	total := 0
	for _, op := range r.Ops {
		op.Header.Done = false
		n, err := EncodePacketValue(buf[total:], reflect.ValueOf(op))
		if err != nil {
			return total, err
		}
		total += n
	}
	r.DoneHeader.Done = true
	n, err := EncodePacketValue(buf[total:], reflect.ValueOf(r.DoneHeader))
	if err != nil {
		return total, err
	}
	total += n

	return total, nil
}

func (r *MultiRequest) Decode(buf []byte) (int, error) {
	r.Ops = make([]MultiRequestOp, 0)
	r.DoneHeader = MultiHeader{-1, true, -1}
	total := 0
	for {
		header := &MultiHeader{}
		n, err := DecodePacketValue(buf[total:], reflect.ValueOf(header))
		if err != nil {
			return total, err
		}
		total += n
		if header.Done {
			r.DoneHeader = *header
			break
		}

		req := RequestStructForOp(header.Type)
		if req == nil {
			return total, ErrAPIError
		}
		n, err = DecodePacketValue(buf[total:], reflect.ValueOf(req))
		if err != nil {
			return total, err
		}
		total += n
		r.Ops = append(r.Ops, MultiRequestOp{*header, req})
	}
	return total, nil
}

func (r *multiResponse) Decode(buf []byte) (int, error) {
	var multiErr error

	r.Ops = make([]multiResponseOp, 0)
	r.DoneHeader = MultiHeader{-1, true, -1}
	total := 0
	for {
		header := &MultiHeader{}
		n, err := DecodePacketValue(buf[total:], reflect.ValueOf(header))
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
		case OpError:
			w = reflect.ValueOf(&res.Err)
		case OpCreate:
			w = reflect.ValueOf(&res.String)
		case OpSetData:
			res.Stat = new(Stat)
			w = reflect.ValueOf(res.Stat)
		case OpCheck, OpDelete:
		}
		if w.IsValid() {
			n, err := DecodePacketValue(buf[total:], w)
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

func DecodePacket(buf []byte, st interface{}) (n int, err error) {
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
	return DecodePacketValue(buf, v)
}

func DecodePacketValue(buf []byte, v reflect.Value) (int, error) {
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
				n2, err := DecodePacketValue(buf[n:], field)
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
				n2, err := DecodePacketValue(buf[n:], values.Index(i))
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

func EncodePacket(buf []byte, st interface{}) (n int, err error) {
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
	return EncodePacketValue(buf, v)
}

func EncodePacketValue(buf []byte, v reflect.Value) (int, error) {
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
				n2, err := EncodePacketValue(buf[n:], field)
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
				n2, err := EncodePacketValue(buf[n:], v.Index(i))
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

func RequestStructForOp(op int32) interface{} {
	switch op {
	case OpClose:
		return &closeRequest{}
	case OpCreate, OpCreateContainer:
		return &CreateRequest{}
	case OpCreateTTL:
		return &CreateTTLRequest{}
	case OpDelete:
		return &DeleteRequest{}
	case OpExists:
		return &existsRequest{}
	case OpGetAcl:
		return &getAclRequest{}
	case OpGetChildren:
		return &getChildrenRequest{}
	case OpGetChildren2:
		return &getChildren2Request{}
	case OpGetData:
		return &getDataRequest{}
	case OpPing:
		return &pingRequest{}
	case OpSetAcl:
		return &setAclRequest{}
	case OpSetData:
		return &SetDataRequest{}
	case OpSetWatches:
		return &SetWatchesRequest{}
	case OpSync:
		return &syncRequest{}
	case OpSetAuth:
		return &setAuthRequest{}
	case OpCheck:
		return &CheckVersionRequest{}
	case OpMulti:
		return &MultiRequest{}
	case OpReconfig:
		return &ReconfigRequest{}
	}
	return nil
}
