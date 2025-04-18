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
	OpNotify          = 0
	OpCreate          = 1
	OpDelete          = 2
	OpExists          = 3
	OpGetData         = 4
	OpSetData         = 5
	OpGetAcl          = 6
	OpSetAcl          = 7
	OpGetChildren     = 8
	OpSync            = 9
	OpPing            = 11
	OpGetChildren2    = 12
	OpCheck           = 13
	OpMulti           = 14
	OpReconfig        = 16
	OpCreateContainer = 19
	OpCreateTTL       = 21
	OpClose           = -11
	OpSetAuth         = 100
	OpSetWatches      = 101
	OpError           = -1
	// Not in protocol, used internally
	OpWatcherEvent = -2
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
	OpNames       = map[int32]string{
		OpNotify:          "notify",
		OpCreate:          "create",
		OpCreateContainer: "createContainer",
		OpCreateTTL:       "createTTL",
		OpDelete:          "delete",
		OpExists:          "exists",
		OpGetData:         "getData",
		OpSetData:         "setData",
		OpGetAcl:          "getACL",
		OpSetAcl:          "setACL",
		OpGetChildren:     "getChildren",
		OpSync:            "sync",
		OpPing:            "ping",
		OpGetChildren2:    "getChildren2",
		OpCheck:           "check",
		OpMulti:           "multi",
		OpReconfig:        "reconfig",
		OpClose:           "close",
		OpSetAuth:         "setAuth",
		OpSetWatches:      "setWatches",

		OpWatcherEvent: "watcherEvent",
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
