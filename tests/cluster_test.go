package tests

import (
	"github.com/NurramoX/gozookeeper/zk"
	"sync"
	"testing"
	"time"
)

type logWriter struct {
	t *testing.T
	p string
}

func (lw logWriter) Write(b []byte) (int, error) {
	lw.t.Logf("%s%s", lw.p, string(b))
	return len(b), nil
}

func TestIntegration_BasicCluster(t *testing.T) {
	ts, err := StartTestCluster(t, 3, nil, logWriter{t: t, p: "[ZKERR] "})
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Stop()
	zk1, _, err := ts.Connect(0)
	if err != nil {
		t.Fatalf("Connect returned error: %+v", err)
	}
	defer zk1.Close()
	zk2, _, err := ts.Connect(1)
	if err != nil {
		t.Fatalf("Connect returned error: %+v", err)
	}
	defer zk2.Close()

	if _, err := zk1.Create("/gozk-test", []byte("foo-cluster"), 0, zk.WorldACL(zk.PermAll)); err != nil {
		t.Fatalf("Create failed on node 1: %+v", err)
	}

	if _, err := zk2.Sync("/gozk-test"); err != nil {
		t.Fatalf("Sync failed on node 2: %+v", err)
	}

	if by, _, err := zk2.Get("/gozk-test"); err != nil {
		t.Fatalf("Get failed on node 2: %+v", err)
	} else if string(by) != "foo-cluster" {
		t.Fatal("Wrong data for node 2")
	}
}

// If the current leader dies, then the session is reestablished with the new one.
func TestIntegration_ClientClusterFailover(t *testing.T) {
	tc, err := StartTestCluster(t, 3, nil, logWriter{t: t, p: "[ZKERR] "})
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Stop()
	zookeeper, evCh, err := tc.ConnectAll()
	if err != nil {
		t.Fatalf("Connect returned error: %+v", err)
	}
	defer zookeeper.Close()

	sl := NewStateLogger(evCh)

	hasSessionEvent1 := sl.NewWatcher(sessionStateMatcher(zk.StateHasSession)).Wait(8 * time.Second)
	if hasSessionEvent1 == nil {
		t.Fatalf("Failed to connect and get session")
	}

	if _, err := zookeeper.Create("/gozk-test", []byte("foo-cluster"), 0, zk.WorldACL(zk.PermAll)); err != nil {
		t.Fatalf("Create failed on node 1: %+v", err)
	}

	hasSessionWatcher2 := sl.NewWatcher(sessionStateMatcher(zk.StateHasSession))

	// Kill the current leader
	tc.StopServer(hasSessionEvent1.Server)

	// Wait for the session to be reconnected with the new leader.
	if hasSessionWatcher2.Wait(8*time.Second) == nil {
		t.Fatalf("Failover failed")
	}

	if by, _, err := zookeeper.Get("/gozk-test"); err != nil {
		t.Fatalf("Get failed on node 2: %+v", err)
	} else if string(by) != "foo-cluster" {
		t.Fatal("Wrong data for node 2")
	}
}

// If a ZooKeeper cluster looses quorum then a session is reconnected as soon
// as the quorum is restored.
func TestIntegration_NoQuorum(t *testing.T) {
	tc, err := StartTestCluster(t, 3, nil, logWriter{t: t, p: "[ZKERR] "})
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Stop()
	zookeeper, evCh, err := tc.ConnectAllTimeout(4 * time.Second)
	if err != nil {
		t.Fatalf("Connect returned error: %+v", err)
	}
	defer zookeeper.Close()
	sl := NewStateLogger(evCh)

	// Wait for initial session to be established
	hasSessionEvent1 := sl.NewWatcher(sessionStateMatcher(zk.StateHasSession)).Wait(8 * time.Second)
	if hasSessionEvent1 == nil {
		t.Fatalf("Failed to connect and get session")
	}
	initialSessionID := zookeeper.SessionID()
	zk.DefaultLogger.Printf("    Session established: id=%d, timeout=%d", zookeeper.SessionID(), zookeeper.SessionTimeoutMs())

	// Kill the ZooKeeper leader and wait for the session to reconnect.
	zk.DefaultLogger.Printf("    Kill the leader")
	disconnectWatcher1 := sl.NewWatcher(sessionStateMatcher(zk.StateDisconnected))
	hasSessionWatcher2 := sl.NewWatcher(sessionStateMatcher(zk.StateHasSession))
	tc.StopServer(hasSessionEvent1.Server)

	disconnectedEvent1 := disconnectWatcher1.Wait(8 * time.Second)
	if disconnectedEvent1 == nil {
		t.Fatalf("Failover failed, missed StateDisconnected event")
	}
	if disconnectedEvent1.Server != hasSessionEvent1.Server {
		t.Fatalf("Unexpected StateDisconnected event, expected=%s, actual=%s",
			hasSessionEvent1.Server, disconnectedEvent1.Server)
	}

	hasSessionEvent2 := hasSessionWatcher2.Wait(8 * time.Second)
	if hasSessionEvent2 == nil {
		t.Fatalf("Failover failed, missed StateHasSession event")
	}

	// Kill the ZooKeeper leader leaving the cluster without quorum.
	zk.DefaultLogger.Printf("    Kill the leader")
	disconnectWatcher2 := sl.NewWatcher(sessionStateMatcher(zk.StateDisconnected))
	tc.StopServer(hasSessionEvent2.Server)

	disconnectedEvent2 := disconnectWatcher2.Wait(8 * time.Second)
	if disconnectedEvent2 == nil {
		t.Fatalf("Failover failed, missed StateDisconnected event")
	}
	if disconnectedEvent2.Server != hasSessionEvent2.Server {
		t.Fatalf("Unexpected StateDisconnected event, expected=%s, actual=%s",
			hasSessionEvent2.Server, disconnectedEvent2.Server)
	}

	// Make sure that we keep retrying connecting to the only remaining
	// ZooKeeper Server, but the attempts are being dropped because there is
	// no quorum.
	zk.DefaultLogger.Printf("    Retrying no luck...")
	var firstDisconnect *zk.Event
	begin := time.Now()
	for time.Now().Sub(begin) < 6*time.Second {
		disconnectedEvent := sl.NewWatcher(sessionStateMatcher(zk.StateDisconnected)).Wait(4 * time.Second)
		if disconnectedEvent == nil {
			t.Fatalf("Disconnected event expected")
		}
		if firstDisconnect == nil {
			firstDisconnect = disconnectedEvent
			continue
		}
		if disconnectedEvent.Server != firstDisconnect.Server {
			t.Fatalf("Disconnect from wrong Server: expected=%s, actual=%s",
				firstDisconnect.Server, disconnectedEvent.Server)
		}
	}

	// Start a ZooKeeper node to restore quorum.
	hasSessionWatcher3 := sl.NewWatcher(sessionStateMatcher(zk.StateHasSession))
	tc.StartServer(hasSessionEvent1.Server)

	// Make sure that session is reconnected with the same ID.
	hasSessionEvent3 := hasSessionWatcher3.Wait(8 * time.Second)
	if hasSessionEvent3 == nil {
		t.Fatalf("Session has not been reconnected")
	}
	if zookeeper.SessionID() != initialSessionID {
		t.Fatalf("Wrong session ID: expected=%d, actual=%d", initialSessionID, zookeeper.SessionID())
	}

	// Make sure that the session is not dropped soon after reconnect
	e := sl.NewWatcher(sessionStateMatcher(zk.StateDisconnected)).Wait(6 * time.Second)
	if e != nil {
		t.Fatalf("Unexpected disconnect")
	}
}

func TestIntegration_WaitForClose(t *testing.T) {
	ts, err := StartTestCluster(t, 1, nil, logWriter{t: t, p: "[ZKERR] "})
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Stop()
	zookeeper, _, err := ts.Connect(0)
	if err != nil {
		t.Fatalf("Connect returned error: %+v", err)
	}
	timeout := time.After(30 * time.Second)
CONNECTED:
	for {
		select {
		case ev := <-zookeeper.EventChan():
			if ev.State == zk.StateConnected {
				break CONNECTED
			}
		case <-timeout:
			zookeeper.Close()
			t.Fatal("Timeout")
		}
	}
	zookeeper.Close()
	for {
		select {
		case _, ok := <-zookeeper.EventChan():
			if !ok {
				return
			}
		case <-timeout:
			t.Fatal("Timeout")
		}
	}
}

func TestIntegration_BadSession(t *testing.T) {
	ts, err := StartTestCluster(t, 1, nil, logWriter{t: t, p: "[ZKERR] "})
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Stop()
	zookeeper, _, err := ts.ConnectAll()
	if err != nil {
		t.Fatalf("Connect returned error: %+v", err)
	}
	defer zookeeper.Close()

	if err := zookeeper.Delete("/gozk-test", -1); err != nil && err != zk.ErrNoNode {
		t.Fatalf("Delete returned error: %+v", err)
	}

	zookeeper.Conn().Close()
	time.Sleep(time.Millisecond * 100)

	if err := zookeeper.Delete("/gozk-test", -1); err != nil && err != zk.ErrNoNode {
		t.Fatalf("Delete returned error: %+v", err)
	}
}

type EventLogger struct {
	events   []zk.Event
	watchers []*EventWatcher
	lock     sync.Mutex
	wg       sync.WaitGroup
}

func NewStateLogger(eventCh <-chan zk.Event) *EventLogger {
	el := &EventLogger{}
	el.wg.Add(1)
	go func() {
		defer el.wg.Done()
		for event := range eventCh {
			el.lock.Lock()
			for _, sw := range el.watchers {
				if !sw.triggered && sw.matcher(event) {
					sw.triggered = true
					sw.matchCh <- event
				}
			}
			zk.DefaultLogger.Printf("    event received: %v\n", event)
			el.events = append(el.events, event)
			el.lock.Unlock()
		}
	}()
	return el
}

func (el *EventLogger) NewWatcher(matcher func(zk.Event) bool) *EventWatcher {
	ew := &EventWatcher{matcher: matcher, matchCh: make(chan zk.Event, 1)}
	el.lock.Lock()
	el.watchers = append(el.watchers, ew)
	el.lock.Unlock()
	return ew
}

func (el *EventLogger) Events() []zk.Event {
	el.lock.Lock()
	transitions := make([]zk.Event, len(el.events))
	copy(transitions, el.events)
	el.lock.Unlock()
	return transitions
}

func (el *EventLogger) Wait4Stop() {
	el.wg.Wait()
}

type EventWatcher struct {
	matcher   func(zk.Event) bool
	matchCh   chan zk.Event
	triggered bool
}

func (ew *EventWatcher) Wait(timeout time.Duration) *zk.Event {
	select {
	case event := <-ew.matchCh:
		return &event
	case <-time.After(timeout):
		return nil
	}
}

func sessionStateMatcher(s zk.State) func(zk.Event) bool {
	return func(e zk.Event) bool {
		return e.Type == zk.EventSession && e.State == s
	}
}
