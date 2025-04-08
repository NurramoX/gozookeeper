package tests

import (
	"context"
	"fmt"
	"github.com/NurramoX/gozookeeper/zk"
	"io"
	"sync"
	"testing"
	"time"
)

func TestIntegration_RecurringReAuthHang(t *testing.T) {
	zkC, err := StartTestCluster(t, 3, io.Discard, io.Discard)
	if err != nil {
		panic(err)
	}
	defer zkC.Stop()

	conn, evtC, err := zkC.ConnectAll()
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	waitForSession(ctx, evtC)
	// Add auth.
	conn.AddAuth("digest", []byte("test:test"))

	var reauthCloseOnce sync.Once
	reauthSig := make(chan struct{}, 1)
	conn.SetResendZkAuthFn(func(ctx context.Context, c *zk.Conn) error {
		// in current implimentation the reauth might be called more than once based on various conditions
		reauthCloseOnce.Do(func() { close(reauthSig) })
		return zk.ResendZkAuth(ctx, c)
	})

	conn.SetDebugCloseRecvLoop(true)
	currentServer := conn.Server()
	zkC.StopServer(currentServer)
	// wait connect to new zookeeper.
	ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	waitForSession(ctx, evtC)

	select {
	case _, ok := <-reauthSig:
		if !ok {
			return // we closed the channel as expected
		}
		t.Fatal("reauth testing channel should have been closed")
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestConcurrentReadAndClose(t *testing.T) {
	WithListenServer(t, func(server string) {
		conn, _, err := zk.Connect([]string{server}, 15*time.Second)
		if err != nil {
			t.Fatalf("Failed to create Connection %s", err)
		}

		okChan := make(chan struct{})
		var setErr error
		go func() {
			_, setErr = conn.Create("/test-path", []byte("test data"), 0, zk.WorldACL(zk.PermAll))
			close(okChan)
		}()

		go func() {
			time.Sleep(1 * time.Second)
			conn.Close()
		}()

		select {
		case <-okChan:
			if setErr != zk.ErrConnectionClosed {
				t.Fatalf("unexpected error returned from Set %v", setErr)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("apparent deadlock!")
		}
	})
}

func TestDeadlockInClose(t *testing.T) {
	c := &zk.Conn{
		ShouldQuit:     make(chan struct{}),
		ConnectTimeout: 1 * time.Second,
		SendChan:       make(chan *zk.Request, zk.SendChanSize),
		Logger:         zk.DefaultLogger,
	}

	for i := 0; i < zk.SendChanSize; i++ {
		c.SendChan <- &zk.Request{}
	}

	okChan := make(chan struct{})
	go func() {
		c.Close()
		close(okChan)
	}()

	select {
	case <-okChan:
	case <-time.After(3 * time.Second):
		t.Fatal("apparent deadlock!")
	}
}

func TestNotifyWatches(t *testing.T) {
	cases := []struct {
		eType   zk.EventType
		path    string
		watches map[zk.WatchPathType]bool
	}{
		{
			zk.EventNodeCreated, "/",
			map[zk.WatchPathType]bool{
				{"/", zk.WatchTypeExist}: true,
				{"/", zk.WatchTypeChild}: false,
				{"/", zk.WatchTypeData}:  false,
			},
		},
		{
			zk.EventNodeCreated, "/a",
			map[zk.WatchPathType]bool{
				{"/b", zk.WatchTypeExist}: false,
			},
		},
		{
			zk.EventNodeDataChanged, "/",
			map[zk.WatchPathType]bool{
				{"/", zk.WatchTypeExist}: true,
				{"/", zk.WatchTypeData}:  true,
				{"/", zk.WatchTypeChild}: false,
			},
		},
		{
			zk.EventNodeChildrenChanged, "/",
			map[zk.WatchPathType]bool{
				{"/", zk.WatchTypeExist}: false,
				{"/", zk.WatchTypeData}:  false,
				{"/", zk.WatchTypeChild}: true,
			},
		},
		{
			zk.EventNodeDeleted, "/",
			map[zk.WatchPathType]bool{
				{"/", zk.WatchTypeExist}: true,
				{"/", zk.WatchTypeData}:  true,
				{"/", zk.WatchTypeChild}: true,
			},
		},
	}

	conn := &zk.Conn{Watchers: make(map[zk.WatchPathType][]chan zk.Event)}

	for idx, c := range cases {
		t.Run(fmt.Sprintf("#%d %s", idx, c.eType), func(t *testing.T) {
			c := c

			notifications := make([]struct {
				path   string
				notify bool
				ch     <-chan zk.Event
			}, len(c.watches))

			var idx int
			for wpt, expectEvent := range c.watches {
				ch := conn.AddWatcher(wpt.Path, wpt.WType)
				notifications[idx].path = wpt.Path
				notifications[idx].notify = expectEvent
				notifications[idx].ch = ch
				idx++
			}
			ev := zk.Event{Type: c.eType, Path: c.path}
			conn.NotifyWatches(ev)

			for _, res := range notifications {
				select {
				case e := <-res.ch:
					if !res.notify || e.Path != res.path {
						t.Fatal("unexpeted notification received")
					}
				default:
					if res.notify {
						t.Fatal("expected notification not received")
					}
				}
			}
		})
	}
}
