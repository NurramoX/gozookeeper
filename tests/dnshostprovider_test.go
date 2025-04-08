package tests

import (
	"context"
	"fmt"
	"github.com/NurramoX/gozookeeper/zk"
	"log"
	"testing"
	"time"
)

type LookupHostOption struct {
	lookupFn zk.LookupHostFn
}

func withLookupHost(lookupFn zk.LookupHostFn) zk.DNSHostProviderOption {
	return LookupHostOption{
		lookupFn: lookupFn,
	}
}

func (o LookupHostOption) Apply(provider *zk.DNSHostProvider) {
	provider.LookupHost = o.lookupFn
}

// TestDNSHostProviderCreate is just like TestCreate, but with an
// overridden HostProvider that ignores the provided hostname.
func TestIntegration_DNSHostProviderCreate(t *testing.T) {
	ts, err := StartTestCluster(t, 1, nil, logWriter{t: t, p: "[ZKERR] "})
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Stop()

	port := ts.Servers[0].Port
	server := fmt.Sprintf("foo.example.com:%d", port)
	hostProvider := zk.NewDNSHostProvider(
		withLookupHost(func(ctx context.Context, host string) ([]string, error) {
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("No lookup context deadline set")
			}
			return []string{"127.0.0.1"}, nil
		}),
	)

	zookeeper, _, err := zk.Connect([]string{server}, time.Second*15, zk.WithHostProvider(hostProvider))
	if err != nil {
		t.Fatalf("Connect returned error: %+v", err)
	}
	defer zookeeper.Close()

	path := "/gozk-test"

	if err := zookeeper.Delete(path, -1); err != nil && err != zk.ErrNoNode {
		t.Fatalf("Delete returned error: %+v", err)
	}
	if p, err := zookeeper.Create(path, []byte{1, 2, 3, 4}, 0, zk.WorldACL(zk.PermAll)); err != nil {
		t.Fatalf("Create returned error: %+v", err)
	} else if p != path {
		t.Fatalf("Create returned different path '%s' != '%s'", p, path)
	}
	if data, stat, err := zookeeper.Get(path); err != nil {
		t.Fatalf("Get returned error: %+v", err)
	} else if stat == nil {
		t.Fatal("Get returned nil stat")
	} else if len(data) < 4 {
		t.Fatal("Get returned wrong size data")
	}
}

// localHostPortsFacade wraps a HostProvider, remapping the
// address/port combinations it returns to "localhost:$PORT" where
// $PORT is chosen from the provided ports.
type localHostPortsFacade struct {
	inner    zk.HostProvider   // The wrapped HostProvider
	ports    []int             // The provided list of ports
	nextPort int               // The next port to use
	mapped   map[string]string // Already-mapped address/port combinations
}

func newLocalHostPortsFacade(inner zk.HostProvider, ports []int) *localHostPortsFacade {
	return &localHostPortsFacade{
		inner:  inner,
		ports:  ports,
		mapped: make(map[string]string),
	}
}

func (lhpf *localHostPortsFacade) Len() int                    { return lhpf.inner.Len() }
func (lhpf *localHostPortsFacade) Connected()                  { lhpf.inner.Connected() }
func (lhpf *localHostPortsFacade) Init(servers []string) error { return lhpf.inner.Init(servers) }
func (lhpf *localHostPortsFacade) Next() (string, bool) {
	server, retryStart := lhpf.inner.Next()

	// If we've already set up a mapping for that Server, just return it.
	if localMapping := lhpf.mapped[server]; localMapping != "" {
		return localMapping, retryStart
	}

	if lhpf.nextPort == len(lhpf.ports) {
		log.Fatalf("localHostPortsFacade out of ports to assign to %q; current config: %q", server, lhpf.mapped)
	}

	localMapping := fmt.Sprintf("localhost:%d", lhpf.ports[lhpf.nextPort])
	lhpf.mapped[server] = localMapping
	lhpf.nextPort++
	return localMapping, retryStart
}

var _ zk.HostProvider = &localHostPortsFacade{}

// TestDNSHostProviderReconnect tests that the zk.Conn correctly
// reconnects when the Zookeeper instance it's connected to
// restarts. It wraps the DNSHostProvider in a lightweight facade that
// remaps addresses to localhost:$PORT combinations corresponding to
// the test ZooKeeper instances.
func TestIntegration_DNSHostProviderReconnect(t *testing.T) {
	ts, err := StartTestCluster(t, 3, nil, logWriter{t: t, p: "[ZKERR] "})
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Stop()

	innerHp := zk.NewDNSHostProvider(
		withLookupHost(func(_ context.Context, host string) ([]string, error) {
			return []string{"192.0.2.1", "192.0.2.2", "192.0.2.3"}, nil
		}),
	)
	ports := make([]int, 0, len(ts.Servers))
	for _, server := range ts.Servers {
		ports = append(ports, server.Port)
	}
	hp := newLocalHostPortsFacade(innerHp, ports)

	zookeeper, _, err := zk.Connect([]string{"foo.example.com:12345"}, time.Second, zk.WithHostProvider(hp))
	if err != nil {
		t.Fatalf("Connect returned error: %+v", err)
	}
	defer zookeeper.Close()

	path := "/gozk-test"

	// Initial operation to force connection.
	if err := zookeeper.Delete(path, -1); err != nil && err != zk.ErrNoNode {
		t.Fatalf("Delete returned error: %+v", err)
	}

	// Figure out which Server we're connected to.
	currentServer := zookeeper.Server()
	t.Logf("Connected to %q. Finding test Server index…", currentServer)
	serverIndex := -1
	for i, server := range ts.Servers {
		server := fmt.Sprintf("localhost:%d", server.Port)
		t.Logf("…trying %q", server)
		if currentServer == server {
			serverIndex = i
			t.Logf("…found at index %d", i)
			break
		}
	}
	if serverIndex == -1 {
		t.Fatalf("Cannot determine test Server index.")
	}

	// Restart the connected Server.
	ts.Servers[serverIndex].Srv.Stop()
	ts.Servers[serverIndex].Srv.Start()

	// Continue with the basic TestCreate tests.
	if p, err := zookeeper.Create(path, []byte{1, 2, 3, 4}, 0, zk.WorldACL(zk.PermAll)); err != nil {
		t.Fatalf("Create returned error: %+v", err)
	} else if p != path {
		t.Fatalf("Create returned different path '%s' != '%s'", p, path)
	}
	if data, stat, err := zookeeper.Get(path); err != nil {
		t.Fatalf("Get returned error: %+v", err)
	} else if stat == nil {
		t.Fatal("Get returned nil stat")
	} else if len(data) < 4 {
		t.Fatal("Get returned wrong size data")
	}

	if zookeeper.Server() == currentServer {
		t.Errorf("Still connected to %q after restart.", currentServer)
	}
}

// TestDNSHostProviderRetryStart tests the `retryStart` functionality
// of DNSHostProvider.
// It's also probably the clearest visual explanation of exactly how
// it works.
func TestDNSHostProviderRetryStart(t *testing.T) {
	t.Parallel()

	hp := zk.NewDNSHostProvider(
		withLookupHost(func(_ context.Context, host string) ([]string, error) {
			return []string{"192.0.2.1", "192.0.2.2", "192.0.2.3"}, nil
		}),
	)

	if err := hp.Init([]string{"foo.example.com:12345"}); err != nil {
		t.Fatal(err)
	}

	testdata := []struct {
		retryStartWant bool
		callConnected  bool
	}{
		// Repeated failures.
		{false, false},
		{false, false},
		{false, false},
		{true, false},
		{false, false},
		{false, false},
		{true, true},

		// One success offsets things.
		{false, false},
		{false, true},
		{false, true},

		// Repeated successes.
		{false, true},
		{false, true},
		{false, true},
		{false, true},
		{false, true},

		// And some more failures.
		{false, false},
		{false, false},
		{true, false}, // Looped back to last known good Server: all alternates failed.
		{false, false},
	}

	for i, td := range testdata {
		_, retryStartGot := hp.Next()
		if retryStartGot != td.retryStartWant {
			t.Errorf("%d: retryStart=%v; want %v", i, retryStartGot, td.retryStartWant)
		}
		if td.callConnected {
			hp.Connected()
		}
	}
}

func TestNewDNSHostProvider(t *testing.T) {
	want := 5 * time.Second
	provider := zk.NewDNSHostProvider(zk.WithLookupTimeout(want))
	if provider.LookupTimeout != want {
		t.Fatalf("expected lookup timeout to be %v, got %v", want, provider.LookupTimeout)
	}
}
