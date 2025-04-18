package tests

import (
	"fmt"
	"github.com/NurramoX/gozookeeper/zk"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	_testConfigName   = "zoo.cfg"
	_testMyIDFileName = "myid"
)

type TestServer struct {
	Port   int
	Path   string
	Srv    *Server
	Config ServerConfigServer
}

type TestCluster struct {
	Path    string
	Config  ServerConfig
	Servers []TestServer
}

// TODO: pull this into its own package to allow for better isolation of integration tests vs. unit
// testing. This should be used on CI systems and local only when needed whereas unit tests should remain
// fast and not rely on external dependencies.
func StartTestCluster(t *testing.T, size int, stdout, stderr io.Writer) (*TestCluster, error) {
	t.Helper()

	if testing.Short() {
		t.Skip("ZK cluster tests skipped in short case.")
	}

	if testing.Verbose() {
		// if testing verbose we just spam the Server logs as many issues with tests will have the ZK Server
		// logs have the cause of the failure in it.
		if stdout == nil {
			stdout = os.Stderr
		} else {
			stdout = io.MultiWriter(stdout, os.Stderr)
		}
	}

	tmpPath := t.TempDir()

	success := false
	startPort := int(rand.Int31n(6000) + 10000)
	cluster := &TestCluster{Path: tmpPath}

	defer func() {
		if !success {
			cluster.Stop()
		}
	}()

	for serverN := 0; serverN < size; serverN++ {
		srvPath := filepath.Join(tmpPath, fmt.Sprintf("srv%d", serverN+1))
		requireNoErrorf(t, os.Mkdir(srvPath, 0700), "failed to make Server path")

		port := startPort + serverN*3
		cfg := ServerConfig{
			ClientPort: port,
			DataDir:    srvPath,
		}

		for i := 0; i < size; i++ {
			serverNConfig := ServerConfigServer{
				ID:                 i + 1,
				Host:               "127.0.0.1",
				PeerPort:           startPort + i*3 + 1,
				LeaderElectionPort: startPort + i*3 + 2,
			}

			cfg.Servers = append(cfg.Servers, serverNConfig)
		}

		cfgPath := filepath.Join(srvPath, _testConfigName)
		fi, err := os.Create(cfgPath)
		requireNoErrorf(t, err)

		requireNoErrorf(t, cfg.Marshall(fi))
		fi.Close()

		fi, err = os.Create(filepath.Join(srvPath, _testMyIDFileName))
		requireNoErrorf(t, err)

		_, err = fmt.Fprintf(fi, "%d\n", serverN+1)
		fi.Close()
		requireNoErrorf(t, err)

		srv, err := NewIntegrationTestServer(t, cfgPath, stdout, stderr)
		requireNoErrorf(t, err)

		if err := srv.Start(); err != nil {
			return nil, err
		}

		cluster.Servers = append(cluster.Servers, TestServer{
			Path:   srvPath,
			Port:   cfg.ClientPort,
			Srv:    srv,
			Config: cfg.Servers[serverN],
		})
		cluster.Config = cfg
	}

	if err := cluster.waitForStart(30, time.Second); err != nil {
		return nil, err
	}

	success = true

	return cluster, nil
}

func (tc *TestCluster) Connect(idx int) (*zk.Conn, <-chan zk.Event, error) {
	return zk.Connect([]string{fmt.Sprintf("127.0.0.1:%d", tc.Servers[idx].Port)}, time.Second*15)
}

func (tc *TestCluster) ConnectAll() (*zk.Conn, <-chan zk.Event, error) {
	return tc.ConnectAllTimeout(time.Second * 15)
}

func (tc *TestCluster) ConnectAllTimeout(sessionTimeout time.Duration) (*zk.Conn, <-chan zk.Event, error) {
	return tc.ConnectWithOptions(sessionTimeout)
}

func (tc *TestCluster) ConnectWithOptions(sessionTimeout time.Duration, options ...zk.ConnOption) (*zk.Conn, <-chan zk.Event, error) {
	hosts := make([]string, len(tc.Servers))
	for i, srv := range tc.Servers {
		hosts[i] = fmt.Sprintf("127.0.0.1:%d", srv.Port)
	}
	zookeeper, ch, err := zk.Connect(hosts, sessionTimeout, options...)
	return zookeeper, ch, err
}

func (tc *TestCluster) Stop() error {
	for _, srv := range tc.Servers {
		srv.Srv.Stop()
	}

	return tc.waitForStop(5, time.Second)
}

// waitForStart blocks until the cluster is up
func (tc *TestCluster) waitForStart(maxRetry int, interval time.Duration) error {
	// verify that the servers are up with SRVR
	serverAddrs := make([]string, len(tc.Servers))
	for i, s := range tc.Servers {
		serverAddrs[i] = fmt.Sprintf("127.0.0.1:%d", s.Port)
	}

	for i := 0; i < maxRetry; i++ {
		_, ok := zk.FLWSrvr(serverAddrs, time.Second)
		if ok {
			return nil
		}
		time.Sleep(interval)
	}

	return fmt.Errorf("unable to verify health of servers")
}

// waitForStop blocks until the cluster is down
func (tc *TestCluster) waitForStop(maxRetry int, interval time.Duration) error {
	// verify that the servers are up with RUOK
	serverAddrs := make([]string, len(tc.Servers))
	for i, s := range tc.Servers {
		serverAddrs[i] = fmt.Sprintf("127.0.0.1:%d", s.Port)
	}

	var success bool
	for i := 0; i < maxRetry && !success; i++ {
		success = true
		for _, ok := range zk.FLWRuok(serverAddrs, time.Second) {
			if ok {
				success = false
			}
		}
		if !success {
			time.Sleep(interval)
		}
	}
	if !success {
		return fmt.Errorf("unable to verify servers are down")
	}
	return nil
}

func (tc *TestCluster) StartServer(server string) {
	for _, s := range tc.Servers {
		if strings.HasSuffix(server, fmt.Sprintf(":%d", s.Port)) {
			s.Srv.Start()
			return
		}
	}
	panic(fmt.Sprintf("unknown Server: %s", server))
}

func (tc *TestCluster) StopServer(server string) {
	for _, s := range tc.Servers {
		if strings.HasSuffix(server, fmt.Sprintf(":%d", s.Port)) {
			s.Srv.Stop()
			return
		}
	}
	panic(fmt.Sprintf("unknown Server: %s", server))
}

func (tc *TestCluster) StartAllServers() error {
	for _, s := range tc.Servers {
		if err := s.Srv.Start(); err != nil {
			return fmt.Errorf("failed to start Server listening on port `%d` : %+v", s.Port, err)
		}
	}

	if err := tc.waitForStart(10, time.Second*2); err != nil {
		return fmt.Errorf("failed to wait to startup zk servers: %v", err)
	}

	return nil
}

func (tc *TestCluster) StopAllServers() error {
	var err error
	for _, s := range tc.Servers {
		if err := s.Srv.Stop(); err != nil {
			err = fmt.Errorf("failed to stop Server listening on port `%d` : %v", s.Port, err)
		}
	}
	if err != nil {
		return err
	}

	if err := tc.waitForStop(5, time.Second); err != nil {
		return fmt.Errorf("failed to wait to startup zk servers: %v", err)
	}

	return nil
}

func requireNoErrorf(t *testing.T, err error, msgAndArgs ...interface{}) {
	t.Helper()

	if err != nil {
		t.Logf("received unexpected error: %v", err)
		t.Fatal(msgAndArgs...)
	}
}
