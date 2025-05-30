package tests

import (
	"github.com/NurramoX/gozookeeper/zk"
	"testing"
	"time"
)

func TestIntegration_Lock(t *testing.T) {
	ts, err := StartTestCluster(t, 1, nil, logWriter{t: t, p: "[ZKERR] "})
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Stop()
	zookeeper, _, err := ts.ConnectAll()
	if err != nil {
		t.Fatalf("Connect returned error: %v", err)
	}
	defer zookeeper.Close()

	acls := zk.WorldACL(zk.PermAll)

	l := zk.NewLock(zookeeper, "/test", acls)
	if err := l.Lock(); err != nil {
		t.Fatal(err)
	}
	if err := l.Unlock(); err != nil {
		t.Fatal(err)
	}

	val := make(chan int, 3)

	if err := l.Lock(); err != nil {
		t.Fatal(err)
	}

	l2 := zk.NewLock(zookeeper, "/test", acls)
	go func() {
		if err := l2.Lock(); err != nil {
			t.Error(err)
			return
		}
		val <- 2
		if err := l2.Unlock(); err != nil {
			t.Error(err)
			return
		}
		val <- 3
	}()
	time.Sleep(time.Millisecond * 100)

	val <- 1
	if err := l.Unlock(); err != nil {
		t.Fatal(err)
	}
	if x := <-val; x != 1 {
		t.Fatalf("Expected 1 instead of %d", x)
	}
	if x := <-val; x != 2 {
		t.Fatalf("Expected 2 instead of %d", x)
	}
	if x := <-val; x != 3 {
		t.Fatalf("Expected 3 instead of %d", x)
	}
}

// This tests creating a lock with a path that's more than 1 node deep (e.g. "/test-multi-level/lock"),
// when a part of that path already exists (i.e. "/test-multi-level" node already exists).
func TestIntegration_MultiLevelLock(t *testing.T) {
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

	acls := zk.WorldACL(zk.PermAll)
	path := "/test-multi-level"
	if p, err := zookeeper.Create(path, []byte{1, 2, 3, 4}, 0, zk.WorldACL(zk.PermAll)); err != nil {
		t.Fatalf("Create returned error: %+v", err)
	} else if p != path {
		t.Fatalf("Create returned different path '%s' != '%s'", p, path)
	}
	l := zk.NewLock(zookeeper, "/test-multi-level/lock", acls)
	defer zookeeper.Delete("/test-multi-level", -1) // Clean up what we've created for this test
	defer zookeeper.Delete("/test-multi-level/lock", -1)
	if err := l.Lock(); err != nil {
		t.Fatal(err)
	}
	if err := l.Unlock(); err != nil {
		t.Fatal(err)
	}
}

func TestParseSeq(t *testing.T) {
	const (
		goLock       = "_c_38553bd6d1d57f710ae70ddcc3d24715-lock-0000000000"
		negativeLock = "_c_38553bd6d1d57f710ae70ddcc3d24715-lock--2147483648"
		pyLock       = "da5719988c244fc793f49ec3aa29b566__lock__0000000003"
	)

	seq, err := zk.ParseSeq(goLock)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 0 {
		t.Fatalf("Expected 0 instead of %d", seq)
	}

	seq, err = zk.ParseSeq(negativeLock)
	if err != nil {
		t.Fatal(err)
	}
	if seq != -2147483648 {
		t.Fatalf("Expected -2147483648 instead of %d", seq)
	}

	seq, err = zk.ParseSeq(pyLock)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 3 {
		t.Fatalf("Expected 3 instead of %d", seq)
	}
}
