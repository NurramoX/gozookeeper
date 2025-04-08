package tests

import (
	"errors"
	"github.com/NurramoX/gozookeeper/zk"
	"reflect"
	"testing"
)

func TestEncodeDecodePacket(t *testing.T) {
	t.Parallel()
	encodeDecodeTest(t, &zk.RequestHeader{Xid: -2, Opcode: 5})
	encodeDecodeTest(t, &zk.ConnectResponse{ProtocolVersion: 1, TimeOut: 2, SessionID: 3})
	encodeDecodeTest(t, &zk.ConnectResponse{ProtocolVersion: 1, TimeOut: 2, SessionID: 3, Passwd: []byte{4, 5, 6}})
	encodeDecodeTest(t, &zk.GetAclResponse{Acl: []zk.ACL{{12, "s", "anyone"}}})
	encodeDecodeTest(t, &zk.GetChildrenResponse{Children: []string{"foo", "bar"}})
	encodeDecodeTest(t, &zk.PathWatchRequest{Path: "path", Watch: true})
	encodeDecodeTest(t, &zk.PathWatchRequest{Path: "path"})
	encodeDecodeTest(t, &zk.CheckVersionRequest{Path: "/", Version: -1})
	encodeDecodeTest(t, &zk.ReconfigRequest{CurConfigId: -1})
	encodeDecodeTest(t, &zk.MultiRequest{Ops: []zk.MultiRequestOp{{Header: zk.MultiHeader{Type: zk.OpCheck, Err: -1}, Op: &zk.CheckVersionRequest{Path: "/", Version: -1}}}})
}

func TestRequestStructForOp(t *testing.T) {
	for op, name := range zk.OpNames {
		if op != zk.OpNotify && op != zk.OpWatcherEvent {
			if s := zk.RequestStructForOp(op); s == nil {
				t.Errorf("No struct for op %s", name)
			}
		}
	}
}

func encodeDecodeTest(t *testing.T, r interface{}) {
	buf := make([]byte, 1024)
	n, err := zk.EncodePacket(buf, r)
	if err != nil {
		t.Errorf("encodePacket returned non-nil error %+v\n", err)
		return
	}
	t.Logf("%+v %x", r, buf[:n])
	r2 := reflect.New(reflect.ValueOf(r).Elem().Type()).Interface()
	n2, err := zk.DecodePacket(buf[:n], r2)
	if err != nil {
		t.Errorf("decodePacket returned non-nil error %+v\n", err)
		return
	}
	if n != n2 {
		t.Errorf("sizes don't match: %d != %d", n, n2)
		return
	}
	if !reflect.DeepEqual(r, r2) {
		t.Errorf("results don't match: %+v != %+v", r, r2)
		return
	}
}

func TestEncodeShortBuffer(t *testing.T) {
	t.Parallel()
	_, err := zk.EncodePacket([]byte{}, &zk.RequestHeader{Xid: 1, Opcode: 2})
	if !errors.Is(err, zk.ErrShortBuffer) {
		t.Errorf("encodePacket should return ErrShortBuffer on a short buffer instead of '%+v'", err)
		return
	}
}

func TestDecodeShortBuffer(t *testing.T) {
	t.Parallel()
	_, err := zk.DecodePacket([]byte{}, &zk.ResponseHeader{})
	if !errors.Is(err, zk.ErrShortBuffer) {
		t.Errorf("decodePacket should return ErrShortBuffer on a short buffer instead of '%+v'", err)
		return
	}
}

func BenchmarkEncode(b *testing.B) {
	buf := make([]byte, 4096)
	st := &zk.ConnectRequest{Passwd: []byte("1234567890")}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := zk.EncodePacket(buf, st); err != nil {
			b.Fatal(err)
		}
	}
}
