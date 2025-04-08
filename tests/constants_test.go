package tests

import (
	"fmt"
	"github.com/NurramoX/gozookeeper/zk"
	"testing"
)

func TestModeString(t *testing.T) {
	if fmt.Sprintf("%v", zk.ModeUnknown) != "unknown" {
		t.Errorf("unknown value should be 'unknown'")
	}

	if fmt.Sprintf("%v", zk.ModeLeader) != "leader" {
		t.Errorf("leader value should be 'leader'")
	}

	if fmt.Sprintf("%v", zk.ModeFollower) != "follower" {
		t.Errorf("follower value should be 'follower'")
	}

	if fmt.Sprintf("%v", zk.ModeStandalone) != "standalone" {
		t.Errorf("standlone value should be 'standalone'")
	}
}
