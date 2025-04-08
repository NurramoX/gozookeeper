package tests

import (
	"github.com/NurramoX/gozookeeper/zk"
	"strings"
	"testing"
)

func TestParseCreateMode(t *testing.T) {
	changeDetectorTests := []struct {
		name         string
		flag         int32
		wantIntValue int32
	}{
		{"valid flag createmode 0 persistant", zk.FlagPersistent, 0},
		{"ephemeral", zk.FlagEphemeral, 1},
		{"sequential", zk.FlagSequence, 2},
		{"ephemeral sequential", zk.FlagEphemeralSequential, 3},
		{"container", zk.FlagContainer, 4},
		{"ttl", zk.FlagTTL, 5},
		{"persistentSequential w/TTL", zk.FlagPersistentSequentialWithTTL, 6},
	}
	for _, tt := range changeDetectorTests {
		t.Run(tt.name, func(t *testing.T) {
			cm, err := zk.ParseCreateMode(tt.flag)
			requireNoErrorf(t, err)
			if cm.Flag != tt.wantIntValue {
				// change detector test for enum values.
				t.Fatalf("createmode value of flag; want: %v, got: %v", cm.Flag, tt.wantIntValue)
			}
		})
	}

	t.Run("failed to parse", func(t *testing.T) {
		cm, err := zk.ParseCreateMode(-123)
		if err == nil {
			t.Fatalf("error expected, got: %v", cm)
		}
		if !strings.Contains(err.Error(), "invalid flag value") {
			t.Fatalf("unexpected error value: %v", err)
		}
	})

}
