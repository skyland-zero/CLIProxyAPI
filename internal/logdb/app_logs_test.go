package logdb

import (
	"testing"

	log "github.com/sirupsen/logrus"
)

func TestAppLogHookLevelsIncludeInfo(t *testing.T) {
	hook := NewAppLogHook()
	levels := hook.Levels()

	for _, want := range []log.Level{log.InfoLevel, log.WarnLevel, log.ErrorLevel, log.FatalLevel, log.PanicLevel} {
		found := false
		for _, level := range levels {
			if level == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected hook levels to include %s", want)
		}
	}
}
