package cocoonset

import (
	"encoding/json"
	"maps"
	"time"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
)

const (
	annotationRebuildHistory = "cocoonset.cocoonstack.io/rebuild-history"
	annotationDeadLetter     = "cocoonset.cocoonstack.io/dead-letter"

	maxRebuildAttempts = 4
)

// rebuildEntry tracks how many times triageSubAgent has rebuilt a slot.
// Persisted as a JSON map keyed by slot in the CocoonSet annotation so
// the count survives the pod delete that erases the in-pod annotation.
type rebuildEntry struct {
	Count       int       `json:"count"`
	LastDeleted time.Time `json:"lastDeleted"`
}

func readRebuildHistory(cs *cocoonv1.CocoonSet) map[int32]rebuildEntry {
	raw := cs.Annotations[annotationRebuildHistory]
	if raw == "" {
		return map[int32]rebuildEntry{}
	}
	m := map[int32]rebuildEntry{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return map[int32]rebuildEntry{}
	}
	if m == nil { // json "null" leaves m nil; callers write to it
		return map[int32]rebuildEntry{}
	}
	return m
}

// encodeRebuildHistory garbage-collects entries for slots no longer in the
// spec and returns the JSON payload for the annotation.
func encodeRebuildHistory(replicas int32, m map[int32]rebuildEntry) (string, error) {
	maps.DeleteFunc(m, func(slot int32, _ rebuildEntry) bool {
		return slot > replicas
	})
	raw, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// backoffDelay returns the wait before the next rebuild attempt: 0, 1s, 5s, 30s.
func backoffDelay(priorCount int) time.Duration {
	switch priorCount {
	case 0:
		return 0
	case 1:
		return 1 * time.Second
	case 2:
		return 5 * time.Second
	default:
		return 30 * time.Second
	}
}
