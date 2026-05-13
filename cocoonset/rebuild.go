package cocoonset

import (
	"encoding/json"
	"time"

	cocoonv1 "github.com/cocoonstack/cocoon-common/apis/v1"
)

const (
	annotationRebuildHistory = "cocoonset.cocoonstack.io/rebuild-history"
	annotationDeadLetter     = "cocoonset.cocoonstack.io/dead-letter"

	maxRebuildAttempts = 3
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
	return m
}

func writeRebuildHistory(cs *cocoonv1.CocoonSet, m map[int32]rebuildEntry) error {
	// Garbage-collect entries for slots no longer in the spec.
	for slot := range m {
		if slot > cs.Spec.Agent.Replicas {
			delete(m, slot)
		}
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if cs.Annotations == nil {
		cs.Annotations = map[string]string{}
	}
	cs.Annotations[annotationRebuildHistory] = string(raw)
	return nil
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
