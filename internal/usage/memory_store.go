package usage

import (
	"context"
	"sync"
	"time"
)

// MemoryStore keeps recent usage events in process memory only.
type MemoryStore struct {
	mu     sync.RWMutex
	events []Event
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

func (s *MemoryStore) Mode() string { return "memory" }

func (s *MemoryStore) Record(_ context.Context, event Event) error {
	if s == nil {
		return nil
	}
	now := nowUTC()
	if event.Timestamp.IsZero() {
		event.Timestamp = now
	}
	event.Timestamp = event.Timestamp.UTC()
	if event.Timestamp.Before(retentionCutoff(now)) {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(now)
	s.events = append(s.events, event)
	return nil
}

func (s *MemoryStore) Snapshot(_ context.Context, query Query) (StatisticsSnapshot, error) {
	events := s.filteredEvents(query, false)
	return buildSnapshot(events), nil
}

func (s *MemoryStore) Events(_ context.Context, query Query) ([]Event, int64, error) {
	events := s.filteredEvents(query, true)
	total := int64(len(events))
	query = clampQueryToRetention(query, nowUTC())
	start := query.Offset
	if start >= len(events) {
		return []Event{}, total, nil
	}
	end := start + query.Limit
	if end > len(events) {
		end = len(events)
	}
	return append([]Event(nil), events[start:end]...), total, nil
}

func (s *MemoryStore) Summary(_ context.Context, query SummaryQuery) ([]SummaryRow, error) {
	events := s.filteredEvents(query.Query, false)
	return buildSummary(events, query.GroupBy), nil
}

func (s *MemoryStore) Delete(_ context.Context, query Query) (int64, error) {
	if s == nil {
		return 0, nil
	}
	now := nowUTC()
	query = clampQueryToRetention(query, now)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(now)
	kept := s.events[:0]
	var deleted int64
	for _, event := range s.events {
		if eventMatches(event, query) {
			deleted++
			continue
		}
		kept = append(kept, event)
	}
	s.events = kept
	return deleted, nil
}

func (s *MemoryStore) filteredEvents(query Query, paged bool) []Event {
	if s == nil {
		return nil
	}
	now := nowUTC()
	query = clampQueryToRetention(query, now)
	s.mu.Lock()
	s.pruneLocked(now)
	snapshot := append([]Event(nil), s.events...)
	s.mu.Unlock()
	filtered := make([]Event, 0, len(snapshot))
	for _, event := range snapshot {
		if eventMatches(event, query) {
			filtered = append(filtered, event)
		}
	}
	if paged {
		sortEventsDesc(filtered)
	}
	return filtered
}

func (s *MemoryStore) pruneLocked(now time.Time) {
	cutoff := retentionCutoff(now)
	kept := s.events[:0]
	for _, event := range s.events {
		if !event.Timestamp.Before(cutoff) {
			kept = append(kept, event)
		}
	}
	s.events = kept
}
