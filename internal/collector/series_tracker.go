package collector

import "sync"

type seriesTracker[K comparable] struct {
	mu       sync.Mutex
	previous map[string]map[K]struct{}
}

func (s *seriesTracker[K]) Prune(node string, current map[K]struct{}, deleteFn func(K)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.previous == nil {
		s.previous = make(map[string]map[K]struct{})
	}

	previous := s.previous[node]
	for key := range current {
		delete(previous, key)
	}
	for key := range previous {
		deleteFn(key)
	}

	s.previous[node] = current
}
