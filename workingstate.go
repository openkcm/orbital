package orbital

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// WorkingState represents the working state of a task.
// It provides methods for storing arbitrary key-value pairs
// and convenience methods for tracking metrics.
type WorkingState struct {
	s  map[string]any
	mu sync.RWMutex
}

// ErrWorkingStateInvalid is returned when the working state is invalid.
var ErrWorkingStateInvalid = errors.New("invalid working state")

// decodeWorkingState decodes the WorkingState from a byte slice.
// It returns an error if the decoding fails.
func decodeWorkingState(data []byte) (*WorkingState, error) {
	var state map[string]any
	err := json.Unmarshal(data, &state)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrWorkingStateInvalid, err)
	}
	return &WorkingState{
		s: state,
	}, nil
}

// Set sets a key-value pair in the WorkingState.
func (w *WorkingState) Set(key string, value any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.s == nil {
		w.s = make(map[string]any)
	}
	w.s[key] = value
}

// Value gets the value for a key from the WorkingState.
func (w *WorkingState) Value(key string) (any, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	val, ok := w.s[key]
	return val, ok
}

// Delete removes a key from the WorkingState.
func (w *WorkingState) Delete(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.s, key)
}

// Inc increments the value of a key and returns the new value.
func (w *WorkingState) Inc(key string) float64 {
	return w.add(key, 1)
}

// Dec decrements a key and returns the new value.
func (w *WorkingState) Dec(key string) float64 {
	return w.add(key, -1)
}

// Add adds the specified amount to a key and returns the new value.
func (w *WorkingState) Add(key string, amount float64) float64 {
	return w.add(key, amount)
}

// Sub subtracts the specified amount from a key and returns the new value.
func (w *WorkingState) Sub(key string, amount float64) float64 {
	return w.add(key, -amount)
}

// DiscardChanges discards any changes made to the WorkingState.
func (w *WorkingState) DiscardChanges() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.s = nil
}

func (w *WorkingState) add(key string, amount float64) float64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.s == nil {
		w.s = make(map[string]any)
	}
	val, ok := w.s[key]
	if !ok {
		w.s[key] = amount
		return amount
	}
	num, ok := val.(float64)
	if !ok {
		num = 0
	}
	num += amount
	w.s[key] = num
	return num
}

// encode encodes the WorkingState to a byte slice.
func (w *WorkingState) encode() ([]byte, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.s == nil {
		return []byte{}, nil
	}
	bytes, err := json.Marshal(w.s)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrWorkingStateInvalid, err)
	}
	return bytes, nil
}
