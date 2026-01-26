package orbital_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
)

func TestWorkingState_SetAndValue(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    any
		expValue any
	}{
		{
			name:     "set and get string value",
			key:      "key1",
			value:    "value1",
			expValue: "value1",
		},
		{
			name:     "set and get float64 value",
			key:      "key2",
			value:    42,
			expValue: 42,
		},
		{
			name:     "set and get struct value",
			key:      "key3",
			value:    struct{ Field string }{Field: "data"},
			expValue: struct{ Field string }{Field: "data"},
		},
		{
			name:     "set and get nil value",
			key:      "key4",
			value:    nil,
			expValue: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ws := &orbital.WorkingState{}
			ws.Set(tt.key, tt.value)

			val, ok := ws.Value(tt.key)
			assert.True(t, ok)
			assert.Equal(t, tt.expValue, val)
		})
	}

	t.Run("get from empty WorkingState", func(t *testing.T) {
		ws := &orbital.WorkingState{}
		_, ok := ws.Value("nonexistent")
		assert.False(t, ok)
	})
}

func TestWorkingState_Delete(t *testing.T) {
	ws := &orbital.WorkingState{}
	ws.Set("key1", "value1")

	ws.Delete("key1")

	_, ok := ws.Value("key1")
	assert.False(t, ok)

	// ensure deleting a non-existent key does not cause issues
	ws.Delete("nonexistent")
}

func TestWorkingState_GaugeMethods(t *testing.T) {
	incTests := []struct {
		name      string
		key       string
		initValue float64
		expValue  float64
	}{
		{
			name:     "increment key",
			key:      "gauge1",
			expValue: 1,
		},
		{
			name:      "increment key with existing value",
			key:       "gauge2",
			initValue: 2,
			expValue:  3,
		},
	}

	for _, tt := range incTests {
		t.Run(tt.name, func(t *testing.T) {
			ws := &orbital.WorkingState{}
			if tt.initValue != 0 {
				ws.Set(tt.key, tt.initValue)
			}
			value := ws.Inc(tt.key)
			assert.InEpsilon(t, tt.expValue, value, 10e-6)

			val, ok := ws.Value(tt.key)
			assert.True(t, ok)
			assert.InEpsilon(t, tt.expValue, val, 10e-6)
		})
	}

	decTests := []struct {
		name      string
		key       string
		initValue float64
		expValue  float64
	}{
		{
			name:     "decrement key",
			key:      "gauge1",
			expValue: -1,
		},
		{
			name:      "decrement key with existing value",
			key:       "gauge2",
			initValue: 5,
			expValue:  4,
		},
	}

	for _, tt := range decTests {
		t.Run(tt.name, func(t *testing.T) {
			ws := &orbital.WorkingState{}
			if tt.initValue != 0 {
				ws.Set(tt.key, tt.initValue)
			}
			value := ws.Dec(tt.key)
			assert.InEpsilon(t, tt.expValue, value, 10e-6)

			val, ok := ws.Value(tt.key)
			assert.True(t, ok)
			assert.InEpsilon(t, tt.expValue, val, 10e-6)
		})
	}

	addTests := []struct {
		name      string
		key       string
		amount    float64
		initValue float64
		expValue  float64
	}{
		{
			name:     "add to key",
			key:      "gauge1",
			amount:   5,
			expValue: 5,
		},
		{
			name:      "add to key with existing value",
			key:       "gauge2",
			amount:    10,
			initValue: 3,
			expValue:  13,
		},
	}

	for _, tt := range addTests {
		t.Run(tt.name, func(t *testing.T) {
			ws := &orbital.WorkingState{}
			if tt.initValue != 0 {
				ws.Set(tt.key, tt.initValue)
			}
			value := ws.Add(tt.key, tt.amount)
			assert.InEpsilon(t, tt.expValue, value, 10e-6)

			val, ok := ws.Value(tt.key)
			assert.True(t, ok)
			assert.InEpsilon(t, tt.expValue, val, 10e-6)
		})
	}

	subTests := []struct {
		name      string
		key       string
		amount    float64
		initValue float64
		expValue  float64
	}{
		{
			name:     "subtract from key",
			key:      "gauge1",
			amount:   4,
			expValue: -4,
		},
		{
			name:      "subtract from key with existing value",
			key:       "gauge2",
			amount:    2,
			initValue: 7,
			expValue:  5,
		},
	}

	for _, tt := range subTests {
		t.Run(tt.name, func(t *testing.T) {
			ws := &orbital.WorkingState{}
			if tt.initValue != 0 {
				ws.Set(tt.key, tt.initValue)
			}
			value := ws.Sub(tt.key, tt.amount)
			assert.InEpsilon(t, tt.expValue, value, 10e-6)

			val, ok := ws.Value(tt.key)
			assert.True(t, ok)
			assert.InEpsilon(t, tt.expValue, val, 10e-6)
		})
	}
}

func TestWorkingState_DiscardChanges(t *testing.T) {
	ws := &orbital.WorkingState{}
	ws.Set("key1", "value1")

	ws.DiscardChanges()

	_, ok := ws.Value("key1")
	assert.False(t, ok)

	// ensure we can set new values after discarding changes
	ws.Set("key1", "value2")
	val, ok := ws.Value("key1")
	assert.True(t, ok)
	assert.Equal(t, "value2", val)
}
