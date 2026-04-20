package orbital_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
)

func TestLabels_Validate(t *testing.T) {
	tests := []struct {
		name      string
		labels    orbital.Labels
		expectErr bool
	}{
		{
			name:      "should pass with nil labels",
			labels:    nil,
			expectErr: false,
		},
		{
			name:      "should pass with empty labels",
			labels:    orbital.Labels{},
			expectErr: false,
		},
		{
			name:      "should pass with valid labels",
			labels:    orbital.Labels{"tenant": "acme", "env": "prod"},
			expectErr: false,
		},
		{
			name:      "should fail with orbital/ prefix",
			labels:    orbital.Labels{"orbital/group-id": "123"},
			expectErr: true,
		},
		{
			name:      "should fail with any orbital/ prefixed key",
			labels:    orbital.Labels{"tenant": "acme", "orbital/custom": "value"},
			expectErr: true,
		},
		{
			name:      "should pass with similar but not reserved prefix",
			labels:    orbital.Labels{"orbital-something": "value"},
			expectErr: false,
		},
		{
			name:      "should pass with orbitals/ prefix (not orbital/)",
			labels:    orbital.Labels{"orbitals/something": "value"},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.labels.Validate()

			if tt.expectErr {
				assert.Error(t, err)
				assert.ErrorIs(t, err, orbital.ErrReservedLabelPrefix)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLabels(t *testing.T) {
	t.Run("should create and access labels", func(t *testing.T) {
		// when
		labels := orbital.Labels{"key": "value"}

		// then
		assert.Equal(t, "value", labels["key"])
	})

	t.Run("should handle multiple key-value pairs", func(t *testing.T) {
		// when
		labels := orbital.Labels{
			"tenant":   "acme",
			"priority": "high",
			"env":      "production",
		}

		// then
		assert.Len(t, labels, 3)
		assert.Equal(t, "acme", labels["tenant"])
		assert.Equal(t, "high", labels["priority"])
		assert.Equal(t, "production", labels["env"])
	})

	t.Run("should handle empty labels", func(t *testing.T) {
		// when
		labels := orbital.Labels{}

		// then
		assert.NotNil(t, labels)
		assert.Empty(t, labels)
	})

	t.Run("should return empty string for missing key", func(t *testing.T) {
		// given
		labels := orbital.Labels{"key": "value"}

		// when
		value := labels["nonexistent"]

		// then
		assert.Empty(t, value)
	})
}

func TestJob_WithLabels(t *testing.T) {
	t.Run("should set labels on job", func(t *testing.T) {
		// given
		labels := orbital.Labels{"tenant": "acme", "env": "prod"}

		// when
		job := orbital.NewJob("type", []byte("data")).WithLabels(labels)

		// then
		assert.Equal(t, labels, job.Labels)
	})

	t.Run("should handle nil labels", func(t *testing.T) {
		// when
		job := orbital.NewJob("type", []byte("data")).WithLabels(nil)

		// then
		assert.Nil(t, job.Labels)
	})

	t.Run("should handle empty labels", func(t *testing.T) {
		// when
		job := orbital.NewJob("type", []byte("data")).WithLabels(orbital.Labels{})

		// then
		assert.NotNil(t, job.Labels)
		assert.Empty(t, job.Labels)
	})
}

func TestLabelConstants(t *testing.T) {
	assert.Equal(t, "orbital/", orbital.LabelPrefixReserved)
	assert.Equal(t, "orbital/group-id", orbital.LabelKeyGroupID)
	assert.Equal(t, "orbital/group-order-key", orbital.LabelKeyGroupOrderKey)
}

func TestMergeLabels(t *testing.T) {
	t.Run("should merge nil labels", func(t *testing.T) {
		// when
		result := orbital.MergeLabels(nil)

		// then
		assert.NotNil(t, result)
		assert.Empty(t, result)
	})

	t.Run("should merge single labels map", func(t *testing.T) {
		// given
		labels := orbital.Labels{"tenant": "acme", "env": "prod"}

		// when
		result := orbital.MergeLabels(labels)

		// then
		assert.Equal(t, "acme", result["tenant"])
		assert.Equal(t, "prod", result["env"])
		assert.Len(t, result, 2)
	})

	t.Run("should merge multiple labels maps", func(t *testing.T) {
		// given
		labels1 := orbital.Labels{"tenant": "acme"}
		labels2 := orbital.Labels{"env": "prod"}
		labels3 := orbital.Labels{"priority": "high"}

		// when
		result := orbital.MergeLabels(labels1, labels2, labels3)

		// then
		assert.Equal(t, "acme", result["tenant"])
		assert.Equal(t, "prod", result["env"])
		assert.Equal(t, "high", result["priority"])
		assert.Len(t, result, 3)
	})

	t.Run("should override with later maps", func(t *testing.T) {
		// given
		labels1 := orbital.Labels{"key": "first", "other": "value"}
		labels2 := orbital.Labels{"key": "second"}

		// when
		result := orbital.MergeLabels(labels1, labels2)

		// then
		assert.Equal(t, "second", result["key"])  // Overridden
		assert.Equal(t, "value", result["other"]) // Kept from first
		assert.Len(t, result, 2)
	})

	t.Run("should not modify original labels", func(t *testing.T) {
		// given
		labels1 := orbital.Labels{"tenant": "acme"}
		labels2 := orbital.Labels{"env": "prod"}

		// when
		result := orbital.MergeLabels(labels1, labels2)

		// then
		assert.Len(t, labels1, 1) // Original unchanged
		assert.Len(t, labels2, 1) // Original unchanged
		assert.Len(t, result, 2)  // New map has 2 entries
	})

	t.Run("should skip nil maps in variadic args", func(t *testing.T) {
		// given
		labels := orbital.Labels{"tenant": "acme"}

		// when
		result := orbital.MergeLabels(nil, labels, nil)

		// then
		assert.Equal(t, "acme", result["tenant"])
		assert.Len(t, result, 1)
	})

	t.Run("should handle empty maps", func(t *testing.T) {
		// given
		labels1 := orbital.Labels{}
		labels2 := orbital.Labels{"tenant": "acme"}

		// when
		result := orbital.MergeLabels(labels1, labels2)

		// then
		assert.Equal(t, "acme", result["tenant"])
		assert.Len(t, result, 1)
	})
}

func TestLabels_ToJSON(t *testing.T) {
	tests := []struct {
		name     string
		labels   orbital.Labels
		expected string
	}{
		{
			name:     "nil labels",
			labels:   nil,
			expected: "null",
		},
		{
			name:     "empty labels",
			labels:   orbital.Labels{},
			expected: "{}",
		},
		{
			name:     "single label",
			labels:   orbital.Labels{"tenant": "acme"},
			expected: `{"tenant":"acme"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// when
			result, err := tt.labels.ToJSON()

			// then
			assert.NoError(t, err)
			assert.JSONEq(t, tt.expected, string(result))
		})
	}
}
