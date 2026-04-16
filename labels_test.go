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
}
