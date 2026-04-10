package orbital_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
)

func TestNewJobGroup(t *testing.T) {
	t.Run("should create job group with type and jobs", func(t *testing.T) {
		// given
		job1 := orbital.NewJob("sync", []byte("data1"))
		job2 := orbital.NewJob("sync", []byte("data2"))
		job3 := orbital.NewJob("sync", []byte("data3"))

		// when
		group := orbital.NewJobGroup("batch-sync", job1, job2, job3)

		// then
		assert.Equal(t, "batch-sync", group.Type)
		assert.Len(t, group.Jobs, 3)
		assert.Equal(t, job1, group.Jobs[0])
		assert.Equal(t, job2, group.Jobs[1])
		assert.Equal(t, job3, group.Jobs[2])
	})

	t.Run("should create job group with empty jobs list", func(t *testing.T) {
		// when
		group := orbital.NewJobGroup("empty-group")

		// then
		assert.Equal(t, "empty-group", group.Type)
		assert.Empty(t, group.Jobs)
	})

	t.Run("should preserve job order", func(t *testing.T) {
		// given
		jobs := make([]orbital.Job, 5)
		for i := range jobs {
			jobs[i] = orbital.NewJob("type", []byte{byte(i)})
		}

		// when
		group := orbital.NewJobGroup("ordered-group", jobs...)

		// then
		assert.Len(t, group.Jobs, 5)
		for i, job := range group.Jobs {
			assert.Equal(t, []byte{byte(i)}, job.Data)
		}
	})

	t.Run("should have zero-value fields for unset properties", func(t *testing.T) {
		// when
		group := orbital.NewJobGroup("test-group")

		// then
		assert.Equal(t, uuid.Nil, group.ID)
		assert.Equal(t, orbital.GroupStatus(""), group.Status)
		assert.Nil(t, group.Labels)
		assert.Empty(t, group.ErrorMessage)
		assert.Zero(t, group.CreatedAt)
		assert.Zero(t, group.UpdatedAt)
	})
}

func TestJobGroup_WithLabels(t *testing.T) {
	t.Run("should set labels on job group", func(t *testing.T) {
		// given
		group := orbital.NewJobGroup("test-group")
		labels := orbital.Labels{"tenant": "acme", "priority": "high"}

		// when
		result := group.WithLabels(labels)

		// then
		assert.Equal(t, labels, result.Labels)
		assert.Equal(t, "acme", result.Labels["tenant"])
		assert.Equal(t, "high", result.Labels["priority"])
	})

	t.Run("should chain with NewJobGroup", func(t *testing.T) {
		// given
		job1 := orbital.NewJob("sync", []byte("data1"))
		job2 := orbital.NewJob("sync", []byte("data2"))
		labels := orbital.Labels{"env": "production"}

		// when
		group := orbital.NewJobGroup("batch-sync", job1, job2).
			WithLabels(labels)

		// then
		assert.Equal(t, "batch-sync", group.Type)
		assert.Len(t, group.Jobs, 2)
		assert.Equal(t, labels, group.Labels)
	})

	t.Run("should return modified job group for chaining", func(t *testing.T) {
		// given
		group := orbital.NewJobGroup("test-group")

		// when
		result := group.WithLabels(orbital.Labels{"key": "value"})

		// then
		assert.Equal(t, "test-group", result.Type)
		assert.Equal(t, "value", result.Labels["key"])
	})

	t.Run("should handle empty labels", func(t *testing.T) {
		// given
		group := orbital.NewJobGroup("test-group")

		// when
		result := group.WithLabels(orbital.Labels{})

		// then
		assert.NotNil(t, result.Labels)
		assert.Empty(t, result.Labels)
	})

	t.Run("should handle nil labels", func(t *testing.T) {
		// given
		group := orbital.NewJobGroup("test-group")

		// when
		result := group.WithLabels(nil)

		// then
		assert.Nil(t, result.Labels)
	})
}

func TestGroupStatus(t *testing.T) {
	t.Run("should have correct string values", func(t *testing.T) {
		// then
		assert.Equal(t, orbital.GroupStatusCreated, orbital.GroupStatus("CREATED"))
		assert.Equal(t, orbital.GroupStatusProcessing, orbital.GroupStatus("PROCESSING"))
		assert.Equal(t, orbital.GroupStatusDone, orbital.GroupStatus("DONE"))
		assert.Equal(t, orbital.GroupStatusFailed, orbital.GroupStatus("FAILED"))
		assert.Equal(t, orbital.GroupStatusCanceled, orbital.GroupStatus("CANCELED"))
	})

	t.Run("should have distinct values", func(t *testing.T) {
		// given
		statuses := []orbital.GroupStatus{
			orbital.GroupStatusCreated,
			orbital.GroupStatusProcessing,
			orbital.GroupStatusDone,
			orbital.GroupStatusFailed,
			orbital.GroupStatusCanceled,
		}

		// then
		seen := make(map[orbital.GroupStatus]bool)
		for _, status := range statuses {
			assert.False(t, seen[status], "duplicate status: %s", status)
			seen[status] = true
		}
		assert.Len(t, seen, 5)
	})
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

func TestJob_GroupID(t *testing.T) {
	t.Run("should have nil GroupID for standalone job", func(t *testing.T) {
		// when
		job := orbital.NewJob("type", []byte("data"))

		// then
		assert.Nil(t, job.GroupID)
	})

	t.Run("should allow setting GroupID", func(t *testing.T) {
		// given
		job := orbital.NewJob("type", []byte("data"))
		groupID := uuid.New()

		// when
		job.GroupID = &groupID

		// then
		assert.NotNil(t, job.GroupID)
		assert.Equal(t, groupID, *job.GroupID)
	})
}
