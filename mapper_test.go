package orbital_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/internal/clock"
	"github.com/openkcm/orbital/store/query"
)

func TestTransformToEntities(t *testing.T) {
	uID := uuid.New()
	now := clock.NowUnixNano()
	// given
	tests := []struct {
		name       string
		entityName query.EntityName
		input      []map[string]any
		expected   []orbital.Entity
		expectErr  error
	}{
		{
			name:       "success case Job",
			entityName: query.EntityNameJobs,
			input: []map[string]any{
				{
					"id":         uID.String(),
					"created_at": now, "updated_at": now, "state": "state",
					"error_message": "error",
				},
			},
			expected: []orbital.Entity{
				{
					Name:      query.EntityNameJobs,
					ID:        uID,
					CreatedAt: now,
					UpdatedAt: now,
				},
			},
		},
		{
			name:       "success case Task",
			entityName: query.EntityNameTasks,
			input: []map[string]any{
				{
					"id":         uID.String(),
					"created_at": now, "updated_at": now,
					"job_id":               "job_id",
					"type":                 "type",
					"data":                 []byte("data"),
					"working_state":        []byte("working_state"),
					"last_reconciled_at":   "last_reconciled_at",
					"reconcile_count":      "reconcile_count",
					"total_sent_count":     "total_sent_count",
					"total_received_count": 1,
					"reconcile_after_sec":  2,
					"etag":                 "etag",
					"status":               "status",
					"target":               "target",
					"error_message":        "error_message",
				},
			},
			expected: []orbital.Entity{
				{
					Name:      query.EntityNameTasks,
					ID:        uID,
					CreatedAt: now,
					UpdatedAt: now,
				},
			},
		},
		{
			name:       "success case JobCursor",
			entityName: query.EntityNameJobCursor,
			input: []map[string]any{
				{
					"id":         uID.String(),
					"created_at": now, "updated_at": now, "job_id": "job_id",
				},
			},
			expected: []orbital.Entity{
				{
					Name:      query.EntityNameJobCursor,
					ID:        uID,
					CreatedAt: now,
					UpdatedAt: now,
				},
			},
		},
		{
			name:       "success case JobEvent",
			entityName: query.EntityNameJobEvent,
			input: []map[string]any{
				{
					"id":         uID.String(),
					"created_at": now, "updated_at": now, "is_notified": true,
				},
			},
			expected: []orbital.Entity{
				{
					Name:      query.EntityNameJobEvent,
					ID:        uID,
					CreatedAt: now,
					UpdatedAt: now,
				},
			},
		},
		{
			name:       "error case JobCursor for a mandatory field filed missing",
			entityName: query.EntityNameJobCursor,
			input: []map[string]any{
				{
					"id":         uID.String(),
					"updated_at": now, "job_id": "job_id",
				},
			},
			expected:  nil,
			expectErr: orbital.ErrMandatoryFields,
		},
		{
			name:       "error case if entityName is unknown",
			entityName: "unknown_entity",
			input: []map[string]any{
				{"error": true},
			},
			expected:  nil,
			expectErr: orbital.ErrInvalidEntityType,
		},
		{
			name:       "error case if input uuid is not valid",
			entityName: query.EntityNameJobCursor,
			input: []map[string]any{
				{
					"id": "wrong-uuid",
				},
			},
			expectErr: orbital.ErrMandatoryFields,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// given
			// making sure that all values are set in expected entities
			if tt.expectErr == nil {
				for i := range tt.expected {
					tt.expected[i].Values = tt.input[i]
				}
			}

			// when
			result, err := orbital.TransformToEntities(tt.entityName, tt.input...)

			// then
			assert.ErrorIs(t, err, tt.expectErr)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestInit(t *testing.T) {
	now := clock.NowUnixNano()
	time.Sleep(1 * time.Microsecond)

	t.Run("should initialize fields", func(t *testing.T) {
		// given
		entity := &orbital.Entity{}

		// when
		orbital.Init(entity)

		// then
		assert.Less(t, now, entity.CreatedAt)
		assert.Less(t, now, entity.UpdatedAt)
		assert.NotEqual(t, uuid.Nil, entity.ID)
		_, ok := entity.Values["id"]
		assert.True(t, ok)
		_, ok = entity.Values["created_at"]
		assert.True(t, ok)
		_, ok = entity.Values["updated_at"]
		assert.True(t, ok)
	})

	t.Run("should preserve fields", func(t *testing.T) {
		// given
		uID := uuid.New()

		entity := &orbital.Entity{
			CreatedAt: now,
			UpdatedAt: now,
			ID:        uID,
		}
		expected := &orbital.Entity{
			ID:        uID,
			UpdatedAt: now,
			CreatedAt: now,
			Values:    map[string]any{},
		}

		// when
		orbital.Init(entity)

		// then
		assert.Equal(t, expected, entity)
	})
}

func TestEncodes(t *testing.T) {
	uID := uuid.New()
	unixTime := clock.NowUnixNano()
	t.Run("success Job", func(t *testing.T) {
		input := []orbital.Job{
			{
				ID:           uID,
				CreatedAt:    unixTime,
				UpdatedAt:    unixTime,
				ErrorMessage: "foo-error",
				Data:         []byte("foo-data"),
				Status:       orbital.JobStatusCreated,
				Type:         "baz-type",
				ExternalID:   "external-id",
			},
		}
		expected := []orbital.Entity{
			{
				Name:      query.EntityNameJobs,
				ID:        uID,
				CreatedAt: unixTime,
				UpdatedAt: unixTime,
				Values: map[string]any{
					"id":            uID,
					"created_at":    unixTime,
					"updated_at":    unixTime,
					"error_message": "foo-error",
					"data":          []byte("foo-data"),
					"status":        orbital.JobStatusCreated,
					"type":          "baz-type",
					"external_id":   "external-id",
				},
			},
		}

		// when
		result, err := orbital.Encodes(input...)

		// then
		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})
	t.Run("success Task", func(t *testing.T) {
		input := []orbital.Task{
			{
				ID:           uID,
				CreatedAt:    unixTime,
				UpdatedAt:    unixTime,
				ErrorMessage: "foo-error",
			},
		}
		expected := []orbital.Entity{
			{
				Name:      query.EntityNameTasks,
				ID:        uID,
				CreatedAt: unixTime,
				UpdatedAt: unixTime,
				Values: map[string]any{
					"id":                   uID,
					"created_at":           unixTime,
					"updated_at":           unixTime,
					"working_state":        []byte(nil),
					"data":                 []byte(nil),
					"type":                 "",
					"etag":                 "",
					"job_id":               uuid.Nil,
					"last_reconciled_at":   int64(0),
					"reconcile_after_sec":  int64(0),
					"reconcile_count":      int64(0),
					"total_sent_count":     int64(0),
					"total_received_count": int64(0),
					"status":               orbital.TaskStatus(""),
					"target":               "",
					"error_message":        "foo-error",
				},
			},
		}

		// when
		result, err := orbital.Encodes(input...)

		// then
		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})
	t.Run("success JobCursor", func(t *testing.T) {
		input := []orbital.JobCursor{
			{
				ID:        uID,
				CreatedAt: unixTime,
				UpdatedAt: unixTime,
			},
		}
		expected := []orbital.Entity{
			{
				Name:      query.EntityNameJobCursor,
				ID:        uID,
				CreatedAt: unixTime,
				UpdatedAt: unixTime,
				Values: map[string]any{
					"id":         uID,
					"created_at": unixTime,
					"updated_at": unixTime,
					"cursor":     orbital.TaskResolverCursor(""),
				},
			},
		}

		// when
		result, err := orbital.Encodes(input...)

		// then
		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})
	t.Run("success JobEvent", func(t *testing.T) {
		input := []orbital.JobEvent{
			{
				ID:         uID,
				IsNotified: true,
				CreatedAt:  unixTime,
				UpdatedAt:  unixTime,
			},
		}
		expected := []orbital.Entity{
			{
				Name:      query.EntityNameJobEvent,
				ID:        uID,
				CreatedAt: unixTime,
				UpdatedAt: unixTime,
				Values: map[string]any{
					"id":          uID,
					"created_at":  unixTime,
					"updated_at":  unixTime,
					"is_notified": true,
				},
			},
		}

		// when
		result, err := orbital.Encodes(input...)

		// then
		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})
}

func TestDecodes(t *testing.T) {
	t.Run("decodes Job", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			// given
			job1 := orbital.Job{
				ID:           uuid.New(),
				Data:         []byte("resource-data-1"),
				Type:         "type-1",
				Status:       orbital.JobStatusConfirmed,
				ErrorMessage: "error-message-1",
				UpdatedAt:    clock.NowUnixNano(),
				CreatedAt:    clock.NowUnixNano(),
			}
			job2 := orbital.Job{
				ID:           uuid.New(),
				Data:         []byte("resource-data-2"),
				Type:         "type-2",
				Status:       orbital.JobStatusCreated,
				ErrorMessage: "error-message-2",
				UpdatedAt:    clock.NowUnixNano(),
				CreatedAt:    clock.NowUnixNano(),
			}

			in, _ := orbital.Encodes(job1, job2)

			// when
			result, err := orbital.Decodes[orbital.Job](in...)

			// then
			assert.NoError(t, err)
			assert.Len(t, result, 2)
			assert.Equal(t, job1, result[0])
			assert.Equal(t, job2, result[1])
		})
		t.Run("error for missing fields in values for the key", func(t *testing.T) {
			keysToDelete := []string{
				"data",
				"type",
				"status",
				"error_message",
			}

			for _, key := range keysToDelete {
				t.Run(key, func(t *testing.T) {
					id := uuid.New()
					entity := orbital.Entity{
						Name:      query.EntityNameJobs,
						ID:        id,
						CreatedAt: 0,
						UpdatedAt: 0,
						Values: map[string]any{
							"id":            id,
							"data":          []byte("data"),
							"type":          "type",
							"status":        "status",
							"error_message": "error",
							"updated_at":    0,
							"created_at":    0,
						},
					}
					delete(entity.Values, key)

					_, err := orbital.Decodes[orbital.Job](entity)
					assert.Error(t, err)
				})
			}
		})
	})
	t.Run("decodes Task", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			// given
			task1 := orbital.Task{
				ID:                 uuid.New(),
				JobID:              uuid.New(),
				Type:               "type-1",
				WorkingState:       []byte("working-state-1"),
				LastReconciledAt:   clock.NowUnixNano(),
				ReconcileCount:     2,
				TotalSentCount:     4,
				TotalReceivedCount: 6,
				ReconcileAfterSec:  clock.NowUnixNano(),
				ETag:               "etag-1",
				Status:             orbital.TaskStatusCreated,
				Target:             "target-1",
				ErrorMessage:       "error-message-1",
				UpdatedAt:          clock.NowUnixNano(),
				CreatedAt:          clock.NowUnixNano(),
			}
			task2 := orbital.Task{
				ID:                 uuid.New(),
				JobID:              uuid.New(),
				Type:               "type-2",
				LastReconciledAt:   clock.NowUnixNano(),
				ReconcileCount:     2,
				TotalSentCount:     1,
				TotalReceivedCount: 2,
				ReconcileAfterSec:  clock.NowUnixNano(),
				ETag:               "etag-2",
				Status:             orbital.TaskStatusCreated,
				Target:             "target-2",
				ErrorMessage:       "error-message-2",
				UpdatedAt:          clock.NowUnixNano(),
				CreatedAt:          clock.NowUnixNano(),
			}

			in, _ := orbital.Encodes(task1, task2)

			// when
			result, err := orbital.Decodes[orbital.Task](in...)

			// then
			assert.NoError(t, err)
			assert.Len(t, result, 2)
			assert.Equal(t, task1, result[0])
			assert.Equal(t, task2, result[1])
		})
		t.Run("error for missing fields in values for the key", func(t *testing.T) {
			keysToDelete := []string{
				"job_id",
				"etag",
				"target",
				"status",
				"working_state",
				"type",
				"data",
				"last_reconciled_at",
				"reconcile_count",
				"total_sent_count",
				"total_received_count",
				"reconcile_after_sec",
				"error_message",
			}

			for _, key := range keysToDelete {
				t.Run(key, func(t *testing.T) {
					id := uuid.New()
					entity := orbital.Entity{
						Name:      query.EntityNameTasks,
						ID:        id,
						CreatedAt: 0,
						UpdatedAt: 0,
						Values: map[string]any{
							"id":                   id,
							"job_id":               id,
							"type":                 "type",
							"data":                 []byte("data"),
							"working_state":        []byte("workingState"),
							"last_reconciled_at":   int64(0),
							"reconcile_count":      int64(0),
							"total_received_count": int64(0),
							"total_sent_count":     int64(0),
							"reconcile_after_sec":  int64(0),
							"etag":                 "etag",
							"status":               "status",
							"target":               "target",
							"error_message":        "error",
							"updated_at":           0,
							"created_at":           0,
						},
					}
					delete(entity.Values, key)

					_, err := orbital.Decodes[orbital.Task](entity)
					assert.Error(t, err)
				})
			}
		})
	})
	t.Run("decodes JobCursor", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			// given
			cursor1 := orbital.JobCursor{
				ID:        uuid.New(),
				Cursor:    "cursor-1",
				UpdatedAt: clock.NowUnixNano(),
				CreatedAt: clock.NowUnixNano(),
			}
			cursor2 := orbital.JobCursor{
				ID:        uuid.New(),
				Cursor:    "cursor-2",
				UpdatedAt: clock.NowUnixNano(),
				CreatedAt: clock.NowUnixNano(),
			}

			in, _ := orbital.Encodes(cursor1, cursor2)

			// when
			result, err := orbital.Decodes[orbital.JobCursor](in...)

			// then
			assert.NoError(t, err)
			assert.Len(t, result, 2)
			assert.Equal(t, cursor1, result[0])
			assert.Equal(t, cursor2, result[1])
		})
		t.Run("error for missing fields in values for the key", func(t *testing.T) {
			keysToDelete := []string{
				"cursor",
			}

			for _, key := range keysToDelete {
				t.Run(key, func(t *testing.T) {
					id := uuid.New()
					entity := orbital.Entity{
						Name:      query.EntityNameJobCursor,
						ID:        id,
						CreatedAt: 0,
						UpdatedAt: 0,
						Values: map[string]any{
							"id":         id,
							"cursor":     "cursor",
							"updated_at": 0,
							"created_at": 0,
						},
					}
					delete(entity.Values, key)

					_, err := orbital.Decodes[orbital.JobCursor](entity)
					assert.Error(t, err)
				})
			}
		})
	})

	t.Run("decodes JobEvent", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			// given
			event1 := orbital.JobEvent{
				ID:         uuid.New(),
				IsNotified: true,
				UpdatedAt:  clock.NowUnixNano(),
				CreatedAt:  clock.NowUnixNano(),
			}
			event2 := orbital.JobEvent{
				ID:         uuid.New(),
				IsNotified: false,
				UpdatedAt:  clock.NowUnixNano(),
				CreatedAt:  clock.NowUnixNano(),
			}

			in, _ := orbital.Encodes(event1, event2)

			// when
			result, err := orbital.Decodes[orbital.JobEvent](in...)

			// then
			assert.NoError(t, err)
			assert.Len(t, result, 2)
			assert.Equal(t, event1, result[0])
			assert.Equal(t, event2, result[1])
		})
		t.Run("error for missing fields in values for the key", func(t *testing.T) {
			keysToDelete := []string{
				"is_notified",
			}

			for _, key := range keysToDelete {
				t.Run(key, func(t *testing.T) {
					id := uuid.New()
					entity := orbital.Entity{
						Name:      query.EntityNameJobEvent,
						ID:        id,
						CreatedAt: 0,
						UpdatedAt: 0,
						Values: map[string]any{
							"id":          id,
							"is_notified": false,
							"updated_at":  0,
							"created_at":  0,
						},
					}
					delete(entity.Values, key)

					_, err := orbital.Decodes[orbital.JobEvent](entity)
					assert.Error(t, err)
				})
			}
		})
	})
	t.Run("return error for missing mandatory fields", func(t *testing.T) {
		// when
		result, err := orbital.Decodes[orbital.Job](orbital.Entity{})

		// then
		assert.Equal(t, orbital.ErrMandatoryFields, errors.Unwrap(err))
		assert.Nil(t, result)
	})
}

func TestDecodeValueVariants(t *testing.T) {
	id := uuid.New()
	now := clock.NowUnixNano()

	t.Run("status as alias type", func(t *testing.T) {
		e := orbital.Entity{
			ID:        id,
			CreatedAt: now,
			UpdatedAt: now,
			Values: map[string]any{
				"type":          "foo",
				"status":        orbital.JobStatusConfirmCanceled,
				"data":          []byte("x"),
				"error_message": "error",
				"external_id":   "external-id",
			},
		}
		job, err := orbital.Decode[orbital.Job](e)
		assert.NoError(t, err)
		assert.Equal(t, orbital.JobStatusConfirmCanceled, job.Status)
	})

	t.Run("status as string type", func(t *testing.T) {
		e := orbital.Entity{
			ID:        id,
			CreatedAt: now,
			UpdatedAt: now,
			Values: map[string]any{
				"type":          "foo",
				"status":        "CONFIRMED",
				"data":          []byte("x"),
				"error_message": "error",
				"external_id":   "external-id",
			},
		}
		job, err := orbital.Decode[orbital.Job](e)
		assert.NoError(t, err)
		assert.Equal(t, orbital.JobStatusConfirmed, job.Status)
	})

	t.Run("nil data blob should return zero Value", func(t *testing.T) {
		e := orbital.Entity{
			ID:        id,
			CreatedAt: now,
			UpdatedAt: now,
			Values: map[string]any{
				"type":          "foo",
				"status":        "CREATED",
				"data":          nil,
				"error_message": "error",
				"external_id":   "external-id",
			},
		}
		job, err := orbital.Decode[orbital.Job](e)
		assert.NoError(t, err)
		assert.Nil(t, job.Data)
	})

	t.Run("missing status key should return ErrMandatoryFields", func(t *testing.T) {
		e := orbital.Entity{
			ID:        id,
			CreatedAt: now,
			UpdatedAt: now,
			Values: map[string]any{
				"type": "foo",
				"data": []byte("x"),
			},
		}
		_, err := orbital.Decode[orbital.Job](e)
		assert.ErrorIs(t, errors.Unwrap(err), orbital.ErrMandatoryFields)
	})

	t.Run("wrong type for data should return ErrInvalidEntityType", func(t *testing.T) {
		e := orbital.Entity{
			ID:        id,
			CreatedAt: now,
			UpdatedAt: now,
			Values: map[string]any{
				"type":        "foo",
				"status":      "CREATED",
				"data":        "not-bytes",
				"external_id": "external-id",
			},
		}
		_, err := orbital.Decode[orbital.Job](e)
		assert.ErrorIs(t, errors.Unwrap(err), orbital.ErrInvalidEntityType)
	})

	t.Run("wrong type for status should return ErrInvalidEntityType", func(t *testing.T) {
		e := orbital.Entity{
			ID:        id,
			CreatedAt: now,
			UpdatedAt: now,
			Values: map[string]any{
				"type":        "foo",
				"status":      10,
				"data":        "not-bytes",
				"external_id": "external-id",
			},
		}
		_, err := orbital.Decode[orbital.Job](e)
		assert.ErrorIs(t, errors.Unwrap(err), orbital.ErrInvalidEntityType)
	})
}
