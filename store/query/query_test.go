package query_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital/store/query"
)

func TestQueryClause(t *testing.T) {
	// given
	uID := uuid.New()
	tts := []struct {
		name          string
		clauseCreator func() query.Clause
		expClause     query.Clause
	}{
		{
			name: "should create clause with CreatedAt",
			clauseCreator: func() query.Clause {
				return query.ClauseWithCreatedAt(100)
			},
			expClause: query.Clause{Field: "created_at", Operator: query.Operator("="), Value: int64(100)},
		},
		{
			name: "should create clause with CreatedAtBefore",
			clauseCreator: func() query.Clause {
				return query.ClauseWithCreatedBefore(100)
			},
			expClause: query.Clause{
				Field: "created_at", Operator: query.Operator("<="), Value: int64(100),
			},
		},
		{
			name: "should create clause wit ID",
			clauseCreator: func() query.Clause {
				return query.ClauseWithID(uID)
			},
			expClause: query.Clause{Field: "id", Operator: query.Operator("="), Value: uID},
		},
		{
			name: "should create clause with Status",
			clauseCreator: func() query.Clause {
				return query.ClauseWithStatus("status")
			},
			expClause: query.Clause{
				Field: "status", Operator: query.Operator("="), Value: "status",
			},
		},
		{
			name: "should create clause with JobID",
			clauseCreator: func() query.Clause {
				return query.ClauseWithJobID(uID)
			},
			expClause: query.Clause{
				Field: "job_id", Operator: query.Operator("="), Value: uID,
			},
		},
		{
			name:          "should create clause with Statuses",
			clauseCreator: func() query.Clause { return query.ClauseWithStatuses("ERROR", "DONE", "CREATES") },
			expClause: query.Clause{
				Field:    "status",
				Operator: query.Operator("IN"),
				Value:    []string{"ERROR", "DONE", "CREATES"},
			},
		},
		{
			name: "should create clause with UpdatedAtBefore",
			clauseCreator: func() query.Clause {
				return query.ClauseWithUpdatedBefore(100)
			},
			expClause: query.Clause{
				Field: "updated_at", Operator: query.Operator("<="), Value: int64(100),
			},
		},
		{
			name: "should create clause with ReadyToBeSent",
			clauseCreator: func() query.Clause {
				return query.ClauseWithReadyToBeSent(100)
			},
			expClause: query.Clause{
				Field:    "(reconcile_after_sec * 1000000000::numeric + last_reconciled_at)",
				Operator: query.Operator("<="),
				Value:    int64(100),
			},
		},
		{
			name: "should create clause with ClauseWithIsNotified",
			clauseCreator: func() query.Clause {
				return query.ClauseWithIsNotified(true)
			},
			expClause: query.Clause{
				Field:    "is_notified",
				Operator: query.Operator("="),
				Value:    true,
			},
		},
	}
	for _, tt := range tts {
		t.Run(tt.name, func(t *testing.T) {
			// when
			result := tt.clauseCreator()

			// then
			assert.Equal(t, tt.expClause, result)
		})
	}
}
