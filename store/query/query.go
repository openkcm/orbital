package query

import (
	"github.com/google/uuid"
)

const (
	// operatorEqual is the operator for equality.
	operatorEqual Operator = "="
	// OperatorLessThanEqual is the operator for less than or equal to.
	operatorLessThanEqual                     Operator   = "<="
	operatorIn                                Operator   = "IN"
	fieldID                                   Field      = "id"
	fieldType                                 Field      = "type"
	fieldJobID                                Field      = "job_id"
	fieldExternalID                           Field      = "external_id"
	fieldStatus                               Field      = "status"
	fieldCreatedAt                            Field      = "created_at"
	fieldUpdatedAt                            Field      = "updated_at"
	fieldSumLastReconciledAtAndReconcileAfter Field      = "(reconcile_after_sec * 1000000000::numeric + last_reconciled_at)"
	fieldIsNotified                           Field      = "is_notified"
	EntityNameJobs                            EntityName = "jobs"
	EntityNameTasks                           EntityName = "tasks"
	EntityNameJobCursor                       EntityName = "job_cursor"
	EntityNameJobEvent                        EntityName = "job_event"
)

type RetrievalMode int

const (
	RetrievalModeDefault RetrievalMode = iota
	RetrievalModeForUpdate
	RetrievalModeForUpdateSkipLocked
)

// Query represents a database query for a specific entity type. It includes
// filtering clauses, pagination cursor, result limit, and an option to enable
// queue-like retrieval mode.
type Query struct {
	EntityName    EntityName // Name of the entity being queried.
	Clauses       []Clause   // Filtering clauses for the query.
	Cursor        Cursor     // Cursor for pagination.
	Limit         int        // Maximum number of results to return.
	RetrievalMode RetrievalMode
	OrderBy       []OrderBy // Fields to order the results by.
}

// OrderBy represents the ordering of query results.
type OrderBy struct {
	Field       Field // The field to order by.
	IsAscending bool  // If true, orders in ascending order; otherwise, descending.
}

// Cursor represents a pagination cursor for queries. It consists of a
// timestamp and a unique identifier to support efficient and consistent
// pagination of results.
type Cursor struct {
	Timestamp int64     // Timestamp for the cursor position.
	ID        uuid.UUID // Unique identifier for the cursor position.
}

// Clause represents a single filtering condition in a query. It specifies
// the field to filter on, the operator to use, and the value to compare.
type Clause struct {
	Field    Field    // The field to filter on.
	Operator Operator // The comparison operator.
	Value    any      // The value to compare against.
}

// EntityName represents the name of the entity in the query.
type EntityName string

// Operator represents the operator used in the query.
type Operator string

// Field represents the field name in the query.
type Field string

// ClauseWithID creates a Clause that filters by the entity's ID field using
// the equality operator and the provided UUID value.
func ClauseWithID(id uuid.UUID) Clause {
	return Clause{Field: fieldID, Operator: operatorEqual, Value: id}
}

// ClauseWithJobID creates a Clause that filters by the job ID field using
// the equality operator and the provided UUID value.
func ClauseWithJobID(jobID uuid.UUID) Clause {
	return Clause{Field: fieldJobID, Operator: operatorEqual, Value: jobID}
}

// ClauseWithExternalID creates a Clause that filters by the externalID field using
// the equality operator and the provided externalID value.
func ClauseWithExternalID(externalID string) Clause {
	return Clause{Field: fieldExternalID, Operator: operatorEqual, Value: externalID}
}

// ClauseWithStatus creates a Clause that filters by the status field using
// the equality operator and the provided status value.
func ClauseWithStatus(status string) Clause {
	return Clause{Field: fieldStatus, Operator: operatorEqual, Value: status}
}

// ClauseWithStatuses creates a Clause that filters by the status field using
// the in operator and the provided status values.
func ClauseWithStatuses(statuses ...string) Clause {
	return Clause{Field: fieldStatus, Operator: operatorIn, Value: statuses}
}

func ClauseWithTypes(types ...string) Clause {
	return Clause{Field: fieldType, Operator: operatorIn, Value: types}
}

// ClauseWithCreatedBefore creates a Clause that filters for entities with a
// created_at field less than or equal to the provided timestamp.
func ClauseWithCreatedBefore(createdAt int64) Clause {
	return Clause{Field: fieldCreatedAt, Operator: operatorLessThanEqual, Value: createdAt}
}

// ClauseWithUpdatedBefore creates a Clause that filters for entities with a
// created_at field less than or equal to the provided timestamp.
func ClauseWithUpdatedBefore(updatedAt int64) Clause {
	return Clause{Field: fieldUpdatedAt, Operator: operatorLessThanEqual, Value: updatedAt}
}

// ClauseWithCreatedAt creates a Clause that filters by the created_at field
// using the equality operator and the provided timestamp value.
func ClauseWithCreatedAt(createdAt int64) Clause {
	return Clause{Field: fieldCreatedAt, Operator: operatorEqual, Value: createdAt}
}

// ClauseWithReadyToBeSent creates a Clause that filters for entities where the sum of reconcile_after_sec and last_reconciled_at is less than or equal to the current time.
func ClauseWithReadyToBeSent(val int64) Clause {
	return Clause{Field: fieldSumLastReconciledAtAndReconcileAfter, Operator: operatorLessThanEqual, Value: val}
}

// OrderByUpdatedAtAscending creates an OrderBy clause that orders results by the updated_at field in ascending order.
func OrderByUpdatedAtAscending() OrderBy {
	return OrderBy{Field: fieldUpdatedAt, IsAscending: true}
}

// ClauseWithIsNotified creates a Clause that filters results based on
// the isNotified field. It returns a Clause that matches entities where
// the isNotified field equals the specified value.
func ClauseWithIsNotified(isNotified bool) Clause {
	return Clause{Field: fieldIsNotified, Operator: operatorEqual, Value: isNotified}
}
