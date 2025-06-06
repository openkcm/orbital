package orbital

import (
	"context"

	"github.com/google/uuid"

	"github.com/openkcm/orbital/store/query"
)

// Store defines the interface for a data store that supports CRUD operations and transactions.
type Store interface {
	Create(ctx context.Context, r ...Entity) ([]Entity, error)
	Update(ctx context.Context, r ...Entity) ([]Entity, error)
	Find(ctx context.Context, q query.Query) (FindResult, error)
	List(ctx context.Context, q query.Query) (ListResult, error)
	Transaction(ctx context.Context, txFunc TransactionFunc) error
}

// Entity represents a generic entity stored in the data store.
// It includes metadata such as the entity's name, unique identifier,created and updated timestamps, and a map of all values.
type (
	Entity struct {
		Name      query.EntityName
		ID        uuid.UUID
		CreatedAt int64
		UpdatedAt int64
		Values    map[string]any
	}
)

// TransactionFunc defines a function type for executing operations within a transaction.
type TransactionFunc func(context.Context, Repository) error

// EntityTypes defines a type constraint for entities that can be used in the repository.
// It allows only types Job, Task, or JobCursor to satisfy this interface.
type EntityTypes interface {
	Job | Task | JobCursor | JobEvent
}

type FindResult struct {
	Entity Entity
	Exists bool
}

type ListResult struct {
	Entities []Entity
	Cursor   query.Cursor
	Exists   bool
}
