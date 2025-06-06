package orbital

import (
	"github.com/google/uuid"
)

// JobCursor stores the cursor for the next taskResolver.
type JobCursor struct {
	ID        uuid.UUID
	Cursor    TaskResolverCursor
	UpdatedAt int64
	CreatedAt int64
}
