package orbital

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/openkcm/orbital/internal/clock"
	"github.com/openkcm/orbital/store/query"
)

// Repository provides methods to interact with the underlying data store.
// It encapsulates a Store implementation for data persistence operations.
type Repository struct {
	Store Store
}

// NewRepository creates a new Repository instance using the provided Store.
// It returns a pointer to the initialized Repository.
func NewRepository(store Store) *Repository {
	return &Repository{
		Store: store,
	}
}

// ErrRepoCreate is returned when the repository fails to create an entity.
var ErrRepoCreate = errors.New("failed to create entity")

type (
	// ListJobsQuery defines the parameters for querying jobs from the repository.
	// It allows filtering by status, creation time, result limit, and enables
	// queue-like retrieval mode when specified.
	ListJobsQuery struct {
		Status             JobStatus   // Filter jobs by their status.
		StatusIn           []JobStatus // Filter jobs by a list of statuses.
		UpdatedAt          int64       // Filter jobs updated at or after this timestamp.
		CreatedAt          int64       // Filter jobs created at or after this timestamp.
		Limit              int         // Maximum number of jobs to return.
		RetrievalModeQueue bool        // If true, enables queue-like retrieval mode.
		OrderByUpdatedAt   bool        // If true, orders jobs by updated_at in descending order.
	}

	// ListTasksQuery defines the parameters for querying tasks from the repository.
	// It supports filtering by job ID, task status, creation timestamp, and result
	// limit.
	ListTasksQuery struct {
		JobID              uuid.UUID    // Filter tasks by the associated job ID.
		Status             TaskStatus   // Filter tasks by their status.
		StatusIn           []TaskStatus // Filter tasks by a list of statuses.
		CreatedAt          int64        // Filter tasks created at or after this timestamp.
		UpdatedAt          int64        // Filter tasks updated at or after this timestamp.
		Limit              int          // Maximum number of tasks to return.
		IsReconcileReady   bool         // If true, filters tasks that are ready to be reconciled.
		RetrievalModeQueue bool         // If true, enables queue-like retrieval mode.
		OrderByUpdatedAt   bool         // If true, orders jobs by updated_at in ascending order.
	}

	// JobEventQuery defines the parameters for querying job events from the repository.
	JobEventQuery struct {
		ID                 uuid.UUID // Filter job events by their ID.
		IsNotified         *bool     // Filter job events by whether they have been notified.
		RetrievalModeQueue bool      // If true, enables queue-like retrieval mode.
		Limit              int       // Maximum number of job events to return.
		OrderByUpdatedAt   bool      // If true, orders job events by updated_at in ascending order.
	}
)

// createJob creates a new job entity in the repository.
// It takes a context and a Job object as input and returns the created Job object or an error if the operation fails.
func (r *Repository) createJob(ctx context.Context, job Job) (Job, error) {
	return createEntity(ctx, job, r)
}

// getJob retrieves a job entity from the repository by its ID.
// It takes a context and a UUID as input and returns the Job entity,
// a boolean indicating if the job was found, and an error if the operation fails.
func (r *Repository) getJob(ctx context.Context, id uuid.UUID) (Job, bool, error) {
	return getEntity[Job](ctx, r,
		query.Query{
			EntityName: query.EntityNameJobs,
			Clauses:    []query.Clause{query.ClauseWithID(id)},
		})
}

// updateJob updates an existing job entity in the repository.
// It takes a context and a Job object as input and returns an error if the update operation fails.
func (r *Repository) updateJob(ctx context.Context, job Job) error {
	err := updateEntity(ctx, job, r)
	if err != nil {
		slog.Error("updateJob", "error", err, "jobID", job.ID)
	}
	return err
}

// listJobs retrieves a list of job entities from the repository based on the provided query parameters.
// It takes a context and a ListJobsQuery object as input and returns a slice of Job entities or an error if the operation fails.
func (r *Repository) listJobs(ctx context.Context, jobsQuery ListJobsQuery) ([]Job, error) {
	q := query.Query{
		EntityName: query.EntityNameJobs,
		Clauses: []query.Clause{
			query.ClauseWithCreatedBefore(jobsQuery.CreatedAt),
		},
		Limit: jobsQuery.Limit,
	}

	if string(jobsQuery.Status) != "" {
		q.Clauses = append(q.Clauses, query.ClauseWithStatus(string(jobsQuery.Status)))
	}

	if len(jobsQuery.StatusIn) > 0 {
		statuses := make([]string, 0, len(jobsQuery.StatusIn))
		for _, status := range jobsQuery.StatusIn {
			statuses = append(statuses, string(status))
		}
		q.Clauses = append(q.Clauses, query.ClauseWithStatuses(statuses...))
	}

	if jobsQuery.UpdatedAt > 0 {
		q.Clauses = append(q.Clauses, query.ClauseWithUpdatedBefore(jobsQuery.UpdatedAt))
	}

	q.RetrievalMode = query.RetrievalModeDefault
	if jobsQuery.RetrievalModeQueue {
		q.RetrievalMode = query.RetrievalModeForUpdateSkipLocked
	}

	if jobsQuery.OrderByUpdatedAt {
		q.OrderBy = append(q.OrderBy, query.OrderByUpdatedAtAscending())
	}

	return listEntities[Job](ctx, r, q)
}

// createTasks creates multiple task entities in the repository.
// It takes a context and a slice of Task objects as input, and returns a slice of UUIDs for the created tasks or an error if the operation fails.
func (r *Repository) createTasks(ctx context.Context, tasks []Task) ([]uuid.UUID, error) {
	taskEntities, err := Encodes(tasks...)
	if err != nil {
		return nil, err
	}
	createdEntities, err := r.Store.Create(ctx, taskEntities...)
	if err != nil {
		return nil, err
	}
	createdTasks, err := Decodes[Task](createdEntities...)
	if err != nil {
		return nil, err
	}
	if len(createdTasks) == 0 {
		return nil, fmt.Errorf("%w task", ErrRepoCreate)
	}
	uIDs := make([]uuid.UUID, 0, len(createdTasks))
	for _, createdTask := range createdTasks {
		uIDs = append(uIDs, createdTask.ID)
	}
	return uIDs, nil
}

// getTask retrieves a task entity from the repository by its ID.
// It takes a context and a UUID as input and returns the Task entity,
// a boolean indicating if the task was found, and an error if the operation fails.
func (r *Repository) getTask(ctx context.Context, id uuid.UUID) (Task, bool, error) {
	return getEntity[Task](ctx, r, query.Query{
		EntityName: query.EntityNameTasks,
		Clauses:    []query.Clause{query.ClauseWithID(id)},
	})
}

// updateTask updates an existing task entity in the repository.
// It takes a context and a Task object as input and returns an error if the update operation fails.
func (r *Repository) updateTask(ctx context.Context, task Task) error {
	err := updateEntity(ctx, task, r)
	if err != nil {
		slog.Error("updateTask", "error", err, "taskID", task.ID)
	}
	return err
}

// listTasks retrieves a list of task entities from the repository based on the provided query parameters.
// It takes a context and a ListTasksQuery object as input and returns a slice of Task entities or an error if the operation fails.
//
//nolint:cyclop
func (r *Repository) listTasks(ctx context.Context, tasksQuery ListTasksQuery) ([]Task, error) {
	q := query.Query{
		EntityName: query.EntityNameTasks,
		Clauses:    []query.Clause{},
	}
	if tasksQuery.Status != "" {
		q.Clauses = append(q.Clauses, query.ClauseWithStatus(string(tasksQuery.Status)))
	}

	if len(tasksQuery.StatusIn) > 0 {
		statuses := make([]string, 0, len(tasksQuery.StatusIn))
		for _, status := range tasksQuery.StatusIn {
			statuses = append(statuses, string(status))
		}
		q.Clauses = append(q.Clauses, query.ClauseWithStatuses(statuses...))
	}

	if tasksQuery.JobID != uuid.Nil {
		q.Clauses = append(q.Clauses, query.ClauseWithJobID(tasksQuery.JobID))
	}

	now := clock.NowUnixNano()
	if tasksQuery.CreatedAt != 0 {
		q.Clauses = append(q.Clauses, query.ClauseWithCreatedBefore(tasksQuery.CreatedAt))
	}
	q.Clauses = append(q.Clauses, query.ClauseWithCreatedBefore(now))

	if tasksQuery.UpdatedAt != 0 {
		q.Clauses = append(q.Clauses, query.ClauseWithUpdatedBefore(tasksQuery.UpdatedAt))
	}
	q.Clauses = append(q.Clauses, query.ClauseWithUpdatedBefore(now))

	if tasksQuery.Limit > 0 {
		q.Limit = tasksQuery.Limit
	}

	if tasksQuery.IsReconcileReady {
		q.Clauses = append(q.Clauses, query.ClauseWithReadyToBeSent(now))
	}

	q.RetrievalMode = query.RetrievalModeDefault
	if tasksQuery.RetrievalModeQueue {
		q.RetrievalMode = query.RetrievalModeForUpdateSkipLocked
	}

	if tasksQuery.OrderByUpdatedAt {
		q.OrderBy = append(q.OrderBy, query.OrderByUpdatedAtAscending())
	}

	return listEntities[Task](ctx, r, q)
}

// createJobCursor creates a new job cursor entity in the repository.
// It takes a context and a JobCursor object as input and returns the created JobCursor object or an error if the operation fails.
func (r *Repository) createJobCursor(ctx context.Context, cursor JobCursor) (JobCursor, error) {
	return createEntity(ctx, cursor, r)
}

// getJobCursor retrieves a job cursor entity from the repository by its ID.
// It takes a context and a UUID as input and returns the JobCursor entity,
// a boolean indicating if the job cursor was found, and an error if the operation fails.
func (r *Repository) getJobCursor(ctx context.Context, id uuid.UUID) (JobCursor, bool, error) {
	return getEntity[JobCursor](ctx, r, query.Query{
		EntityName: query.EntityNameJobCursor,
		Clauses:    []query.Clause{query.ClauseWithID(id)},
	})
}

// updateJobCursor updates an existing job cursor entity in the repository.
// It takes a context and a JobCursor object as input and returns an error if the update operation fails.
func (r *Repository) updateJobCursor(ctx context.Context, jobCursor JobCursor) error {
	return updateEntity(ctx, jobCursor, r)
}

// createJobEvent creates a new job event in the repository.
// It uses the generic createEntity function to persist the event.
// Returns the created JobEvent and any error encountered during creation.
func (r *Repository) createJobEvent(ctx context.Context, event JobEvent) (JobEvent, error) {
	return createEntity(ctx, event, r)
}

// getJobEvent retrieves a JobEvent from the repository based on the provided JobEventQuery.
// It constructs a query using the parameters in eventQuery, such as IsNotified, ID,
// and OrderByUpdatedAt. Returns the found JobEvent, a boolean indicating if it was found,
// and any error encountered.
func (r *Repository) getJobEvent(ctx context.Context, eventQuery JobEventQuery) (JobEvent, bool, error) {
	q := query.Query{
		EntityName:    query.EntityNameJobEvent,
		Clauses:       []query.Clause{},
		RetrievalMode: query.RetrievalModeDefault,
		Limit:         eventQuery.Limit,
	}

	if eventQuery.RetrievalModeQueue {
		q.RetrievalMode = query.RetrievalModeForUpdateSkipLocked
	}

	if eventQuery.IsNotified != nil {
		q.Clauses = append(q.Clauses, query.ClauseWithIsNotified(*eventQuery.IsNotified))
	}
	if eventQuery.ID != uuid.Nil {
		q.Clauses = append(q.Clauses, query.ClauseWithID(eventQuery.ID))
	}
	if eventQuery.OrderByUpdatedAt {
		q.OrderBy = append(q.OrderBy, query.OrderByUpdatedAtAscending())
	}
	return getEntity[JobEvent](ctx, r, q)
}

// updateJobEvent updates an existing JobEvent entity in the repository.
// It uses the generic updateEntity function to persist changes to the event.
// Returns any error encountered during the update.
func (r *Repository) updateJobEvent(ctx context.Context, event JobEvent) error {
	return updateEntity(ctx, event, r)
}

// transaction executes a transactional operation within the repository.
// It takes a context and a TransactionFunc as input and returns an error if the transaction fails.
func (r *Repository) transaction(ctx context.Context, txFunc TransactionFunc) error {
	return r.Store.Transaction(ctx, txFunc)
}

// getEntity retrieves an entity of type T from the repository using the provided
// query. It returns the entity, a boolean indicating if the entity exists, and
// an error if the retrieval or decoding fails.
func getEntity[T EntityTypes](ctx context.Context, r *Repository, q query.Query) (T, bool, error) {
	var entity T
	result, err := r.Store.Find(ctx, q)
	if err != nil {
		return entity, false, err
	}
	if !result.Exists {
		return entity, false, nil
	}
	entity, err = Decode[T](result.Entity)
	return entity, err == nil, err
}

// updateEntity encodes the given entity of type T and updates it in the
// repository. It returns an error if encoding or updating fails.
func updateEntity[T EntityTypes](ctx context.Context, entity T, r *Repository) error {
	encodedEntity, err := Encode(entity)
	if err != nil {
		return err
	}
	_, err = r.Store.Update(ctx, encodedEntity)
	return err
}

// listEntities retrieves a list of entities of type T from the repository using
// the provided query. It returns a slice of entities and an error if the
// retrieval or decoding fails.
func listEntities[T EntityTypes](ctx context.Context, r *Repository, q query.Query) ([]T, error) {
	result, err := r.Store.List(ctx, q)
	if err != nil {
		return nil, err
	}
	if !result.Exists {
		return []T{}, nil
	}
	return Decodes[T](result.Entities...)
}

// createEntity encodes the given entity of type T and creates it in the
// repository. It returns the created entity and an error if encoding, creation,
// or decoding fails.
func createEntity[T EntityTypes](ctx context.Context, entity T, r *Repository) (T, error) {
	var out T
	encodedEntity, err := Encode(entity)
	if err != nil {
		return out, err
	}
	createdEntities, err := r.Store.Create(ctx, encodedEntity)
	if err != nil {
		return out, err
	}
	decodedEntities, err := Decodes[T](createdEntities...)
	if err != nil {
		return out, err
	}
	if len(decodedEntities) == 0 {
		return out, fmt.Errorf("%w %T", ErrRepoCreate, entity)
	}
	return decodedEntities[0], nil
}

// getJobForUpdate retrieves a job entity by its ID and ensures that the job is locked for update.
func (r *Repository) getJobForUpdate(ctx context.Context, id uuid.UUID) (Job, bool, error) {
	q := query.Query{
		EntityName:    query.EntityNameJobs,
		Clauses:       []query.Clause{query.ClauseWithID(id)},
		RetrievalMode: query.RetrievalModeForUpdate,
	}

	return getEntity[Job](ctx, r, q)
}
