package orbital

import (
	"context"

	"github.com/google/uuid"
)

func ConfirmJob(m *Manager) func(ctx context.Context) error {
	return m.confirmJob
}

func CreateTask(m *Manager) func(ctx context.Context) error {
	return m.createTask
}

func CreateRepoJob(r *Repository) func(ctx context.Context, job Job) (Job, error) {
	return r.createJob
}

func GetRepoJob(r *Repository) func(ctx context.Context, id uuid.UUID) (Job, bool, error) {
	return r.getJob
}

func UpdateRepoJob(r *Repository) func(ctx context.Context, job Job) error {
	return r.updateJob
}

func ListRepoJobs(r *Repository) func(ctx context.Context, q ListJobsQuery) ([]Job, error) {
	return r.listJobs
}

func CreateRepoTasks(r *Repository) func(ctx context.Context, tasks []Task) ([]uuid.UUID, error) {
	return r.createTasks
}

func GetRepoTask(r *Repository) func(ctx context.Context, id uuid.UUID) (Task, bool, error) {
	return r.getTask
}

func ListRepoTasks(r *Repository) func(ctx context.Context, q ListTasksQuery) ([]Task, error) {
	return r.listTasks
}

func CreateRepoJobCursor(r *Repository) func(ctx context.Context, job JobCursor) (JobCursor, error) {
	return r.createJobCursor
}

func GetRepoJobCursor(r *Repository) func(ctx context.Context, id uuid.UUID) (JobCursor, bool, error) {
	return r.getJobCursor
}

func UpdateRepoJobCursor(r *Repository) func(ctx context.Context, job JobCursor) error {
	return r.updateJobCursor
}

func Transaction(r *Repository) func(ctx context.Context, txFunc TransactionFunc) error {
	return r.transaction
}

func Reconcile(m *Manager) func(ctx context.Context) error {
	return m.reconcile
}

func ProcessResponse(m *Manager) func(ctx context.Context, resp TaskResponse) error {
	return m.processResponse
}

func RecordJobEvent(m *Manager) func(ctx context.Context, repo Repository, jobID uuid.UUID) error {
	return m.recordJobTerminationEvent
}

func SendJobEvent(m *Manager) func(ctx context.Context) error {
	return m.sendJobTerminationEvent
}

func CreateRepoJobEvent(r *Repository) func(ctx context.Context, event JobEvent) (JobEvent, error) {
	return r.createJobEvent
}

func GetRepoJobEvent(r *Repository) func(ctx context.Context, q JobEventQuery) (JobEvent, bool, error) {
	return r.getJobEvent
}

func UpdateRepoJobEvent(r *Repository) func(ctx context.Context, event JobEvent) error {
	return r.updateJobEvent
}

func GetRepoJobForUpdate(r *Repository) func(ctx context.Context, id uuid.UUID) (Job, bool, error) {
	return r.getJobForUpdate
}
