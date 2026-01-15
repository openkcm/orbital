package orbital

import (
	"context"
	"sync"

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

func UpdateRepoTask(r *Repository) func(ctx context.Context, task Task) error {
	return r.updateTask
}

func GetRepoTaskForUpdate(r *Repository) func(ctx context.Context, id uuid.UUID) (Task, bool, error) {
	return r.getTaskForUpdate
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

func HandleTask(m *Manager) func(ctx context.Context, wg *sync.WaitGroup, repo Repository, job Job, task Task) {
	return m.handleTask
}

func HandleResponses(m *Manager) func(ctx context.Context, initiator ManagerTarget, target string) {
	return m.handleResponses
}

func ProcessResponse(m *Manager) func(ctx context.Context, resp TaskResponse) error {
	return m.processResponse
}

func RecordJobTerminatedEvent(m *Manager) func(ctx context.Context, repo Repository, job Job) error {
	return m.recordJobTerminatedEvent
}

func SendJobTerminatedEvent(m *Manager) func(ctx context.Context) error {
	return m.sendJobTerminatedEvent
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

func ToCanonicalData[T TaskRequest | TaskResponse](in T) ([]byte, error) {
	return toCanonicalData(in)
}
