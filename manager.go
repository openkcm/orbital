package orbital

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/openkcm/common-sdk/pkg/logger"

	slogctx "github.com/veqryn/slog-context"

	"github.com/openkcm/orbital/internal/clock"
	"github.com/openkcm/orbital/internal/retry"
	"github.com/openkcm/orbital/internal/worker"
)

type (
	// Manager is the interface for managing jobs,
	// including their creation, state transitions, and lifecycle handling.
	Manager struct {
		Config               Config
		repo                 *Repository
		jobConfirmFunc       JobConfirmFunc
		taskResolveFunc      TaskResolveFunc
		jobDoneEventFunc     JobTerminatedEventFunc
		jobCanceledEventFunc JobTerminatedEventFunc
		jobFailedEventFunc   JobTerminatedEventFunc
		targets              map[string]ManagerTarget
	}

	// JobTerminatedEventFunc defines a callback function type for sending job events.
	JobTerminatedEventFunc func(ctx context.Context, job Job) error

	// ManagerOptsFunc is a function type to configure Manager options.
	ManagerOptsFunc func(mgr *Manager)

	// JobConfirmFunc defines a function that determines whether a job can be confirmed.
	// It returns a ConfirmResult struct with the confirmation result and an error if the process fails.
	JobConfirmFunc func(ctx context.Context, job Job) (JobConfirmResult, error)

	// JobConfirmResult represents the result of a job confirmation operation.
	JobConfirmResult struct {
		// Done indicates whether the confirming process is complete.
		Done bool
		// IsCanceled indicates whether the job needs to be canceled.
		IsCanceled bool
		// CanceledErrorMessage provides an error message if the job is canceled.
		CanceledErrorMessage string
	}

	// TaskResolverCursor is the type for the next cursor.
	TaskResolverCursor string

	// TaskResolverResult represents the response from resolving tasks.
	TaskResolverResult struct {
		// TaskInfos contains the data to be sent for each target.
		TaskInfos []TaskInfo
		// Cursor provides information for pagination or continuation.
		Cursor TaskResolverCursor
		// Done indicates whether the resolution process is complete.
		Done bool
		// IsCanceled indicates whether the job needs to be canceled.
		IsCanceled bool
		// CanceledErrorMessage provides an error message if the job is canceled.
		CanceledErrorMessage string
	}

	// TaskResolveFunc is a  function type that resolves targets for the creation tasks for a job and the cursor.
	TaskResolveFunc func(ctx context.Context, job Job, cursor TaskResolverCursor) (TaskResolverResult, error)

	// Config contains configuration for job processing.
	Config struct {
		// TaskLimitNum is the maximum number of tasks to process at once.
		TaskLimitNum int
		// ConfirmJobAfter is the delay before confirming a job.
		ConfirmJobAfter time.Duration
		// ConfirmJobWorkerConfig holds the configuration for the job confirmation worker.
		ConfirmJobWorkerConfig WorkerConfig
		// CreateTasksWorkerConfig holds the configuration for the task creation worker.
		CreateTasksWorkerConfig WorkerConfig
		// ReconcilesWorkerConfig holds the configuration for the reconciliation worker.
		ReconcileWorkerConfig WorkerConfig
		// NotifyWorkerConfig holds the configuration for the notification worker.
		NotifyWorkerConfig WorkerConfig
		// BackoffBaseIntervalSec is the base interval for exponential backoff in seconds.
		// Default is 10 seconds.
		BackoffBaseIntervalSec int64
		// BackoffMaxIntervalSec is the maximum interval for exponential backoff in seconds.
		// Default is 10240 seconds (2 hours and 50 minutes).
		BackoffMaxIntervalSec int64
		// MaxReconcileCount is the maximum number of times a task can be reconciled.
		// Default is 10.
		MaxReconcileCount int64
	}
)

// Default values for configs.
const (
	defConfirmJobAfter     = 0
	defNoOfWorker          = 5
	defWorkTimeout         = 5 * time.Second
	defTaskLimitNum        = 500
	defWorkExecInterval    = 10 * time.Second
	defReconcileInterval   = 5 * time.Second
	defBackoffBaseInterval = 10
	defBackoffMaxInterval  = 10240
	defMaxReconcileCount   = 10
)

var (
	ErrTaskResolverNotSet = errors.New("taskResolver not set")
	ErrMsgFailedTasks     = "job has failed tasks"
	ErrJobUnCancelable    = errors.New("job cannot be canceled in its current state")
	ErrJobNotFound        = errors.New("job not found")
	ErrJobAlreadyExists   = errors.New("job already exists")
	ErrNoClientForTarget  = errors.New("no client for task target")
	ErrLoadingJob         = errors.New("failed to load job")
	ErrUpdatingJob        = errors.New("failed to update job")
	ErrTaskNotFound       = errors.New("task not found")
)

// NewManager creates a new Manager instance.
func NewManager(repo *Repository, taskResolver TaskResolveFunc, optFuncs ...ManagerOptsFunc) (*Manager, error) {
	if taskResolver == nil {
		return nil, ErrTaskResolverNotSet
	}
	mgr := &Manager{
		repo:            repo,
		taskResolveFunc: taskResolver,
		Config: Config{
			ConfirmJobWorkerConfig: WorkerConfig{
				NoOfWorkers:  defNoOfWorker,
				ExecInterval: defWorkExecInterval,
				Timeout:      defWorkTimeout,
			},
			CreateTasksWorkerConfig: WorkerConfig{
				NoOfWorkers:  defNoOfWorker,
				ExecInterval: defWorkExecInterval,
				Timeout:      defWorkTimeout,
			},
			ReconcileWorkerConfig: WorkerConfig{
				NoOfWorkers:  defNoOfWorker,
				ExecInterval: defReconcileInterval,
				Timeout:      defWorkTimeout,
			},
			NotifyWorkerConfig: WorkerConfig{
				NoOfWorkers:  defNoOfWorker,
				ExecInterval: defWorkExecInterval,
				Timeout:      defWorkTimeout,
			},
			ConfirmJobAfter:        defConfirmJobAfter,
			TaskLimitNum:           defTaskLimitNum,
			BackoffBaseIntervalSec: defBackoffBaseInterval,
			BackoffMaxIntervalSec:  defBackoffMaxInterval,
			MaxReconcileCount:      defMaxReconcileCount,
		},
		jobConfirmFunc: func(_ context.Context, _ Job) (JobConfirmResult, error) {
			return JobConfirmResult{
				Done: true,
			}, nil
		},
	}
	for _, optFunc := range optFuncs {
		optFunc(mgr)
	}

	return mgr, nil
}

// WithJobConfirmFunc registers a function to confirm jobs.
func WithJobConfirmFunc(f JobConfirmFunc) ManagerOptsFunc {
	return func(opts *Manager) {
		opts.jobConfirmFunc = f
	}
}

// WithTargets set the map that maps the target string to its ManagerTarget.
func WithTargets(targets map[string]ManagerTarget) ManagerOptsFunc {
	return func(m *Manager) {
		m.targets = targets
	}
}

// WithJobDoneEventFunc registers a function to send job done events.
func WithJobDoneEventFunc(f JobTerminatedEventFunc) ManagerOptsFunc {
	return func(m *Manager) {
		m.jobDoneEventFunc = f
	}
}

// WithJobCanceledEventFunc registers a function to send job canceled events.
func WithJobCanceledEventFunc(f JobTerminatedEventFunc) ManagerOptsFunc {
	return func(m *Manager) {
		m.jobCanceledEventFunc = f
	}
}

// WithJobFailedEventFunc registers a function to send job failed events.
func WithJobFailedEventFunc(f JobTerminatedEventFunc) ManagerOptsFunc {
	return func(m *Manager) {
		m.jobFailedEventFunc = f
	}
}

// Start starts the job manager to process jobs.
func (m *Manager) Start(ctx context.Context) error {
	runner := worker.Runner{
		Works: []worker.Work{
			{
				Name:         "confirm-job",
				Fn:           m.confirmJob,
				NoOfWorkers:  m.Config.ConfirmJobWorkerConfig.NoOfWorkers,
				ExecInterval: m.Config.ConfirmJobWorkerConfig.ExecInterval,
				Timeout:      m.Config.ConfirmJobWorkerConfig.Timeout,
			},
			{
				Name:         "create-task",
				Fn:           m.createTask,
				NoOfWorkers:  m.Config.CreateTasksWorkerConfig.NoOfWorkers,
				ExecInterval: m.Config.CreateTasksWorkerConfig.ExecInterval,
				Timeout:      m.Config.CreateTasksWorkerConfig.Timeout,
			},
			{
				Name:         "reconcile",
				Fn:           m.reconcile,
				ExecInterval: m.Config.ReconcileWorkerConfig.ExecInterval,
				NoOfWorkers:  m.Config.ReconcileWorkerConfig.NoOfWorkers,
				Timeout:      m.Config.ReconcileWorkerConfig.Timeout,
			},
			{
				Name:         "notify-event",
				Fn:           m.sendJobTerminatedEvent,
				ExecInterval: m.Config.NotifyWorkerConfig.ExecInterval,
				NoOfWorkers:  m.Config.NotifyWorkerConfig.NoOfWorkers,
				Timeout:      m.Config.NotifyWorkerConfig.Timeout,
			},
		},
	}
	m.startResponseReaders(ctx)

	return runner.Run(ctx)
}

// PrepareJob prepares a job by creating it in the repository with status CREATED.
// It returns an error if a job with the same type and external ID in a non-terminal status already exists.
func (m *Manager) PrepareJob(ctx context.Context, job Job) (Job, error) {
	job.Status = JobStatusCreated
	job, err := m.repo.createJob(ctx, job)
	if err != nil {
		if errors.Is(err, ErrEntityUniqueViolation) {
			return job, ErrJobAlreadyExists
		}
		return job, err
	}

	slogctx.Debug(ctx, "new job prepared", "jobID", job.ID, "externalID", job.ExternalID, "type", job.Type)
	return job, nil
}

// GetJob retrieves a job by its ID from the repository.
func (m *Manager) GetJob(ctx context.Context, jobID uuid.UUID) (Job, bool, error) {
	return m.repo.getJob(ctx, jobID)
}

// ListTasks retrieves tasks by query from the repository.
func (m *Manager) ListTasks(ctx context.Context, query ListTasksQuery) ([]Task, error) {
	return m.repo.listTasks(ctx, query)
}

// CancelJob cancels a job and associated running tasks. It updates the job status to "USER_CANCEL".
func (m *Manager) CancelJob(ctx context.Context, jobID uuid.UUID) error {
	return m.repo.transaction(ctx, func(ctx context.Context, repo Repository) error {
		slogctx.Debug(ctx, "canceling job", "jobID", jobID)
		job, ok, err := repo.getJobForUpdate(ctx, jobID)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrLoadingJob, err)
		}
		if !ok {
			return ErrJobNotFound
		}
		if !job.isCancelable() {
			return ErrJobUnCancelable
		}

		job.Status = JobStatusUserCanceled
		job.ErrorMessage = "job has been canceled by the user"

		if err := m.updateJobAndCreateJobEvent(ctx, repo, job); err != nil {
			return fmt.Errorf("%w: %w", ErrUpdatingJob, err)
		}

		return nil
	})
}

// confirmJob processes jobs in the CREATED state that were created before a specified delay
// and attempts to confirm them within a transaction.
func (m *Manager) confirmJob(ctx context.Context) error {
	return m.repo.transaction(ctx, func(ctx context.Context, repo Repository) error {
		now := clock.Now()
		jobs, err := repo.listJobs(ctx, ListJobsQuery{
			Status:             JobStatusCreated,
			CreatedAt:          clock.ToUnixNano(now.Add(-m.Config.ConfirmJobAfter)),
			Limit:              1,
			RetrievalModeQueue: true,
			OrderByUpdatedAt:   true,
		})
		if err != nil {
			slogctx.Error(ctx, "listing jobs to be confirmed failed", "error", err)
			return err
		}
		if len(jobs) == 0 {
			slogctx.Log(ctx, logger.LevelTrace, "no jobs to confirm")
			return nil
		}
		job := jobs[0]

		ctx = slogctx.With(ctx, "jobID", job.ID, "externalID", job.ExternalID, "type", job.Type)
		slogctx.Debug(ctx, "confirming job")

		err = m.handleConfirmJob(ctx, repo, job)
		if err != nil {
			slogctx.Error(ctx, "failed to confirm job", "error", err)
		}

		slogctx.Debug(ctx, "job confirmed", "status", job.Status)

		return err
	})
}

// handleConfirmJob executes the confirmation function for a job and updates its state
// based on the confirmation result returned by the function.
func (m *Manager) handleConfirmJob(ctx context.Context, repo Repository, job Job) error {
	res, err := m.jobConfirmFunc(ctx, job)
	if err != nil {
		slogctx.Error(ctx, "error in job confirmation function", "error", err)
		// NOTE: here we update the job to change the updated_at timestamp in order to spread the fetching of jobs.
		return repo.updateJob(ctx, job)
	}
	if res.IsCanceled {
		slogctx.Debug(ctx, "job canceled by job confirmation function")
		job.Status = JobStatusConfirmCanceled
		job.ErrorMessage = res.CanceledErrorMessage
		return m.updateJobAndCreateJobEvent(ctx, repo, job)
	}
	job.Status = JobStatusConfirming
	if res.Done {
		job.Status = JobStatusConfirmed
	}
	return repo.updateJob(ctx, job)
}

// createTask orchestrates the creation of tasks for jobs in the repository.
// It retrieves jobs, resolves their targets, creates tasks, updates job states,
// and manages job cursors within a transactional context.
func (m *Manager) createTask(ctx context.Context) error {
	return m.repo.transaction(ctx, func(ctx context.Context, repo Repository) error {
		job, ok, err := m.jobForTaskCreation(ctx, repo)
		if err != nil {
			slogctx.Error(ctx, "failed to get job for task creation", "error", err)
			return err
		}
		if !ok {
			slogctx.Log(ctx, logger.LevelTrace, "no jobs found for task creation")
			return nil
		}

		return m.createTasksForJob(ctx, repo, job)
	})
}

func (m *Manager) createTasksForJob(ctx context.Context, repo Repository, job Job) error {
	ctx = slogctx.With(ctx, "jobID", job.ID, "externalID", job.ExternalID, "type", job.Type)
	slogctx.Debug(ctx, "creating tasks for job")

	jobCursor, found, err := m.getJobCursor(ctx, repo, job.ID)
	if err != nil {
		slogctx.Error(ctx, "failed to get job cursor", "error", err)
		return err
	}

	resolverResult, err := m.taskResolveFunc(ctx, job, jobCursor.Cursor)
	if err != nil {
		slogctx.Error(ctx, "error in task resolver function", "error", err)
		// NOTE: here we update the job to change the updated_at timestamp in order to spread the fetching of jobs.
		return repo.updateJob(ctx, job)
	}
	if resolverResult.IsCanceled {
		slogctx.Debug(ctx, "job canceled by task resolver")
		job.Status = JobStatusResolveCanceled
		job.ErrorMessage = resolverResult.CanceledErrorMessage
		return m.updateJobAndCreateJobEvent(ctx, repo, job)
	}

	if err := m.targetsExist(resolverResult.TaskInfos); err != nil {
		slogctx.Debug(ctx, "failed to resolve task targets for job", "error", err)
		job.Status = JobStatusFailed
		job.ErrorMessage = err.Error()
		return m.updateJobAndCreateJobEvent(ctx, repo, job)
	}

	// Create tasks and update the cursor.
	jobCursor.Cursor = resolverResult.Cursor
	_, err = repo.createTasks(ctx, newTasks(job.ID, resolverResult.TaskInfos))
	if err != nil {
		slogctx.Error(ctx, "failed to create tasks for job", "error", err)
		return err
	}

	err = m.createOrUpdateCursor(ctx, repo, found, jobCursor)
	if err != nil {
		slogctx.Error(ctx, "failed to create/update tasks cursor for job ", "error", err)
		return err
	}

	if resolverResult.Done {
		slogctx.Debug(ctx, "tasks resolved successfully")
		job.Status = JobStatusReady
	} else {
		slogctx.Debug(ctx, "task resolving still in progress")
		job.Status = JobStatusResolving
	}

	return repo.updateJob(ctx, job)
}

// targetsExist checks if all targets in the provided TaskInfo slice have corresponding clients.
// It returns an error if any target does not have a corresponding client.
func (m *Manager) targetsExist(infos []TaskInfo) error {
	for _, info := range infos {
		if _, ok := m.targets[info.Target]; !ok {
			return fmt.Errorf("%w target: %s", ErrNoClientForTarget, info.Target)
		}
	}
	return nil
}

// createOrUpdateCursor creates or updates a JobCursor in the repository.
// If the JobCursor is found, it updates the existing record; otherwise, it creates a new one.
func (m *Manager) createOrUpdateCursor(ctx context.Context, repo Repository, found bool, jobCursor JobCursor) error {
	if found {
		return repo.updateJobCursor(ctx, jobCursor)
	}
	_, err := repo.createJobCursor(ctx, jobCursor)
	return err
}

// jobForTaskCreation retrieves the next job eligible for task creation from the repository.
// It searches for jobs with statuses "resolving" or "confirmed" that were created at the
// current UTC Unix timestamp. Returns the job, a boolean indicating if a job was found,
// and an error if the retrieval fails.
func (m *Manager) jobForTaskCreation(ctx context.Context, repo Repository) (Job, bool, error) {
	var empty Job
	jobs, err := repo.listJobs(ctx, ListJobsQuery{
		StatusIn:           []JobStatus{JobStatusResolving, JobStatusConfirmed},
		CreatedAt:          clock.NowUnixNano(),
		Limit:              1,
		RetrievalModeQueue: true,
		OrderByUpdatedAt:   true,
	})
	if err != nil {
		return empty, false, err
	}
	if len(jobs) == 0 {
		return empty, false, nil
	}
	return jobs[0], true, nil
}

// getJobCursor retrieves the JobCursor for a given job ID from the repository.
// If the JobCursor is not found, it initializes a new JobCursor with the provided job ID.
func (m *Manager) getJobCursor(ctx context.Context, repo Repository, jobID uuid.UUID) (JobCursor, bool, error) {
	jobCursor, found, err := repo.getJobCursor(ctx, jobID)
	if err != nil {
		return JobCursor{}, false, err
	}
	if !found {
		jobCursor.ID = jobID
	}
	return jobCursor, found, nil
}

// reconcile processes a job that is in state READY or PROCESSING.
// It sends a task request for each task of the job that is in state CREATED or PROCESSING.
// It terminates the job if all tasks are processed.
// It updates the job status to DONE if all tasks are processed successfully or to FAILED if any task failed.
func (m *Manager) reconcile(ctx context.Context) error {
	return m.repo.transaction(ctx, func(ctx context.Context, repo Repository) error {
		job, err := m.getJobForReconcile(ctx, repo)
		if err != nil {
			slogctx.Error(ctx, "failed to get job to be reconciled", "error", err)
			return err
		}
		if job.ID == uuid.Nil {
			slogctx.Log(ctx, logger.LevelTrace, "no jobs ready for reconciliation")
			return nil
		}

		ctx = slogctx.With(ctx, "jobID", job.ID, "externalID", job.ExternalID, "type", job.Type)

		job.Status = JobStatusProcessing

		// Check if there are any tasks not terminated yet,
		// ignoring the reconcile ready flag.
		tasks, err := repo.listTasks(ctx, ListTasksQuery{
			JobID:    job.ID,
			StatusIn: []TaskStatus{TaskStatusCreated, TaskStatusProcessing},
			Limit:    1,
		})
		if err != nil {
			slogctx.Error(ctx, "listing not terminated tasks for job failed", "error", err)
			return err
		}

		if len(tasks) == 0 {
			slogctx.Debug(ctx, "all tasks for job are processed, terminating job")
			return m.terminateJob(ctx, repo, job)
		}

		tasks, err = repo.listTasks(ctx, ListTasksQuery{
			JobID:              job.ID,
			IsReconcileReady:   true,
			StatusIn:           []TaskStatus{TaskStatusCreated, TaskStatusProcessing},
			OrderByUpdatedAt:   true,
			RetrievalModeQueue: true,
			Limit:              m.Config.TaskLimitNum,
		})
		if err != nil {
			slogctx.Error(ctx, "listing tasks to be reconciled failed", "error", err)
			return err
		}

		m.handleTasks(ctx, repo, job, tasks)

		return repo.updateJob(ctx, job)
	})
}

// getJobForReconcile retrieves the next job that is ready for reconciliation.
func (m *Manager) getJobForReconcile(ctx context.Context, repo Repository) (Job, error) {
	jobs, err := repo.listJobs(ctx, ListJobsQuery{
		StatusIn:           []JobStatus{JobStatusReady, JobStatusProcessing},
		CreatedAt:          clock.NowUnixNano(),
		OrderByUpdatedAt:   true,
		RetrievalModeQueue: true,
		Limit:              1,
	})
	if err != nil {
		return Job{}, err
	}

	if len(jobs) == 0 {
		return Job{}, nil
	}

	return jobs[0], nil
}

// terminateJob updates the job status to DONE or FAILED based on the status of its tasks.
func (m *Manager) terminateJob(ctx context.Context, repo Repository, job Job) error {
	slogctx.Debug(ctx, "terminating job")
	job.Status = JobStatusDone

	tasks, err := repo.listTasks(ctx, ListTasksQuery{
		JobID:    job.ID,
		StatusIn: []TaskStatus{TaskStatusFailed},
		Limit:    1,
	})
	if err != nil {
		slogctx.Error(ctx, "listing failed tasks for job failed", "error", err)
		return err
	}
	if len(tasks) > 0 {
		slogctx.Debug(ctx, "job has failed tasks, marking job as failed", "count", len(tasks))
		job.ErrorMessage = ErrMsgFailedTasks
		job.Status = JobStatusFailed
	}

	return m.updateJobAndCreateJobEvent(ctx, repo, job)
}

// handleTasks handles a list of tasks concurrently.
func (m *Manager) handleTasks(ctx context.Context, repo Repository, job Job, tasks []Task) {
	if len(tasks) == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, task := range tasks {
		wg.Add(1)
		go m.handleTask(ctx, &wg, repo, job, task)
	}
	wg.Wait()
}

// handleTask handles a single task.
// It checks if the task can be sent based on its sent count and max sent count.
// If the task has reached its max sent count, it updates the task status to FAILED.
// If the task can be sent, it retrieves the corresponding client,
// sends the task request, and updates the task.
func (m *Manager) handleTask(ctx context.Context, wg *sync.WaitGroup, repo Repository, job Job, task Task) {
	defer wg.Done()

	ctx = slogctx.With(ctx, "externalID", job.ExternalID, "taskID", task.ID, "etag", task.ETag, "target",
		task.Target, "status", task.Status, "reconcileCount", task.ReconcileCount,
		"reconcileAfterSec", task.ReconcileAfterSec)

	if task.ReconcileCount >= m.Config.MaxReconcileCount {
		slogctx.Debug(ctx, "max reconcile count for task exceeded")
		task.ETag = uuid.NewString()
		task.Status = TaskStatusFailed
		repo.updateTask(ctx, task) //nolint:errcheck
		return
	}

	mgrTarget, ok := m.targets[task.Target]
	// this is an additional safeguard, this should never happen as we check for the existence of targets while creating tasks.
	if !ok || mgrTarget.Client == nil {
		errLog := "no managerTarget found for task target, marking task as failed"
		if mgrTarget.Client == nil {
			errLog = "no initiator client found for task target, marking task as failed"
		}
		slogctx.Warn(ctx, errLog)
		task.Status = TaskStatusFailed
		repo.updateTask(ctx, task) //nolint:errcheck
		return
	}

	task.LastReconciledAt = clock.NowUnixNano()
	task.ReconcileCount++
	task.Status = TaskStatusProcessing
	task.ReconcileAfterSec = retry.ExponentialBackoffInterval(
		m.Config.BackoffBaseIntervalSec,
		m.Config.BackoffMaxIntervalSec,
		task.ReconcileCount,
	)

	req := TaskRequest{
		TaskID:       task.ID,
		Type:         task.Type,
		ExternalID:   job.ExternalID,
		Data:         task.Data,
		WorkingState: task.WorkingState,
		ETag:         task.ETag,
	}

	if mgrTarget.Signer != nil {
		signature, err := mgrTarget.Signer.Sign(ctx, req)
		if err != nil {
			slogctx.Warn(ctx, "task request signing failed , marking task as failed")
			task.Status = TaskStatusFailed
			repo.updateTask(ctx, task) //nolint:errcheck
			return
		}
		req.addMeta(signature)
	}

	slogctx.Debug(ctx, "sending task request", slog.Any("request", req))

	if err := mgrTarget.Client.SendTaskRequest(ctx, req); err != nil {
		slogctx.Error(ctx, "failed to send task request", "error", err)
		repo.updateTask(ctx, task) //nolint:errcheck
		return
	}
	task.TotalSentCount++

	repo.updateTask(ctx, task) //nolint:errcheck
}

// updateJobAndCreateJobEvent updates the given job in the repository and records a job event.
// It logs errors if updating the job or creating the job event fails.
// Returns an error if any operation fails.
func (m *Manager) updateJobAndCreateJobEvent(ctx context.Context, repo Repository, job Job) error {
	err := m.recordJobTerminatedEvent(ctx, repo, job)
	if err != nil {
		slogctx.Error(ctx, "failed to record job terminated event", "error", err)
		return err
	}
	return repo.updateJob(ctx, job)
}

// startResponseReaders starts goroutines to handle responses from each target Initiator.
func (m *Manager) startResponseReaders(ctx context.Context) {
	for target, mgrTarget := range m.targets {
		go m.handleResponses(ctx, mgrTarget, target)
	}
}

// handleResponses continuously reads TaskResponse messages from a client.
func (m *Manager) handleResponses(ctx context.Context, mgrTarget ManagerTarget, target string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			resp, err := mgrTarget.Client.ReceiveTaskResponse(ctx)
			if err != nil {
				slogctx.Error(ctx, "error receiving task response from target", "error", err, "target", target)
				continue
			}

			newCtx := slogctx.With(ctx, "target", target, "externalID", resp.ExternalID, "taskID", resp.TaskID.String(), "etag", resp.ETag)
			slogctx.Debug(newCtx, "received task response")

			if mgrTarget.Verifier != nil {
				err = mgrTarget.Verifier.Verify(newCtx, resp)
				if err != nil {
					slogctx.Error(newCtx, "failed while verifying task response signature", "error", err)
					continue
				}
			}

			if err := m.processResponse(newCtx, resp); err != nil {
				slogctx.Error(newCtx, "failed to process task response", "error", err)
			}
		}
	}
}

// processResponse applies a TaskResponse to the corresponding Task in the repository.
func (m *Manager) processResponse(ctx context.Context, resp TaskResponse) error {
	return m.repo.transaction(ctx, func(txCtx context.Context, repo Repository) error {
		task, found, err := repo.getTaskForUpdate(txCtx, resp.TaskID)
		if err != nil {
			return err
		}

		if !found {
			return fmt.Errorf("%w, taskID: %s", ErrTaskNotFound, resp.TaskID)
		}

		if err != nil || !found {
			return err
		}
		txCtx = slogctx.With(txCtx, "externalID", resp.ExternalID, "taskID", task.ID, "etag", task.ETag)

		if resp.ETag != task.ETag {
			slogctx.Debug(txCtx, "discarding stale response")
			return nil
		}

		task.WorkingState = resp.WorkingState
		task.ETag = uuid.NewString() // Generate a new ETag for the updated task
		task.Status = TaskStatus(resp.Status)
		task.ReconcileAfterSec = resp.ReconcileAfterSec
		task.LastReconciledAt = clock.NowUnixNano() // Update the last reconciled time to now unix to reset it.
		task.ReconcileCount = 0                     // Reset the reconcile count since the task has been processed successfully.
		task.TotalReceivedCount++
		if task.Status == TaskStatusFailed {
			slogctx.Debug(txCtx, "task failed", "errorMessage", resp.ErrorMessage)
			task.ErrorMessage = resp.ErrorMessage
		}

		return repo.updateTask(txCtx, task)
	})
}
