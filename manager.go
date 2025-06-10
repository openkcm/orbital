package orbital

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/openkcm/orbital/internal/worker"
)

type (
	// Manager is the interface for managing jobs,
	// including their creation, state transitions, and lifecycle handling.
	Manager struct {
		repo                    *Repository
		Config                  *Config
		confirmFunc             JobConfirmFunc
		taskResolver            TaskResolverFunc
		targetToInitiator       map[string]Initiator
		JobTerminationEventFunc TerminationEventFunc
	}
	// TerminationEventFunc defines a callback function type for sending job events.
	TerminationEventFunc func(ctx context.Context, job Job) error

	// ManagerOptsFunc is a function type to configure Manager options.
	ManagerOptsFunc func(mgr *Manager)

	// JobConfirmFunc defines a function that determines whether a job can be confirmed.
	// It returns a ConfirmResult struct with the confirmation result and an error if the process fails.
	JobConfirmFunc func(ctx context.Context, job Job) (JobConfirmResult, error)
	// JobConfirmResult represents the result of a job confirmation operation.
	JobConfirmResult struct {
		Confirmed bool
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
		// IsAborted indicates whether the job needs to be aborted.
		IsAborted bool
		// AbortedErrorMessage is the error message if the job was aborted.
		AbortedErrorMessage string
	}

	// TaskResolverFunc is a  function type that resolves targets for the creation tasks for a job and the cursor.
	TaskResolverFunc func(ctx context.Context, job Job, cursor TaskResolverCursor) (TaskResolverResult, error)

	// Config contains configuration for job processing.
	Config struct {
		// JobsLimitNum is the maximum number of jobs to process at once.
		JobsLimitNum int
		// TaskLimitNum is the maximum number of tasks to process at once.
		TaskLimitNum int
		// ConfirmJobInterval is the interval between jobs scheduler attempts.
		ConfirmJobInterval time.Duration
		// JobConfig contains configuration for job processing.
		// ConfirmJobWorkerConfig holds the configuration for the job confirmation worker.
		ConfirmJobWorkerConfig WorkerConfig
		// CreateTasksWorkerConfig holds the configuration for the task creation worker.
		CreateTasksWorkerConfig WorkerConfig
		// ReconcilesWorkerConfig holds the configuration for the reconciliation worker.
		ReconcileWorkerConfig WorkerConfig
		// NotifyWorkerConfig holds the configuration for the notification worker.
		NotifyWorkerConfig WorkerConfig
		// ConfirmJobDelay is the delay before confirming a job.
		ConfirmJobDelay time.Duration
		// ConfirmJobTimeout is the timeout for jobs confirmation attempts.
		ConfirmJobTimeout time.Duration
		// CreateTasksInterval is the interval for task creation.
		CreateTasksInterval time.Duration
		// ProcessingJobDelay is the delay before processing jobs in reconcileTasks.
		ProcessingJobDelay time.Duration
	}
)

// Default values for configs.
const (
	defConfirmJobDelay     = 5 * time.Second
	defNoOfWorker          = 5
	defWorkTimeout         = 5 * time.Second
	defTaskLimitNum        = 500
	defWorkExecInterval    = 10 * time.Second
	defReconcileInterval   = 5 * time.Second
	defProcessingJobDelay  = 5 * time.Second
	defProcessingTaskDelay = 5 * time.Second
)

// Internal constants.
const (
	repoOpTimeout = 2 * time.Second
)

var ErrTaskResolverNotSet = errors.New("taskResolver not set")

var ErrMsgFailedTasks = "job has failed tasks"

// NewManager creates a new Manager instance.
func NewManager(repo *Repository, taskResolver TaskResolverFunc, optFuncs ...ManagerOptsFunc) (*Manager, error) {
	if taskResolver == nil {
		return nil, ErrTaskResolverNotSet
	}
	mgr := &Manager{
		repo:         repo,
		taskResolver: taskResolver,
		Config: &Config{
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
			ConfirmJobDelay:    defConfirmJobDelay,
			ProcessingJobDelay: defProcessingJobDelay,
			TaskLimitNum:       defTaskLimitNum,
		},
		confirmFunc: func(_ context.Context, _ Job) (JobConfirmResult, error) {
			return JobConfirmResult{
				Confirmed: true,
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
		opts.confirmFunc = f
	}
}

// WithTasksLimitNum sets Managers JobsLimitNum param.
func WithTasksLimitNum(n int) ManagerOptsFunc {
	return func(opts *Manager) {
		opts.Config.TaskLimitNum = n
	}
}

// WithTargetClients set the map that maps the target string to its Initiator.
func WithTargetClients(targetToInitiators map[string]Initiator) ManagerOptsFunc {
	return func(m *Manager) {
		m.targetToInitiator = targetToInitiators
	}
}

// WithProcessingJobDelay sets the delay before processing jobs in reconcileTasks.
func WithProcessingJobDelay(d time.Duration) ManagerOptsFunc {
	return func(m *Manager) {
		m.Config.ProcessingJobDelay = d
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
				Fn:           m.sendJobTerminationEvent,
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
func (m *Manager) PrepareJob(ctx context.Context, job Job) (Job, error) {
	job.Status = JobStatusCreated
	return m.repo.createJob(ctx, job)
}

// GetJob retrieves a job by its ID from the repository.
func (m *Manager) GetJob(ctx context.Context, jobID uuid.UUID) (Job, bool, error) {
	return m.repo.getJob(ctx, jobID)
}

// ListTasks retrieves tasks by query from the repository.
func (m *Manager) ListTasks(ctx context.Context, query ListTasksQuery) ([]Task, error) {
	return m.repo.listTasks(ctx, query)
}

// confirmJob processes jobs in the CREATED state that were created before a specified delay
// and attempts to confirm them within a transaction.
func (m *Manager) confirmJob(ctx context.Context) error {
	return m.repo.transaction(ctx, func(ctx context.Context, repo Repository) error {
		listCtx, cancel := context.WithTimeout(ctx, repoOpTimeout)
		defer cancel()
		nowUTC := time.Now().UTC()
		jobs, err := repo.listJobs(listCtx, ListJobsQuery{
			Status:             JobStatusCreated,
			CreatedAt:          nowUTC.Add(-m.Config.ConfirmJobDelay).Unix(),
			Limit:              1,
			RetrievalModeQueue: true,
			OrderByUpdatedAt:   true,
		})
		if err != nil {
			slog.Error("confirmJob listJobs", "error", err)
			return err
		}
		if len(jobs) == 0 {
			slog.Debug("no jobs to confirm")
			return nil
		}
		job := jobs[0]

		err = m.handleConfirmJob(ctx, repo, job)
		if err != nil {
			slog.Error("handleConfirmJob", "error", err)
		}
		return err
	})
}

// handleConfirmJob executes the confirmation function for a job and updates its state
// based on the confirmation result returned by the function.
func (m *Manager) handleConfirmJob(ctx context.Context, repo Repository, job Job) error {
	res, err := m.confirmFunc(ctx, job)
	if err != nil {
		// NOTE: here we update the job to change the updated_at timestamp in order to spread the fetching of jobs.
		return repo.updateJob(ctx, job)
	}
	job.Status = JobStatusConfirmed
	if !res.Confirmed {
		job.Status = JobStatusCanceled
	}
	updateCtx, cancel := context.WithTimeout(ctx, repoOpTimeout)
	defer cancel()
	return repo.updateJob(updateCtx, job)
}

// createTask orchestrates the creation of tasks for jobs in the repository.
// It retrieves jobs, resolves their targets, creates tasks, updates job states,
// and manages job cursors within a transactional context.
func (m *Manager) createTask(ctx context.Context) error {
	return m.repo.transaction(ctx, func(ctx context.Context, repo Repository) error {
		job, ok, err := jobForTaskCreation(ctx, repo)
		if err != nil {
			return err
		}

		if !ok {
			slog.Debug("no jobs found for task creation")
			return nil
		}

		jobCursor, found, err := jobCursor(ctx, repo, job.ID)
		if err != nil {
			slog.Error("jobCursor", "error", err, "jobID", job.ID)
			return err
		}

		resolverResult, err := m.taskResolver(ctx, job, jobCursor.Cursor)
		if err != nil {
			slog.Error("taskResolver", "error", err, "jobID", job.ID)
			// NOTE: here we update the job to change the updated_at timestamp in order to spread the fetching of jobs.
			return repo.updateJob(ctx, job)
		}
		if resolverResult.IsAborted {
			job.Status = JobStatusAborted
			job.ErrorMessage = resolverResult.AbortedErrorMessage
			return repo.updateJob(ctx, job)
		}

		jobCursor.Cursor = resolverResult.Cursor
		_, err = repo.createTasks(ctx, newTasks(job.ID, resolverResult.TaskInfos))
		if err != nil {
			slog.Error("createTasks", "error", err, "jobID", job.ID)
			return err
		}

		err = createOrUpdateCursor(ctx, repo, found, jobCursor)
		if err != nil {
			slog.Error("createOrUpdateCursor", "error", err, "jobID", job.ID)
			return err
		}

		job.Status = JobStatusResolving
		if resolverResult.Done {
			job.Status = JobStatusReady
		}
		err = repo.updateJob(ctx, job)
		if err != nil {
			slog.Error("updateJob", "error", err, "jobID", job.ID)
			return err
		}
		return nil
	})
}

// createOrUpdateCursor creates or updates a JobCursor in the repository.
// If the JobCursor is found, it updates the existing record; otherwise, it creates a new one.
func createOrUpdateCursor(ctx context.Context, repo Repository, found bool, jobCursor JobCursor) error {
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
func jobForTaskCreation(ctx context.Context, repo Repository) (Job, bool, error) {
	var empty Job
	utcUnix := time.Now().UTC().Unix()
	jobs, err := repo.listJobs(ctx, ListJobsQuery{
		StatusIn:           []JobStatus{JobStatusResolving, JobStatusConfirmed},
		CreatedAt:          utcUnix,
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

// jobCursor retrieves the JobCursor for a given job ID from the repository.
// If the JobCursor is not found, it initializes a new JobCursor with the provided job ID.
func jobCursor(ctx context.Context, repo Repository, jobID uuid.UUID) (JobCursor, bool, error) {
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
// It terminates the job if any of its tasks have failed or all tasks are done.
//
//nolint:funlen
func (m *Manager) reconcile(ctx context.Context) error {
	return m.repo.transaction(ctx, func(ctx context.Context, repo Repository) error {
		job, err := m.getJobForReconcile(ctx, repo)
		if err != nil {
			return err
		}
		if job.ID == uuid.Nil {
			slog.Debug("no jobs ready for reconciliation")
			return nil
		}
		job.Status = JobStatusProcessing

		tasks, err := repo.listTasks(ctx, ListTasksQuery{
			JobID:    job.ID,
			Limit:    1,
			StatusIn: []TaskStatus{TaskStatusFailed},
		})
		if err != nil {
			return err
		}

		if len(tasks) > 0 {
			job.Status = JobStatusFailed
			job.ErrorMessage = ErrMsgFailedTasks
			return m.updateJobAndCreateJobEvent(ctx, repo, job)
		}

		tasks, err = repo.listTasks(ctx, ListTasksQuery{
			JobID:              job.ID,
			RetrievalModeQueue: true,
			IsReconcileReady:   true,
			Limit:              m.Config.TaskLimitNum,
			StatusIn:           []TaskStatus{TaskStatusCreated, TaskStatusProcessing},
			OrderByUpdatedAt:   true,
		})
		if err != nil {
			return err
		}

		if len(tasks) == 0 {
			job.Status = JobStatusDone
			return m.updateJobAndCreateJobEvent(ctx, repo, job)
		}

		var wg sync.WaitGroup
		for _, task := range tasks {
			initiator, ok := m.targetToInitiator[task.Target]
			if !ok {
				slog.Warn("no initiator for task target", "target", task.Target, "taskID", task.ID)
				continue
			}

			wg.Add(1)
			go m.sendRequestAndUpdateTask(ctx, &wg, repo, initiator, task)
		}
		wg.Wait()

		if err := repo.updateJob(ctx, job); err != nil {
			slog.Error("updateJob", "error", err, "jobID", job.ID)
			return err
		}

		return nil
	})
}

func (m *Manager) getJobForReconcile(ctx context.Context, repo Repository) (Job, error) {
	jobs, err := repo.listJobs(ctx, ListJobsQuery{
		StatusIn:           []JobStatus{JobStatusReady, JobStatusProcessing},
		CreatedAt:          time.Now().Unix(),
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

// sendRequestAndUpdateTask sends a TaskRequest to the client and updates the Task.
func (m *Manager) sendRequestAndUpdateTask(ctx context.Context, wg *sync.WaitGroup, repo Repository, client Initiator, task Task) {
	defer wg.Done()

	task.LastSentAt = time.Now().Unix()
	task.SentCount++
	task.Status = TaskStatusProcessing

	req := TaskRequest{
		TaskID:       task.ID,
		Type:         task.Type,
		Data:         task.Data,
		WorkingState: task.WorkingState,
		ETag:         task.ETag,
	}

	if err := client.SendTaskRequest(ctx, req); err != nil {
		slog.Error("sendRequestAndUpdateTask", "error", err, "taskID", task.ID)
		return
	}

	if err := repo.updateTask(ctx, task); err != nil {
		slog.Error("updateTask", "error", err, "taskID", task.ID)
	}
}

// updateJobAndCreateJobEvent updates the given job in the repository and records a job event.
// It logs errors if updating the job or creating the job event fails.
// Returns an error if any operation fails.
func (m *Manager) updateJobAndCreateJobEvent(ctx context.Context, repo Repository, job Job) error {
	if err := repo.updateJob(ctx, job); err != nil {
		slog.Error("updateJob", "error", err, "jobID", job.ID)
		return err
	}

	_, err := repo.createJobEvent(ctx, JobEvent{ID: job.ID})
	if err != nil {
		slog.Error("createJobEvent", "error", err, "jobID", job.ID)
		return err
	}
	return nil
}

// startResponseReaders starts goroutines to handle responses from each target Initiator.
func (m *Manager) startResponseReaders(ctx context.Context) {
	for target, initiator := range m.targetToInitiator {
		go m.handleResponses(ctx, initiator, target)
	}
}

// handleResponses continuously reads TaskResponse messages from a client.
func (m *Manager) handleResponses(ctx context.Context, client Initiator, target string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			resp, err := client.ReceiveTaskResponse(ctx)
			if err != nil {
				slog.Error("receiveTask", "error", err, "target", target)
				continue
			}
			if err := m.processResponse(ctx, resp); err != nil {
				slog.Error("processResponse", "error", err, "taskID", resp.TaskID)
			}
		}
	}
}

// processResponse applies a TaskResponse to the corresponding Task in the repository.
func (m *Manager) processResponse(ctx context.Context, resp TaskResponse) error {
	return m.repo.transaction(ctx, func(txCtx context.Context, repo Repository) error {
		task, found, err := repo.getTask(txCtx, resp.TaskID)
		if err != nil || !found {
			return err
		}

		if resp.ETag != task.ETag {
			slog.Debug("discarding stale response", "taskID", task.ID)
			return nil
		}

		task.WorkingState = resp.WorkingState
		task.ETag = uuid.NewString() // Generate a new ETag for the updated task
		task.Status = TaskStatus(resp.Status)
		task.ReconcileAfterSec = resp.ReconcileAfterSec

		return repo.updateTask(txCtx, task)
	})
}
