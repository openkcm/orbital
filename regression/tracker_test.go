package regression_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	slogctx "github.com/veqryn/slog-context"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/client/amqp"
)

// ManagerTracker tracks the state and statistics of a Manager instance
// during integration tests. It holds references to the manager, database,
// AMQP clients, and counters for various job states.
type ManagerTracker struct {
	name             string
	manager          *orbital.Manager
	db               *sql.DB
	client           []*amqp.Client        // information about all the target amqp clients.
	noOfJobConfirmed *counter[orbital.Job] // tracks number of confirmed jobs.
	noOfJobResolved  *counter[orbital.Job] // tracks number of resolved jobs.
	noOfJobDone      *counter[orbital.Job] // tracks number of done jobs.
	noOfJobCanceled  *counter[orbital.Job] // tracks number of canceled jobs.
	noOfJobFailed    *counter[orbital.Job] // tracks number of failed jobs.
}

// OperatorTracker tracks the state and statistics of an Operator instance
// during integration tests. It holds references to the operator, AMQP client,
// and a counter for the number of processed tasks.
type OperatorTracker struct {
	name              string
	operator          *orbital.Operator
	client            *amqp.Client                     // information about the amqp client.
	noOfTaskProcessed *counter[orbital.HandlerRequest] // tracks number of processed tasks.
}

// toCount is a type constraint that matches either orbital.HandlerRequest or orbital.Job.
// It is used for generic functions that operate on these types.
type toCount interface {
	orbital.HandlerRequest | orbital.Job
}

// counter is a generic thread-safe counter that maps string keys to slices
// of values of type T. It uses a RWMutex to protect concurrent access.
type counter[T toCount] struct {
	mux   sync.RWMutex
	value map[string][]T
}

// NewManagerTracker creates and initializes a ManagerTracker for testing purposes.
// It sets up AMQP clients for each operator, configures job and task tracking counters,
// and returns the constructed ManagerTracker instance.
func NewManagerTracker(ctx context.Context,
	env *testEnvironment,
	name string,
	noOfOperator int,
	terminalEventChan chan struct{},
) (*ManagerTracker, error) {
	tracker := &ManagerTracker{
		name:             name,
		client:           []*amqp.Client{},
		noOfJobConfirmed: newCounter[orbital.Job](),
		noOfJobResolved:  newCounter[orbital.Job](),
		noOfJobDone:      newCounter[orbital.Job](),
		noOfJobCanceled:  newCounter[orbital.Job](),
		noOfJobFailed:    newCounter[orbital.Job](),
	}

	// initializes the targets
	targets := make(map[string]orbital.TargetManager, noOfOperator)
	for _, operatorName := range deriveOperatorNames(noOfOperator) {
		toMgr, toOpr := deriveQueueName(operatorName)
		client, err := createAMQPClient(ctx, env, toOpr, toMgr)
		if err != nil {
			return tracker, err
		}
		tracker.client = append(tracker.client, client)
		targets[operatorName] = orbital.TargetManager{
			Client: client,
		}
	}

	// initializes the task resolver
	taskResolver := func(_ context.Context, job orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
		tracker.noOfJobResolved.Add(job.ID.String(), job)

		taskInfos := make([]orbital.TaskInfo, 0, noOfOperator)
		for _, operatorName := range deriveOperatorNames(noOfOperator) {
			taskInfos = append(taskInfos, orbital.TaskInfo{
				Data:   []byte(addJobIDAndOperator(job.ID.String(), operatorName)),
				Type:   deriveTaskType(operatorName),
				Target: operatorName,
			})
		}

		return orbital.CompleteTaskResolver().WithTaskInfo(taskInfos), nil
	}

	// initializes the terminal functions
	opts := []orbital.ManagerOptsFunc{
		orbital.WithJobConfirmFunc(func(ctx context.Context, job orbital.Job) (orbital.JobConfirmerResult, error) {
			tracker.noOfJobConfirmed.Add(job.ID.String(), job)
			slogctx.Info(ctx, "confirmFunc", "managerName", tracker.name)
			return orbital.CompleteJobConfirmer(), nil
		}),
		orbital.WithJobDoneEventFunc(func(ctx context.Context, job orbital.Job) error {
			tracker.noOfJobDone.Add(job.ID.String(), job)
			slogctx.Info(ctx, "jobDone", "managerName", tracker.name, "jobID", job.ID)
			terminalEventChan <- struct{}{}
			return nil
		}),
		orbital.WithJobFailedEventFunc(func(ctx context.Context, job orbital.Job) error {
			tracker.noOfJobFailed.Add(job.ID.String(), job)
			slogctx.Info(ctx, "jobFailed", "managerName", tracker.name, "jobID", job.ID)
			terminalEventChan <- struct{}{}
			return nil
		}),
		orbital.WithJobCanceledEventFunc(func(ctx context.Context, job orbital.Job) error {
			tracker.noOfJobCanceled.Add(job.ID.String(), job)
			slogctx.Info(ctx, "jobCanceled", "managerName", tracker.name, "jobID", job.ID)
			terminalEventChan <- struct{}{}
			return nil
		}),
		orbital.WithTargets(targets),
	}

	// initializes the orbital store
	store, db, err := createOrbitalStore(ctx, env)
	if err != nil {
		return nil, err
	}
	tracker.db = db

	// initializes the manager
	manager, err := orbital.NewManager(orbital.NewRepository(store), taskResolver, opts...)

	manager.Config.ConfirmJobWorkerConfig.ExecInterval = 500 * time.Millisecond
	manager.Config.CreateTasksWorkerConfig.ExecInterval = 500 * time.Millisecond
	manager.Config.ReconcileWorkerConfig.ExecInterval = 500 * time.Millisecond
	manager.Config.NotifyWorkerConfig.ExecInterval = 500 * time.Millisecond

	tracker.manager = manager
	return tracker, err
}

// Cleanup closes all AMQP clients and the database associated with the ManagerTracker.
// It should be called to release resources after tests are complete.
func (me *ManagerTracker) Cleanup(ctx context.Context) {
	for _, cli := range me.client {
		cli.Close(ctx)
	}
	me.db.Close()
}

// NewOperatorTracker creates and initializes an OperatorTracker for testing purposes.
// It sets up an AMQP client, creates an orbital.Operator, and registers a handler
// that tracks the number of processed tasks. The handler always returns a ResultDone response.
func NewOperatorTracker(ctx context.Context, env *testEnvironment, name string) (*OperatorTracker, error) {
	tracker := &OperatorTracker{
		name:              name,
		noOfTaskProcessed: newCounter[orbital.HandlerRequest](),
	}

	toMgr, toOpr := deriveQueueName(name)
	client, err := createAMQPClient(ctx, env, toMgr, toOpr)
	if err != nil {
		return tracker, err
	}

	tracker.client = client
	operator, err := orbital.NewOperator(orbital.TargetOperator{Client: client})
	if err != nil {
		return tracker, fmt.Errorf("failed to create operator: %w", err)
	}
	tracker.operator = operator

	handler := func(ctx context.Context, req orbital.HandlerRequest, resp *orbital.HandlerResponse) error {
		tracker.noOfTaskProcessed.Add(req.TaskID.String(), req)
		slogctx.Info(ctx, "processing task", "operatorName", tracker.name, "data", string(req.Data))
		resp.Result = orbital.ResultDone
		return nil
	}

	taskType := deriveTaskType(name)
	err = tracker.operator.RegisterHandler(taskType, handler)
	if err != nil {
		return tracker, fmt.Errorf("failed to register handler for %s: %w", taskType, err)
	}
	return tracker, nil
}

// Cleanup closes the AMQP client associated with the OperatorTracker.
// It should be called to release resources after tests are complete.
func (me *OperatorTracker) Cleanup(ctx context.Context) {
	me.client.Close(ctx)
}

func newCounter[T toCount]() *counter[T] {
	return &counter[T]{
		mux:   sync.RWMutex{},
		value: make(map[string][]T),
	}
}

func (me *counter[T]) Add(key string, data T) {
	me.mux.Lock()
	defer me.mux.Unlock()
	v, ok := me.value[key]
	if !ok {
		me.value[key] = []T{data}
		return
	}
	v = append(v, data)
	me.value[key] = v
}

func deriveQueueName(operatorName string) (string, string) {
	return "toManager" + operatorName, "toOperator" + operatorName
}

func deriveTaskType(operationName string) string {
	return "taskType" + operationName
}

func deriveOperatorNames(noOfOperator int) []string {
	res := make([]string, 0, noOfOperator)
	for i := range noOfOperator {
		res = append(res, fmt.Sprintf("operator%d", i))
	}
	return res
}

func deriveManagerNames(noOfManager int) []string {
	res := make([]string, 0, noOfManager)
	for i := range noOfManager {
		res = append(res, fmt.Sprintf("manager%d", i))
	}
	return res
}

func addJobIDAndOperator(jobID, operatorName string) string {
	return jobID + "#" + operatorName
}

var errExtractJobIDAndOperator = errors.New("error while extracting JobID and Operator")

func extractJobIDAndOperator(s string) (string, string, error) {
	jobAndOperator := strings.Split(s, "#")
	if len(jobAndOperator) != 2 {
		return "", "", errExtractJobIDAndOperator
	}
	return jobAndOperator[0], jobAndOperator[1], nil
}
