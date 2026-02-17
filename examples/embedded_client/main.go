package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"

	stdsql "database/sql"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/client/embedded"
	"github.com/openkcm/orbital/store/sql"
)

// This example uses PostgreSQL as the database with following connection parameters.
// Use `make docker-compose-up` to start the database.
var (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "secret"
	dbname   = "orbital"
	sslmode  = "disable"
)

var (
	jobType1 = "type1"
	jobType2 = "type2"

	target1 = "target1"
	target2 = "target2"

	target1Called atomic.Uint32
	target2Called atomic.Uint32
)

var errUnknownJobType = errors.New("unknown job type")

func main() {
	targets := map[string]orbital.TargetManager{}
	var err error

	// Create embedded clients with handlers
	client1, err := embedded.NewClient(handler1)
	handleErr("Failed to create client1", err)
	targets[target1] = orbital.TargetManager{Client: client1}
	client2, err := embedded.NewClient(handler2)
	handleErr("Failed to create client2", err)
	targets[target2] = orbital.TargetManager{Client: client2}

	// Create PostgreSQL database handle
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		host, port, user, password, dbname, sslmode)
	db, err := stdsql.Open("postgres", connStr)
	handleErr("Failed to create database handle", err)
	defer db.Close()

	ctx := context.Background()

	// Create a repository
	store, err := sql.New(ctx, db)
	handleErr("Failed to create store", err)

	repo := orbital.NewRepository(store)

	// Declare termination function and channel to receive terminated jobs
	terminated := make(chan orbital.Job)
	terminateFunc := func(_ context.Context, job orbital.Job) error {
		terminated <- job
		return nil
	}
	// Create a manager with the repository, embedded clients and termination function
	manager, err := orbital.NewManager(repo,
		resolveTask,
		orbital.WithTargets(targets),
		orbital.WithJobDoneEventFunc(terminateFunc),
		orbital.WithJobFailedEventFunc(terminateFunc),
		orbital.WithJobCanceledEventFunc(terminateFunc),
	)
	handleErr("Failed to create manager", err)
	// Adjust manager configuration
	manager.Config.ConfirmJobWorkerConfig.ExecInterval = 1 * time.Second
	manager.Config.CreateTasksWorkerConfig.ExecInterval = 1 * time.Second
	manager.Config.ReconcileWorkerConfig.ExecInterval = 1 * time.Second
	manager.Config.NotifyWorkerConfig.ExecInterval = 1 * time.Second

	err = manager.Start(ctx)
	handleErr("Failed to start manager", err)

	// Prepare jobs
	jobTypes := []string{jobType1, jobType2}
	for _, t := range jobTypes {
		_, err := manager.PrepareJob(ctx, orbital.Job{
			Type: t,
			Data: []byte{},
		})
		handleErr("Failed to prepare job", err)
	}

	// Wait for jobs to be terminated
	for range jobTypes {
		job := <-terminated
		log.Printf("Job with type %s, status: %s", job.Type, job.Status)
	}

	time.Sleep(1 * time.Second) // time to update notification status after termination
}

func resolveTask(_ context.Context, job orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
	targets := map[string]string{
		jobType1: target1,
		jobType2: target2,
	}
	target, ok := targets[job.Type]
	if !ok {
		return nil, errUnknownJobType
	}
	return orbital.CompleteTaskResolver().
		WithTaskInfo([]orbital.TaskInfo{
			{
				Data:   job.Data,
				Type:   job.Type,
				Target: target,
			},
		}), nil
}

func handler1(_ context.Context, req orbital.HandlerRequest, resp *orbital.HandlerResponse) {
	count := target1Called.Add(1)
	if count == 5 {
		resp.Complete()
		return
	}
	log.Printf("Handling task with type %s, count: %d", req.TaskType, count)
	// default: resp.ContinueAndWaitFor(0) to continue immediately
}

func handler2(_ context.Context, req orbital.HandlerRequest, resp *orbital.HandlerResponse) {
	count := target2Called.Add(1)
	if count == 5 {
		resp.Fail("failed at count 5")
		return
	}
	log.Printf("Handling task with type %s, count: %d", req.TaskType, count)
	// default: resp.ContinueAndWaitFor(0) to continue immediately
}

func handleErr(msg string, err error) {
	if err != nil {
		log.Fatalf("%s: %s\n", msg, err)
	}
}
