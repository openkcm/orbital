package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"

	_ "github.com/lib/pq"

	stdsql "database/sql"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/store/sql"
)

// This example uses PostgreSQL as the database with following connection parameters.
var (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "secret"
	dbname   = "orbital"
	sslmode  = "disable"
)

// This is a simple resource struct.
type Resource struct {
	ID   string
	Data string
}

// This is a simple in-memory database to store resources.
var resourceDB = make(map[string]Resource)

func main() {
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

	// Create a orbital manager
	orbitalManager, err := orbital.NewManager(repo,
		// Register a task resolver function for jobs
		taskResolver(),
		// Register a confirm function for jobs
		orbital.WithJobConfirmFunc(jobConfirmFunc),
		// Register a job done event func
		orbital.WithJobDoneEventFunc(jobDoneEventFunc),
	)
	handleErr("Manager initialization failed", err)

	// Start the job manager
	err = orbitalManager.Start(ctx)
	handleErr("Failed to start job manager", err)

	// Create and store a resource
	resource := Resource{
		ID:   uuid.NewString(),
		Data: "resource-data",
	}
	resourceDB[resource.ID] = resource
	log.Printf("Resource stored: %s\n", resource.ID)

	// Prepare a job for the stored resource
	job := orbital.NewJob(resource.Data, []byte("RESOURCE_CREATED"))
	createdJob, err := orbitalManager.PrepareJob(ctx, job)
	handleErr("Failed to prepare job", err)

	// Check the jobs status for Ready
	checkJobStatus(ctx, orbitalManager, createdJob.ID, orbital.JobStatusReady)

	err = listTasks(ctx, orbitalManager, orbital.ListTasksQuery{
		JobID: createdJob.ID,
	})
	handleErr("Failed to fetch tasks", err)
}

func listTasks(ctx context.Context, manager *orbital.Manager, query orbital.ListTasksQuery) error {
	tasks, err := manager.ListTasks(ctx, query)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		log.Println("Task ID:", task.ID)
		log.Println("   Job ID:", task.JobID)
		log.Println("   Target:", task.Target)
	}
	return nil
}

func jobConfirmFunc(_ context.Context, _ orbital.Job) (orbital.JobConfirmResult, error) {
	return orbital.JobConfirmResult{Confirmed: true}, nil
}

func jobDoneEventFunc(_ context.Context, job orbital.Job) error {
	log.Printf("Job %s event func called with status %s\n", job.ID, job.Status)
	return nil
}

func taskResolver() orbital.TaskResolveFunc {
	return func(_ context.Context, _ orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
		return orbital.TaskResolverResult{
			TaskInfos: []orbital.TaskInfo{
				{
					Data:   []byte("data-1"),
					Type:   "type-1",
					Target: "target-1",
				},
				{
					Data:   []byte("data-2"),
					Type:   "type-2",
					Target: "target-2",
				},
			},
			Done: true,
		}, nil
	}
}

func checkJobStatus(ctx context.Context, manager *orbital.Manager, jobID uuid.UUID, state orbital.JobStatus) {
	log.Println("Checking for following JobStatus", state)
	for {
		job, _, err := manager.GetJob(ctx, jobID)
		if err != nil {
			log.Printf("Error getting job: %s\n", err)
			return
		}

		log.Printf("Job status: %s\n", job.Status)

		if job.Status == state {
			break
		}

		time.Sleep(1 * time.Second)
	}
}

func handleErr(msg string, err error) {
	if err != nil {
		log.Fatalf("%s: %s\n", msg, err)
	}
}
