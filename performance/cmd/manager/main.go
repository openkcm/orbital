package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/interactortest"
	"github.com/openkcm/orbital/store/sql"

	_ "github.com/lib/pq"

	stdsql "database/sql"
)

type termination struct {
	failures      atomic.Int64
	dones         atomic.Int64
	cancellations atomic.Int64
}

func main() {
	log.Println("Starting orbital manager performance test...")
	cfg := newConfig()

	go startPrometheusServer(cfg.prometheusPort)

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		cfg.postgres.host, cfg.postgres.port, cfg.postgres.user,
		cfg.postgres.password, cfg.postgres.dbname, cfg.postgres.sslmode)
	db, err := stdsql.Open("postgres", connStr)
	handleErr("Failed to create database handle", err)
	defer db.Close()

	ctx := context.Background()

	store, err := sql.New(ctx, db)
	handleErr("Failed to create store", err)
	repo := orbital.NewRepository(store)

	targetToClient := make(map[string]orbital.Initiator, cfg.targetsNum)
	for i := range cfg.targetsNum {
		target := targetName(i)
		client, err := operatorMock(cfg.operatorMock)
		handleErr("Failed to create client for target "+target, err)

		targetToClient[target] = client
	}

	termination := &termination{}
	manager, err := orbital.NewManager(
		repo,
		taskResolveFunc(cfg.targetsNum, cfg.taskResolveFunc),

		orbital.WithJobConfirmFunc(jobConfirmFunc(cfg.jobConfirmFunc)),

		orbital.WithTargetClients(targetToClient),

		orbital.WithJobDoneEventFunc(jobTerminateFunc(&termination.dones)),
		orbital.WithJobFailedEventFunc(jobTerminateFunc(&termination.failures)),
		orbital.WithJobCanceledEventFunc(jobTerminateFunc(&termination.cancellations)),
	)
	handleErr("Failed to create orbital manager", err)
	manager.Config = cfg.manager

	before := time.Now()
	log.Printf("Preparing %d jobs with %d targets each...\n", cfg.jobsNum, cfg.targetsNum)
	for i := range cfg.jobsNum {
		job := orbital.NewJob("test-job", []byte{})

		_, err = manager.PrepareJob(ctx, job)
		if err != nil {
			log.Printf("Failed to prepare job %d: %s\n", i, err)
		}
	}

	err = manager.Start(ctx)
	handleErr("Failed to start orbital manager", err)

	ctxTimeout, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()
	checkForTermination(ctxTimeout, termination, cfg.jobsNum)

	log.Printf("All jobs terminated after %s\n", time.Since(before).Truncate(time.Millisecond))

	select {}
}

func startPrometheusServer(port string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	err := http.ListenAndServe(":"+port, mux)
	handleErr("Failed to start http server", err)
}

func jobConfirmFunc(cfg jobConfirmFuncConfig) orbital.JobConfirmFunc {
	return func(_ context.Context, _ orbital.Job) (orbital.JobConfirmResult, error) {
		errRand := rand.Float64()
		if errRand <= cfg.errorRate {
			return orbital.JobConfirmResult{}, errors.New("job confirm error")
		}

		timeRand := randIntN(cfg.latencyAverageSec * 2)
		time.Sleep(time.Duration(timeRand) * time.Second)

		cancelRand := rand.Float64()
		if cancelRand <= cfg.cancelRate {
			return orbital.JobConfirmResult{
				Confirmed: false,
			}, nil
		}

		return orbital.JobConfirmResult{
			Confirmed: true,
		}, nil
	}
}

func taskResolveFunc(targetsNum int, cfg taskResolveFuncConfig) orbital.TaskResolveFunc {
	return func(_ context.Context, _ orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
		errRand := rand.Float64()
		if errRand <= cfg.errorRate {
			return orbital.TaskResolverResult{}, errors.New("task resolve error")
		}

		timeRand := randIntN(cfg.latencyAverageSec * 2)
		time.Sleep(time.Duration(timeRand) * time.Second)

		unfinishedRand := rand.Float64()
		if unfinishedRand <= cfg.unfinishedRate {
			return orbital.TaskResolverResult{
				Done: false,
			}, nil
		}

		cancelRand := rand.Float64()
		if cancelRand <= cfg.cancelRate {
			return orbital.TaskResolverResult{
				IsCanceled: true,
			}, nil
		}

		infos := make([]orbital.TaskInfo, targetsNum)
		for i := range targetsNum {
			infos[i] = orbital.TaskInfo{
				Data:   fmt.Appendf(nil, "task-%d", i),
				Type:   "test-task",
				Target: targetName(i),
			}
		}

		return orbital.TaskResolverResult{
			TaskInfos: infos,
			Done:      true,
		}, nil
	}
}

func targetName(i int) string {
	return fmt.Sprintf("target-%d", i)
}

func jobTerminateFunc(terminatedJobs *atomic.Int64) orbital.JobTerminatedEventFunc {
	return func(_ context.Context, _ orbital.Job) error {
		terminatedJobs.Add(1)
		return nil
	}
}

func operatorMock(cfg operatorMockConfig) (*interactortest.Initiator, error) {
	return interactortest.NewInitiator(
		func(_ context.Context, request orbital.TaskRequest) (orbital.TaskResponse, error) {
			errRand := rand.Float64()
			if errRand <= cfg.errorRate {
				return orbital.TaskResponse{}, errors.New("client error")
			}

			timeRand := randIntN(cfg.latencyAverageSec * 2)
			time.Sleep(time.Duration(timeRand) * time.Second)

			response := orbital.TaskResponse{
				TaskID: request.TaskID,
				ETag:   request.ETag,
				Type:   request.Type,
			}

			retryRand := rand.Float64()
			if retryRand <= cfg.unfinishedRate {
				response.ReconcileAfterSec = int64(rand.IntN((cfg.latencyAverageSec + 1) * 2))
				response.Status = string(orbital.TaskStatusProcessing)
				return response, nil
			}

			taskFailRand := rand.Float64()
			if taskFailRand <= cfg.taskFailureRate {
				response.Status = string(orbital.TaskStatusFailed)
				return response, nil
			}

			response.Status = string(orbital.TaskStatusDone)
			return response, nil
		}, nil,
	)
}

func checkForTermination(ctx context.Context, termination *termination, expTerminated int) {
	log.Println("Checking for job termination...")
	for {
		select {
		case <-ctx.Done():
			log.Println("Context cancelled, exiting...")
			return
		default:
			dones := termination.dones.Load()
			failures := termination.failures.Load()
			cancellations := termination.cancellations.Load()

			sum := dones + failures + cancellations
			log.Printf("Jobs terminated: %d/%d; done: %d; failures: %d; cancellations: %d\n",
				sum, expTerminated, dones, failures, cancellations)
			if sum >= int64(expTerminated) {
				return
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func randIntN(n int) int {
	if n <= 0 {
		return 0
	}
	return rand.IntN(n)
}

func handleErr(msg string, err error) {
	if err != nil {
		log.Fatalf("%s: %s\n", msg, err)
	}
}
