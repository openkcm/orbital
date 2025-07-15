package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
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
	cfg := newEnvConfig()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		cfg.postgres.host, cfg.postgres.port, cfg.postgres.user,
		cfg.postgres.password, cfg.postgres.dbname, cfg.postgres.sslmode)
	db, err := stdsql.Open("postgres", connStr)
	handleErr("Failed to create database handle", err)
	defer db.Close()

	ctx := context.Background()

	var manager *orbital.Manager
	store, err := sql.New(ctx, db)
	handleErr("Failed to create store", err)
	repo := orbital.NewRepository(store)

	var muxtex sync.Mutex
	var managerCancelFunc context.CancelFunc
	var managerCtx context.Context

	mux.HandleFunc("/orbital/run", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		handleErr("Failed to read request body", err)

		testCfg := TestConfig{}
		err = json.Unmarshal(body, &testCfg)
		handleErr("Failed to unmarshal request body", err)

		muxtex.Lock()
		if managerCancelFunc != nil {
			managerCancelFunc()
		}
		_, err = db.ExecContext(ctx, "DELETE from jobs")
		handleErr("Failed to delete jobs", err)
		_, err = db.ExecContext(ctx, "DELETE from tasks")
		handleErr("Failed to delete tasks", err)
		_, err = db.ExecContext(ctx, "DELETE from job_cursor")
		handleErr("Failed to delete job_cursor", err)
		_, err = db.ExecContext(ctx, "DELETE from job_event")
		handleErr("Failed to delete job_event", err)
		time.Sleep(10 * time.Second) // Give some time for the previous manager to finish
		managerCtx, managerCancelFunc = context.WithCancel(ctx)

		targetToClient := make(map[string]orbital.Initiator, testCfg.TargetsNum)
		for i := range testCfg.TargetsNum {
			target := targetName(i)
			client, err := operatorMock(testCfg.OperatorMock)
			handleErr("Failed to create client for target "+target, err)

			targetToClient[target] = client
		}

		termination := &termination{}

		manager, err = orbital.NewManager(
			repo,
			taskResolveFunc(testCfg.TargetsNum, testCfg.TaskResolveFunc),

			orbital.WithJobConfirmFunc(jobConfirmFunc(testCfg.JobConfirmFunc)),

			orbital.WithTargetClients(targetToClient),

			orbital.WithJobDoneEventFunc(jobTerminateFunc(&termination.dones)),
			orbital.WithJobFailedEventFunc(jobTerminateFunc(&termination.failures)),
			orbital.WithJobCanceledEventFunc(jobTerminateFunc(&termination.cancellations)),
		)

		handleErr("Failed to create orbital manager", err)

		log.Printf("%#v", testCfg.Manager)
		manager.Config = testCfg.Manager

		log.Printf("Preparing %d jobs with %d targets each...\n", testCfg.JobsNum, testCfg.TargetsNum)
		for i := range testCfg.JobsNum {
			job := orbital.NewJob("test-job", []byte{})

			_, err = manager.PrepareJob(ctx, job)
			if err != nil {
				log.Printf("Failed to prepare job %d: %s\n", i, err)
				w.Write([]byte("Failed to create jobs\n"))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		err = manager.Start(managerCtx)
		handleErr("Failed to start orbital manager", err)

		fileName := time.Now().Format("2006-01-02T15:04:05 ") + testCfg.TestName
		fileName = strings.ReplaceAll(fileName, " ", "_") + ".zip"

		cpuBuff := new(bytes.Buffer)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}

		if err := pprof.StartCPUProfile(cpuBuff); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}

		timeTaken := checkForTermination(ctx, termination, testCfg)

		testParams := struct {
			DateTime  time.Time
			TimeTaken time.Duration
			Config    TestConfig
		}{
			DateTime:  time.Now(),
			TimeTaken: timeTaken,
			Config:    testCfg,
		}

		testParamByte, err := json.MarshalIndent(testParams, "", "    ")
		handleErr("Failed to marshal test parameters", err)

		memBuf := new(bytes.Buffer)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		runtime.GC()
		if err := pprof.Lookup("allocs").WriteTo(memBuf, 0); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		pprof.StopCPUProfile()

		downLoadByte, err := zipMultipleFiles(map[string][]byte{
			"mem.prof":   memBuf.Bytes(),
			"cpu.prof":   cpuBuff.Bytes(),
			"testParams": testParamByte,
		})
		handleErr("Failed to zip files", err)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", "attachment; filename=\""+fileName+"\"")
		http.ServeContent(w, r, fileName, time.Now(), bytes.NewReader(downLoadByte))
	})
	err = http.ListenAndServe(":"+cfg.prometheusPort, mux)
	handleErr("Failed to start http server", err)
}

func jobConfirmFunc(cfg JobConfirmFuncConfig) orbital.JobConfirmFunc {
	return func(_ context.Context, _ orbital.Job) (orbital.JobConfirmResult, error) {
		errRand := rand.Float64()
		if errRand <= cfg.ErrorRate {
			return orbital.JobConfirmResult{}, errors.New("job confirm error")
		}

		timeRand := randIntN(cfg.LatencyAverageSec * 2)
		time.Sleep(time.Duration(timeRand) * time.Second)

		cancelRand := rand.Float64()
		if cancelRand <= cfg.CancelRate {
			return orbital.JobConfirmResult{
				Confirmed: false,
			}, nil
		}

		return orbital.JobConfirmResult{
			Confirmed: true,
		}, nil
	}
}

func taskResolveFunc(targetsNum int, cfg TaskResolveFuncConfig) orbital.TaskResolveFunc {
	return func(_ context.Context, _ orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
		errRand := rand.Float64()
		if errRand <= cfg.ErrorRate {
			return orbital.TaskResolverResult{}, errors.New("task resolve error")
		}

		timeRand := randIntN(cfg.LatencyAverageSec * 2)
		time.Sleep(time.Duration(timeRand) * time.Second)

		unfinishedRand := rand.Float64()
		if unfinishedRand <= cfg.UnfinishedRate {
			return orbital.TaskResolverResult{
				Done: false,
			}, nil
		}

		cancelRand := rand.Float64()
		if cancelRand <= cfg.CancelRate {
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

func operatorMock(cfg OperatorMockConfig) (*interactortest.Initiator, error) {
	return interactortest.NewInitiator(
		func(_ context.Context, request orbital.TaskRequest) (orbital.TaskResponse, error) {
			errRand := rand.Float64()
			if errRand <= cfg.ErrorRate {
				return orbital.TaskResponse{}, errors.New("client error")
			}

			timeRand := randIntN(cfg.LatencyAverageSec * 2)
			time.Sleep(time.Duration(timeRand) * time.Second)

			response := orbital.TaskResponse{
				TaskID: request.TaskID,
				ETag:   request.ETag,
				Type:   request.Type,
			}

			retryRand := rand.Float64()
			if retryRand <= cfg.UnfinishedRate {
				response.ReconcileAfterSec = int64(rand.IntN((cfg.LatencyAverageSec + 1) * 2))
				response.Status = string(orbital.TaskStatusProcessing)
				return response, nil
			}

			taskFailRand := rand.Float64()
			if taskFailRand <= cfg.TaskFailureRate {
				response.Status = string(orbital.TaskStatusFailed)
				return response, nil
			}

			response.Status = string(orbital.TaskStatusDone)
			return response, nil
		}, nil,
	)
}

func checkForTermination(ctx context.Context, termination *termination, config TestConfig) time.Duration {
	before := time.Now()
	log.Println("Checking for job termination...")
	for {
		select {
		case <-ctx.Done():
			log.Println("Context cancelled, exiting...")
			return 0 * time.Second
		default:
			dones := termination.dones.Load()
			failures := termination.failures.Load()
			cancellations := termination.cancellations.Load()

			sum := dones + failures + cancellations
			log.Printf("Jobs terminated: %d/%d; done: %d; failures: %d; cancellations: %d\n",
				sum, config.JobsNum, dones, failures, cancellations)
			if sum >= int64(config.JobsNum) {
				timeTaken := time.Since(before).Truncate(time.Millisecond)
				return timeTaken
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func zipMultipleFiles(files map[string][]byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	zw := zip.NewWriter(buf)
	for name, data := range files {
		f, err := zw.Create(name)
		if err != nil {
			zw.Close()
			return nil, err
		}
		_, err = f.Write(data)
		if err != nil {
			zw.Close()
			return nil, err
		}
	}
	err := zw.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
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
