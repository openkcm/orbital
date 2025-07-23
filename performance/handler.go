package performance

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openkcm/orbital"
)

type Handler struct {
	db         *sql.DB
	repo       *orbital.Repository
	cancelTest context.CancelFunc
	mutex      sync.Mutex
}

type TestRunMetrics struct {
	DateTime         time.Time
	TimeTaken        time.Duration
	JobDones         int64
	JobFailures      int64
	JobCancellations int64

	TestConfig TestConfig
}

type testRunResult struct {
	metrics    TestRunMetrics
	memProfile *bytes.Buffer
	cpuProfile *bytes.Buffer
}

type termination struct {
	expectedJobs int

	jobFailures      atomic.Int64
	jobDones         atomic.Int64
	jobCancellations atomic.Int64
}

func NewHandler(db *sql.DB, repo *orbital.Repository) *Handler {
	return &Handler{
		db:   db,
		repo: repo,
	}
}

func (h *Handler) StartTest(w http.ResponseWriter, r *http.Request) {
	h.mutex.Lock()
	if h.cancelTest != nil {
		h.mutex.Unlock()
		http.Error(w, "Test already running", http.StatusConflict)
		return
	}
	ctxCancel, cancel := context.WithCancel(r.Context())
	h.cancelTest = cancel
	h.mutex.Unlock()

	defer func() {
		h.mutex.Lock()
		h.cancelTest = nil
		h.mutex.Unlock()
		_ = h.cleanupDB()
		cancel()
	}()

	testCfg, err := parseTestConfig(r)
	if err != nil {
		http.Error(w, "Failed to parse test configuration: "+err.Error(), http.StatusBadRequest)
		return
	}

	termination := &termination{
		expectedJobs: testCfg.JobsNum,
	}
	manager, err := initManager(h.repo, testCfg, termination)
	if err != nil {
		http.Error(w, "Failed to create orbital manager: "+err.Error(), http.StatusInternalServerError)
		return
	}

	err = prepareJobs(ctxCancel, manager, testCfg.JobsNum, testCfg.TargetsNum)
	if err != nil {
		http.Error(w, "Failed to prepare jobs: "+err.Error(), http.StatusInternalServerError)
		return
	}

	result, err := runTest(ctxCancel, manager, termination)
	if err != nil {
		http.Error(w, "Test failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	result.metrics.TestConfig = testCfg

	metricsBytes, err := json.MarshalIndent(result.metrics, "", "  ")
	if err != nil {
		http.Error(w, "Failed to marshal metrics: "+err.Error(), http.StatusInternalServerError)
		return
	}

	downloadBytes, err := zipMultipleFiles(map[string][]byte{
		"mem.prof": result.memProfile.Bytes(),
		"cpu.prof": result.cpuProfile.Bytes(),
		"metrics":  metricsBytes,
	})
	if err != nil {
		http.Error(w, "Failed to create zip file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	fileName := fileName(testCfg.TestName)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", "attachment; filename=\""+fileName+"\"")
	http.ServeContent(w, r, fileName, time.Now(), bytes.NewReader(downloadBytes))
}

func (h *Handler) StopTest(w http.ResponseWriter, r *http.Request) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if h.cancelTest == nil {
		http.Error(w, "No test is running", http.StatusConflict)
		return
	}
	h.cancelTest()
	h.cancelTest = nil

	err := h.cleanupDB()
	if err != nil {
		http.Error(w, "Failed to clean up database: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Test stopped successfully"))
}

func parseTestConfig(r *http.Request) (TestConfig, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return TestConfig{}, err
	}
	testCfg, err := newTestConfig(body)
	if err != nil {
		return TestConfig{}, err
	}
	return testCfg, nil
}

func prepareJobs(ctx context.Context, manager *orbital.Manager, jobsNum, targetsNum int) error {
	log.Printf("Preparing %d jobs with %d targets each...\n", jobsNum, targetsNum)
	for range jobsNum {
		job := orbital.NewJob("test-job", []byte{})
		_, err := manager.PrepareJob(ctx, job)
		if err != nil {
			return err
		}
	}
	return nil
}

func runTest(ctx context.Context, manager *orbital.Manager, termination *termination) (testRunResult, error) {
	cpuBuf := new(bytes.Buffer)
	if err := pprof.StartCPUProfile(cpuBuf); err != nil {
		return testRunResult{}, fmt.Errorf("could not start CPU profile: %w", err)
	}
	defer pprof.StopCPUProfile()

	if err := manager.Start(ctx); err != nil {
		return testRunResult{}, fmt.Errorf("failed to start manager: %w", err)
	}

	timeTaken := checkForJobTermination(ctx, termination)

	memBuf := new(bytes.Buffer)
	runtime.GC()
	if err := pprof.Lookup("allocs").WriteTo(memBuf, 0); err != nil {
		return testRunResult{}, fmt.Errorf("could not create memory profile: %w", err)
	}

	result := TestRunMetrics{
		DateTime:         time.Now(),
		TimeTaken:        timeTaken,
		JobDones:         termination.jobDones.Load(),
		JobFailures:      termination.jobFailures.Load(),
		JobCancellations: termination.jobCancellations.Load(),
	}

	return testRunResult{
		metrics:    result,
		memProfile: memBuf,
		cpuProfile: cpuBuf,
	}, nil
}

func checkForJobTermination(ctx context.Context, t *termination) time.Duration {
	before := time.Now()
	log.Println("Checking for job termination...")
	for {
		select {
		case <-ctx.Done():
			log.Println("Context cancelled, exiting...")
			return time.Since(before).Truncate(time.Millisecond)
		default:
			dones := t.jobDones.Load()
			failures := t.jobFailures.Load()
			cancellations := t.jobCancellations.Load()

			sum := dones + failures + cancellations
			log.Printf("Jobs terminated: %d/%d; done: %d; failures: %d; cancellations: %d\n",
				sum, t.expectedJobs, dones, failures, cancellations)
			if sum >= int64(t.expectedJobs) {
				return time.Since(before).Truncate(time.Millisecond)
			}
			time.Sleep(1 * time.Second) // this interfers with the test duration, should be fixed
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

func (h *Handler) cleanupDB() error {
	ctx := context.Background()
	tables := []string{"jobs", "tasks", "job_cursor", "job_event"}
	for _, table := range tables {
		if _, err := h.db.ExecContext(ctx, "DELETE FROM "+table); err != nil {
			log.Printf("Failed to clean up table %s: %v", table, err)
			return err
		}
	}
	return nil
}

func fileName(testName string) string {
	fileName := time.Now().Format(time.RFC3339) + "_" + testName
	return strings.ReplaceAll(fileName, " ", "_") + ".zip"
}
