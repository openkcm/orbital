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
	TotalTime        time.Duration
	JobDones         int64
	JobFailures      int64
	JobCancellations int64

	Parameters Parameters
}

type testRunResult struct {
	metrics    TestRunMetrics
	memProfile *bytes.Buffer
	cpuProfile *bytes.Buffer
}

type termination struct {
	expectedJobs int
	finished     chan struct{}

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
	ctx, cancel := context.WithCancel(r.Context())

	err := h.acquireTestLock(cancel)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	defer func() {
		h.releaseTestLock()
		_ = h.cleanupDB()
		cancel()
	}()

	cfg, err := parseParameters(r)
	if err != nil {
		http.Error(w, "Failed to parse test configuration: "+err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.executeTest(ctx, cfg)
	if err != nil {
		http.Error(w, "Test failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	h.sendTestResults(w, r, result, cfg)
}

func (h *Handler) StopTest(w http.ResponseWriter, _ *http.Request) {
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
	_, err = w.Write([]byte("Test stopped successfully"))
	if err != nil {
		log.Printf("Failed to write response: %v", err)
		return
	}
}

func (h *Handler) acquireTestLock(cancel context.CancelFunc) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	if h.cancelTest != nil {
		return fmt.Errorf("test already running")
	}
	h.cancelTest = cancel
	return nil
}

func (h *Handler) releaseTestLock() {
	h.mutex.Lock()
	h.cancelTest = nil
	h.mutex.Unlock()
}

func (h *Handler) executeTest(ctx context.Context, cfg Parameters) (testRunResult, error) {
	termination := &termination{
		expectedJobs: cfg.JobsNum,
		finished:     make(chan struct{}),
	}

	manager, err := initManager(h.repo, cfg, termination)
	if err != nil {
		return testRunResult{}, fmt.Errorf("failed to create orbital manager: %w", err)
	}

	err = prepareJobs(ctx, manager, cfg.JobsNum)
	if err != nil {
		return testRunResult{}, fmt.Errorf("failed to prepare jobs: %w", err)
	}

	result, err := profileTest(ctx, manager, termination)
	if err != nil {
		return testRunResult{}, fmt.Errorf("test execution failed: %w", err)
	}

	result.metrics.Parameters = cfg
	return result, nil
}

func (h *Handler) sendTestResults(w http.ResponseWriter, r *http.Request, result testRunResult, cfg Parameters) {
	metricsBytes, err := json.MarshalIndent(result.metrics, "", "  ")
	if err != nil {
		http.Error(w, "Failed to marshal metrics: "+err.Error(), http.StatusInternalServerError)
		return
	}

	downloadBytes, err := zipMultipleFiles(map[string][]byte{
		"mem.prof":              result.memProfile.Bytes(),
		"cpu.prof":              result.cpuProfile.Bytes(),
		"business_metrics.json": metricsBytes,
	})
	if err != nil {
		http.Error(w, "Failed to create zip file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	fileName := fileName(cfg.TestName)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", "attachment; filename=\""+fileName+"\"")
	http.ServeContent(w, r, fileName, time.Now(), bytes.NewReader(downloadBytes))
}

func parseParameters(r *http.Request) (Parameters, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return Parameters{}, err
	}
	testCfg, err := newParameters(body)
	if err != nil {
		return Parameters{}, err
	}
	return testCfg, nil
}

func prepareJobs(ctx context.Context, manager *orbital.Manager, jobsNum int) error {
	workers := min(jobsNum, 10)
	jobs := make(chan int, jobsNum)
	errs := make(chan error, workers)

	for i := range jobsNum {
		jobs <- i
	}
	close(jobs)

	for range workers {
		go func() {
			for range jobs {
				jobCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				job := orbital.NewJob("test-job", []byte{})
				_, err := manager.PrepareJob(jobCtx, job)
				cancel()
				if err != nil {
					select {
					case errs <- err:
					case <-ctx.Done():
						return
					}
					return
				}
			}
			errs <- nil
		}()
	}

	for range workers {
		if err := <-errs; err != nil {
			return err
		}
	}
	return nil
}

func profileTest(ctx context.Context, manager *orbital.Manager, termination *termination) (testRunResult, error) {
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
		DateTime:         time.Now().UTC(),
		TotalTime:        timeTaken,
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
	start := time.Now()

	select {
	case <-ctx.Done():
		return time.Since(start)
	case <-t.finished:
		return time.Since(start)
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tables := []string{"jobs", "tasks", "job_cursor", "job_event"}
	for _, table := range tables {
		_, err := h.db.ExecContext(ctx, "DELETE FROM "+table)
		if err != nil {
			log.Printf("Failed to clean up table %s: %v", table, err)
			return err
		}
	}
	return nil
}

func fileName(testName string) string {
	fileName := time.Now().UTC().Format(time.RFC3339) + "_" + testName
	return strings.ReplaceAll(fileName, " ", "_") + ".zip"
}
