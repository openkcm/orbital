package worker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/openkcm/common-sdk/pkg/logger"

	slogctx "github.com/veqryn/slog-context"
)

type (
	// Func defines the signature for a unit of work to be executed.
	Func func(ctx context.Context) error
	// Runner manages and executes a set of Work items.
	Runner struct {
		Works      []Work             // Works holds the list of work items to be executed.
		cancelFunc context.CancelFunc // cancelFunc is used to cancel the execution of all work items.
		wg         sync.WaitGroup     // wg is used to track all the goroutines spawned by the Runner.
	}
)

// Work defines a unit of work to be executed by the Runner.
type Work struct {
	Name         string        // Name identifies the work item.
	Fn           Func          // Fn is the function to execute.
	NoOfWorkers  int           // NoOfWorkers specifies the number of concurrent workers for this work item.
	ExecInterval time.Duration // ExecInterval is the interval between executions of the work function.
	Timeout      time.Duration // Timeout is the maximum duration allowed for the work function to complete.
}

var (
	errRunnerAlreadyRunning     = errors.New("runner is already running")
	errWorkerChanInitialization = errors.New("runner channel initialization failed")
	errRunnerNotRunning         = errors.New("runner is not running")
)

// Run starts all configured Work items in the Runner. It returns an error if the Runner is already running.
// Each Work item is set up and scheduled in its own goroutine.
func (r *Runner) Run(ctx context.Context) error {
	if r.cancelFunc != nil {
		return errRunnerAlreadyRunning
	}
	ctxCancel, cancel := context.WithCancel(ctx)
	r.cancelFunc = cancel

	// set up channels and workers for each work item based on the number of workers specified.
	workChans := make(map[string]chan struct{})
	for _, work := range r.Works {
		workChan := make(chan struct{}, work.NoOfWorkers)
		workChans[work.Name] = workChan
		setupWorkers(ctxCancel, &r.wg, workChan, work)
	}
	// start the workers for each work item.
	for _, work := range r.Works {
		workChan, ok := workChans[work.Name]
		if !ok {
			return errWorkerChanInitialization
		}
		r.wg.Go(func() {
			startWorkers(ctxCancel, workChan, work)
		})
	}
	return nil
}

// Stop halts all running Work items managed by the Runner.
// It cancels the context for all workers and closes their work channels.
// Returns an error if the Runner is not running.
func (r *Runner) Stop(ctx context.Context) error {
	if r.cancelFunc == nil {
		return errRunnerNotRunning
	}

	r.cancelFunc()
	r.cancelFunc = nil

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		slogctx.Info(ctx, "runner stopped gracefully")
		return nil
	case <-ctx.Done():
		slogctx.Error(ctx, "runner shutdown timed out")
		return ctx.Err()
	}
}

// setupWorkers starts the specified number of worker goroutines for the given Work item.
// Each worker listens for signals on workChan to execute the associated function with a timeout.
// Errors and timeouts are logged. The worker exits if the parent context is cancelled.
func setupWorkers(ctxCancel context.Context, wg *sync.WaitGroup, workChan <-chan struct{}, work Work) {
	for range work.NoOfWorkers {
		wg.Go(func() {
			for {
				select {
				case _, ok := <-workChan:
					if !ok {
						slogctx.Error(ctxCancel, "worker channel closed", "name", work.Name)
						return
					}

					ctxTimeout, cancel := context.WithTimeout(ctxCancel, work.Timeout)
					errChan := make(chan error, 1)

					wg.Go(func() {
						defer close(errChan)
						slogctx.Log(ctxCancel, logger.LevelTrace, "worker started", "name", work.Name)
						errChan <- work.Fn(ctxTimeout)
					})
					select {
					case err := <-errChan:
						if err != nil {
							slogctx.Error(ctxCancel, "worker error", "name", work.Name, "error", err)
						}
					case <-ctxTimeout.Done():
						slogctx.Error(ctxCancel, "worker timeout", "name", work.Name)
					}

					cancel()
				case <-ctxCancel.Done():
					slogctx.Info(ctxCancel, "worker canceled", "name", work.Name)
					return
				}
			}
		})
	}
}

// startWorkers periodically signals workers to execute the work function.
// It sends a signal for each worker at the specified execution interval.
// The function exits when the context is cancelled.
func startWorkers(ctxCancel context.Context, workChan chan<- struct{}, work Work) {
	ticker := time.NewTicker(work.ExecInterval)
	defer ticker.Stop()

	defer close(workChan)

	running := true
	for running {
		select {
		case <-ticker.C:
			select {
			case workChan <- struct{}{}:
			case <-ctxCancel.Done():
				running = false
			}
		case <-ctxCancel.Done():
			running = false
		}
	}
}
