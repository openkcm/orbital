package worker_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital/internal/worker"
)

func TestStart(t *testing.T) {
	t.Run("should call the required number of workers func within a defined time interval", func(t *testing.T) {
		/**
		* ctxTimeout      ------------------->3 Second
		* workerInterval  ---------->         2 Second
		* workerTimeout   ---->               10 Millisecond
		*
		* here we should have only one execution of the ticker.
		* since ctxTimeout is 3 second and workerInterval is 2 second,
		* the ticker will be executed only once.
		*
		**/
		tts := []struct {
			noOfWorker     int
			expWorkFnCalls int32
		}{
			{
				noOfWorker:     10,
				expWorkFnCalls: 1,
			},
			{
				noOfWorker:     1,
				expWorkFnCalls: 1,
			},
			{
				noOfWorker:     0,
				expWorkFnCalls: 0,
			},
		}
		for _, tt := range tts {
			t.Run(fmt.Sprintf("[%+v]", tt), func(t *testing.T) {
				ctxTimeOut, cancel := context.WithTimeout(t.Context(), 3*time.Second)
				defer cancel()

				var calledTimes atomic.Int32

				w := worker.Runner{
					Works: []worker.Work{
						{
							Name: "test-1",
							Fn: func(_ context.Context) error {
								calledTimes.Add(1)
								return nil
							},
							NoOfWorkers:  tt.noOfWorker,
							ExecInterval: 2 * time.Second,
							Timeout:      10 * time.Millisecond,
						},
					},
				}
				// when
				err := w.Run(ctxTimeOut)
				assert.NoError(t, err)
				defer func() {
					stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer stopCancel()
					err := w.Stop(stopCtx)
					assert.NoError(t, err)
				}()

				<-ctxTimeOut.Done()

				// then
				assert.Equal(t, tt.expWorkFnCalls, calledTimes.Load())
			})
		}
	})

	t.Run("should return error if Run is called 2 times subsequently", func(t *testing.T) {
		w := worker.Runner{
			Works: []worker.Work{
				{
					Name: "test-2",
					Fn: func(_ context.Context) error {
						return nil
					},
					NoOfWorkers:  1,
					ExecInterval: 3 * time.Second,
					Timeout:      400 * time.Millisecond,
				},
			},
		}

		// starting runner
		ctx := t.Context()
		err := w.Run(ctx)
		assert.NoError(t, err)
		defer func() {
			err := w.Stop(ctx)
			assert.NoError(t, err)
		}()

		// when
		// starting runner again
		err = w.Run(t.Context())
		assert.Error(t, err)
	})

	t.Run("should not return error while calling Run if the runner is stopped first", func(t *testing.T) {
		w := worker.Runner{
			Works: []worker.Work{
				{
					Name: "test-3",
					Fn: func(_ context.Context) error {
						return nil
					},
					NoOfWorkers:  1,
					ExecInterval: 3 * time.Second,
					Timeout:      400 * time.Millisecond,
				},
			},
		}

		// starting runner
		ctx := t.Context()
		err := w.Run(ctx)
		assert.NoError(t, err)

		// stopping runner
		err = w.Stop(ctx)
		assert.NoError(t, err)

		// when
		// starting runner again
		err = w.Run(t.Context())
		assert.NoError(t, err)
		defer func() {
			err := w.Stop(t.Context())
			assert.NoError(t, err)
		}()
	})

	t.Run("should continue working if the worker timeout", func(t *testing.T) {
		/**
		* ctxTimeout      ------------------->4 Second
		* workerInterval  ------>             1 Second
		* workerTimeout   -->                 1 Millisecond
		* workerFuncSleep ----------->        2 Second
		*
		**/
		ctxTimeOut, cancel := context.WithTimeout(t.Context(), 4*time.Second)
		defer cancel()

		var calledTimes atomic.Int32
		w := worker.Runner{
			Works: []worker.Work{
				{
					Name: "test-4",
					Fn: func(_ context.Context) error {
						calledTimes.Add(1)
						time.Sleep(2 * time.Second)
						return nil
					},
					NoOfWorkers:  1,
					ExecInterval: 1 * time.Second,
					Timeout:      1 * time.Millisecond,
				},
			},
		}

		// starting runner
		err := w.Run(ctxTimeOut)
		assert.NoError(t, err)
		defer func() {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer stopCancel()
			_ = w.Stop(stopCtx)
		}()

		<-ctxTimeOut.Done()

		assert.GreaterOrEqual(t, calledTimes.Load(), int32(2))
	})
}

func TestStop(t *testing.T) {
	t.Run("should return error if Stop is called without Run", func(t *testing.T) {
		w := worker.Runner{
			Works: []worker.Work{
				{
					Name: "test-stop-1",
					Fn: func(_ context.Context) error {
						return nil
					},
					NoOfWorkers:  1,
					ExecInterval: 1 * time.Second,
					Timeout:      100 * time.Millisecond,
				},
			},
		}

		// when: calling Stop without Run
		err := w.Stop(t.Context())

		// then: should return error
		assert.Error(t, err)
	})

	t.Run("should wait for worker goroutines to exit before returning", func(t *testing.T) {
		workerStarted := make(chan struct{})
		allowWorkerToFinish := make(chan struct{})

		w := worker.Runner{
			Works: []worker.Work{
				{
					Name: "test-stop-2",
					Fn: func(ctx context.Context) error {
						close(workerStarted)
						<-ctx.Done()
						<-allowWorkerToFinish
						return nil
					},
					NoOfWorkers:  1,
					ExecInterval: 50 * time.Millisecond,
					Timeout:      5 * time.Second,
				},
			},
		}

		err := w.Run(t.Context())
		assert.NoError(t, err)

		<-workerStarted

		stopDone := make(chan error, 1)
		go func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			stopDone <- w.Stop(stopCtx)
		}()

		select {
		case <-stopDone:
			t.Fatal("Stop() returned before worker finished - it should have waited")
		default:
		}

		close(allowWorkerToFinish)

		select {
		case err := <-stopDone:
			assert.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Stop() did not return after worker finished")
		}
	})

	t.Run("should respect context timeout during Stop", func(t *testing.T) {
		workerStarted := make(chan struct{})

		w := worker.Runner{
			Works: []worker.Work{
				{
					Name: "test-stop-3",
					Fn: func(_ context.Context) error {
						close(workerStarted)
						select {}
					},
					NoOfWorkers:  1,
					ExecInterval: 10 * time.Millisecond,
					Timeout:      10 * time.Second,
				},
			},
		}

		runCtx, runCancel := context.WithCancel(t.Context())
		defer runCancel()

		err := w.Run(runCtx)
		assert.NoError(t, err)

		select {
		case <-workerStarted:
		case <-time.After(1 * time.Second):
			t.Fatal("worker did not start in time")
		}

		stopCtx, stopCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer stopCancel()

		err = w.Stop(stopCtx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("should handle zero workers without deadlock", func(t *testing.T) {
		w := worker.Runner{
			Works: []worker.Work{
				{
					Name:         "test-stop-4",
					Fn:           func(_ context.Context) error { return nil },
					NoOfWorkers:  0, // Zero workers
					ExecInterval: 1 * time.Second,
					Timeout:      100 * time.Millisecond,
				},
			},
		}

		ctx := t.Context()
		err := w.Run(ctx)
		assert.NoError(t, err)

		stopCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		err = w.Stop(stopCtx)
		assert.NoError(t, err, "Stop should complete without deadlock for zero workers")
	})
}
