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
					err := w.Stop()
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
		err := w.Run(t.Context())
		assert.NoError(t, err)
		defer func() {
			err := w.Stop()
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
		err := w.Run(t.Context())
		assert.NoError(t, err)

		// stopping runner
		err = w.Stop()
		assert.NoError(t, err)

		// when
		// starting runner again
		err = w.Run(t.Context())
		assert.NoError(t, err)
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
					Name: "test-3",
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

		<-ctxTimeOut.Done()

		assert.GreaterOrEqual(t, calledTimes.Load(), int32(2))
	})
}
