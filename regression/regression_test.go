package regression_test

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	slogctx "github.com/veqryn/slog-context"

	"github.com/openkcm/orbital"
)

func TestNewRegression(t *testing.T) {
	isRegressionActive := os.Getenv("REGRESSION_ACTIVE")
	if isRegressionActive != "true" {
		t.Skip("Regression test is skipped")
	}
	noOfJobs := envIntOrDefault("REGRESSION_NO_JOBS", 100)
	noOfManager := envIntOrDefault("REGRESSION_NO_MANAGERS", 10)
	noOfOperator := envIntOrDefault("REGRESSION_NO_OPERATORS", 10)
	testTimeout := time.Duration(envIntOrDefault("REGRESSION_TIMEOUT_MINUTES", 3)) * time.Minute

	t.Run("regression", func(t *testing.T) {
		ctx := t.Context()
		slogctx.Info(ctx, "Starting testing",
			"NUMBER OF JOBS", noOfJobs,
			"NO OF MANAGERS", noOfManager,
			"NO OF OPERATORS", noOfOperator,
			"TEST TIMEOUT", testTimeout.Minutes())
		env, err := setupEnv(ctx, t, "db"+strings.ReplaceAll(uuid.NewString(), "-", ""))
		require.NoError(t, err, "failed to set up test environment")
		defer func() {
			err := env.Cleanup(ctx)
			assert.NoError(t, err, "failed to clean up test environment")
		}()

		ctxCancel, cancel := context.WithCancel(ctx)

		optTrackers := make([]*OperatorTracker, 0, noOfOperator)
		for _, operatorName := range deriveOperatorNames(noOfOperator) {
			op, err := NewOperatorTracker(ctxCancel, env, operatorName)
			require.NoError(t, err)
			optTrackers = append(optTrackers, op)
			go op.operator.ListenAndRespond(ctxCancel)
		}

		defer func() {
			for _, optTracker := range optTrackers {
				optTracker.Cleanup(ctx)
			}
		}()

		terminalEventChan := make(chan struct{}, noOfManager)

		mgrTrackers := make([]*ManagerTracker, 0, noOfManager)
		for _, managerName := range deriveManagerNames(noOfManager) {
			mgr, err := NewManagerTracker(ctxCancel, env, managerName, noOfOperator, terminalEventChan)
			require.NoError(t, err)
			mgrTrackers = append(mgrTrackers, mgr)
		}

		defer func() {
			for _, mgrTracker := range mgrTrackers {
				mgrTracker.Cleanup(ctx)
			}
		}()

		expJobIDs := map[string]struct{}{}
		for i := range noOfJobs {
			// distributing job creation between managers
			mgr := mgrTrackers[i%len(mgrTrackers)]
			job, err := mgr.manager.PrepareJob(ctxCancel, orbital.NewJob("jobtype", []byte("")))
			require.NoError(t, err)
			slogctx.Info(ctx, "created job with", "managerName", mgr.name, "JobID", job.ID)
			expJobIDs[job.ID.String()] = struct{}{}
		}

		for _, mgr := range mgrTrackers {
			err = mgr.manager.Start(ctxCancel)
			require.NoError(t, err)
		}

		ctxTimeOut, cancelTimeout := context.WithTimeout(ctx, testTimeout)
		defer cancelTimeout()
		var terminalEventCalls atomic.Int32
		for {
			select {
			case <-ctxTimeOut.Done():
				assert.Fail(t, "test have timeout could not complete within the time peroid")
				cancel()
				return
			case <-terminalEventChan:
				terminalEventCalls.Add(1)
				if int(terminalEventCalls.Load()) == noOfJobs {
					assertManagerMetrics(t, mgrTrackers, noOfJobs)
					assertOperatorMetrics(t, optTrackers, noOfJobs, expJobIDs)
					cancel()
					return
				}
			}
		}
	})
}

func assertOperatorMetrics(t *testing.T, optTrackers []*OperatorTracker, noOfJobs int, expJobIDs map[string]struct{}) {
	t.Helper()
	// checking if the operator tasks have been routed correctly
	for _, optTracker := range optTrackers {
		// checking if the request came with correct data.
		// Here since we are appending the jobID and the operator name we are making sure
		// that we are sending the correct data to the operator.
		assert.Len(t, optTracker.noOfTaskProcessed.value, noOfJobs)
		jobIDs := map[string]struct{}{}
		for _, taskProcessed := range optTracker.noOfTaskProcessed.value {
			assert.GreaterOrEqual(t, len(taskProcessed), 1)
			jobID, operatorName, err := extractJobIDAndOperator(string(taskProcessed[0].Data))
			assert.NoError(t, err)
			jobIDs[jobID] = struct{}{}
			assert.Equal(t, optTracker.name, operatorName)
		}
		// check if all jobs are processed by the operator
		assert.Len(t, jobIDs, len(expJobIDs))
		for jobID := range jobIDs {
			_, ok := expJobIDs[jobID]
			assert.True(t, ok)
		}
	}
}

func assertManagerMetrics(t *testing.T, mgrTrackers []*ManagerTracker, noOfJobs int) {
	t.Helper()
	// checking if all the jobs in the manager have been processed successfully
	var actNoOfJobConfirmed, actNoOfJobResolved, actNoOfJobDone, actNoOfJobCanceled, actNoOfJobFailed int
	for _, mgrTracker := range mgrTrackers {
		noOfConfirmedJob := len(mgrTracker.noOfJobConfirmed.value)
		actNoOfJobConfirmed += noOfConfirmedJob

		noOfResolvedJob := len(mgrTracker.noOfJobResolved.value)
		actNoOfJobResolved += noOfResolvedJob

		noOfJobDone := len(mgrTracker.noOfJobDone.value)
		actNoOfJobDone += noOfJobDone

		// making sure that manager is distributing the work,
		// this condition will fail if the number of jobs are less.
		assert.Greater(t, noOfConfirmedJob, 1)
		assert.Greater(t, noOfResolvedJob, 1)
		assert.Greater(t, noOfJobDone, 1)

		actNoOfJobCanceled += len(mgrTracker.noOfJobCanceled.value)
		actNoOfJobFailed += len(mgrTracker.noOfJobFailed.value)
	}
	assert.Equal(t, noOfJobs, actNoOfJobConfirmed)
	assert.Equal(t, noOfJobs, actNoOfJobResolved)
	assert.Equal(t, noOfJobs, actNoOfJobDone)
	assert.Equal(t, 0, actNoOfJobCanceled)
	assert.Equal(t, 0, actNoOfJobFailed)
}
