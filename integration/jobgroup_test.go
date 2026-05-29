package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/client/embedded"
	"github.com/openkcm/orbital/store/sql"
)

func TestJobGroupFlows(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	pg := setupPostgres(ctx, t)

	tests := []struct {
		name string
		test func(ctx context.Context, t *testing.T, store *sql.SQL)
	}{
		{"complete sequentially", testJobGroupCompleteSequentially},
		{"fail and stop promotion", testJobGroupFailAndStopPromotion},
		{"cancel and stop promotion", testJobGroupCancelAndStopPromotion},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := createStore(ctx, t, pg)
			tt.test(ctx, t, store)
		})
	}
}

func testJobGroupCompleteSequentially(ctx context.Context, t *testing.T, store *sql.SQL) {
	t.Helper()

	var counter atomic.Int32
	var order [3]int32

	handlers := []orbital.HandlerFunc{
		func(_ context.Context, _ orbital.HandlerRequest, resp *orbital.HandlerResponse) {
			order[0] = counter.Add(1)
			resp.Complete()
		},
		func(_ context.Context, _ orbital.HandlerRequest, resp *orbital.HandlerResponse) {
			order[1] = counter.Add(1)
			resp.Complete()
		},
		func(_ context.Context, _ orbital.HandlerRequest, resp *orbital.HandlerResponse) {
			order[2] = counter.Add(1)
			resp.Complete()
		},
	}

	targetManagers := make(map[string]orbital.TargetManager, len(handlers))
	jobs := make([]orbital.Job, len(handlers))
	for i, h := range handlers {
		target := fmt.Sprintf("target-%d", i)

		client, err := embedded.NewClient(h)
		require.NoError(t, err)
		t.Cleanup(func() { client.Close(ctx) })

		targetManagers[target] = orbital.TargetManager{Client: client}
		jobs[i] = orbital.NewJob("test-type", requireEncode(t, jobData{Target: target}))
	}

	var jobGroupDoneCalls atomic.Int32
	jobGroupTerminated := make(chan orbital.JobGroup, 1)

	repo := orbital.NewRepository(store)
	manager, err := orbital.NewManager(repo,
		resolveTargetFromJobData(),
		orbital.WithTargets(targetManagers),
		orbital.WithJobGroupDoneEventFunc(func(_ context.Context, group orbital.JobGroup) error {
			jobGroupDoneCalls.Add(1)
			jobGroupTerminated <- group
			return nil
		}),
	)
	require.NoError(t, err)
	withFastIntervals(manager)

	require.NoError(t, manager.Start(ctx))
	t.Cleanup(func() { manager.Stop(ctx) })

	group, err := manager.PrepareJobGroup(ctx, orbital.JobGroup{
		Type: "sequential-job-group",
		Jobs: jobs,
	})
	require.NoError(t, err)

	select {
	case g := <-jobGroupTerminated:
		assert.Equal(t, group.ID, g.ID)
		assert.Equal(t, orbital.JobGroupStatusDone, g.Status)
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for job group completion")
	}

	finalGroup, found, err := manager.GetJobGroup(ctx, group.ID)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, orbital.JobGroupStatusDone, finalGroup.Status)

	for _, job := range finalGroup.Jobs {
		assert.Equal(t, orbital.JobStatusDone, job.Status)
	}

	assert.Equal(t, int32(1), order[0])
	assert.Equal(t, int32(2), order[1])
	assert.Equal(t, int32(3), order[2])
	assert.Equal(t, int32(1), jobGroupDoneCalls.Load())
}

func testJobGroupFailAndStopPromotion(ctx context.Context, t *testing.T, store *sql.SQL) {
	t.Helper()

	handlers := []orbital.HandlerFunc{
		func(_ context.Context, _ orbital.HandlerRequest, resp *orbital.HandlerResponse) {
			resp.Complete()
		},
		func(_ context.Context, _ orbital.HandlerRequest, resp *orbital.HandlerResponse) {
			resp.Fail("broken")
		},
		func(_ context.Context, _ orbital.HandlerRequest, resp *orbital.HandlerResponse) {
			resp.Complete()
		},
	}

	targetManagers := make(map[string]orbital.TargetManager, len(handlers))
	jobs := make([]orbital.Job, len(handlers))
	for i, h := range handlers {
		target := fmt.Sprintf("target-%d", i)

		client, err := embedded.NewClient(h)
		require.NoError(t, err)
		t.Cleanup(func() { client.Close(ctx) })

		targetManagers[target] = orbital.TargetManager{Client: client}
		jobs[i] = orbital.NewJob("test-type", requireEncode(t, jobData{Target: target}))
	}

	var jobGroupFailedCalls atomic.Int32
	jobGroupTerminated := make(chan orbital.JobGroup, 1)

	repo := orbital.NewRepository(store)
	manager, err := orbital.NewManager(repo,
		resolveTargetFromJobData(),
		orbital.WithTargets(targetManagers),
		orbital.WithJobGroupFailedEventFunc(func(_ context.Context, group orbital.JobGroup) error {
			jobGroupFailedCalls.Add(1)
			jobGroupTerminated <- group
			return nil
		}),
	)
	require.NoError(t, err)
	withFastIntervals(manager)

	require.NoError(t, manager.Start(ctx))
	t.Cleanup(func() { manager.Stop(ctx) })

	group, err := manager.PrepareJobGroup(ctx, orbital.JobGroup{
		Type: "fail-job-group",
		Jobs: jobs,
	})
	require.NoError(t, err)

	select {
	case g := <-jobGroupTerminated:
		assert.Equal(t, group.ID, g.ID)
		assert.Equal(t, orbital.JobGroupStatusFailed, g.Status)
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for job group termination")
	}

	finalGroup, found, err := manager.GetJobGroup(ctx, group.ID)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, orbital.JobGroupStatusFailed, finalGroup.Status)
	assert.Equal(t, orbital.JobStatusDone, finalGroup.Jobs[0].Status)
	assert.Equal(t, orbital.JobStatusFailed, finalGroup.Jobs[1].Status)
	assert.Equal(t, orbital.JobStatusScheduled, finalGroup.Jobs[2].Status)
	assert.Equal(t, int32(1), jobGroupFailedCalls.Load())
}

func testJobGroupCancelAndStopPromotion(ctx context.Context, t *testing.T, store *sql.SQL) {
	t.Helper()

	blocked := make(chan struct{})
	t.Cleanup(func() { close(blocked) })
	reached := make(chan struct{})

	handlers := []orbital.HandlerFunc{
		func(_ context.Context, _ orbital.HandlerRequest, resp *orbital.HandlerResponse) {
			close(reached)
			<-blocked
			resp.Complete()
		},
		func(_ context.Context, _ orbital.HandlerRequest, resp *orbital.HandlerResponse) {
			resp.Complete()
		},
	}

	targetManagers := make(map[string]orbital.TargetManager, len(handlers))
	jobs := make([]orbital.Job, len(handlers))
	for i, h := range handlers {
		target := fmt.Sprintf("target-%d", i)

		client, err := embedded.NewClient(h)
		require.NoError(t, err)
		t.Cleanup(func() { client.Close(ctx) })

		targetManagers[target] = orbital.TargetManager{Client: client}
		jobs[i] = orbital.NewJob("test-type", requireEncode(t, jobData{Target: target}))
	}

	var jobGroupCanceledCalls atomic.Int32
	jobGroupTerminated := make(chan orbital.JobGroup, 1)

	repo := orbital.NewRepository(store)
	manager, err := orbital.NewManager(repo,
		resolveTargetFromJobData(),
		orbital.WithTargets(targetManagers),
		orbital.WithJobGroupCanceledEventFunc(func(_ context.Context, group orbital.JobGroup) error {
			jobGroupCanceledCalls.Add(1)
			jobGroupTerminated <- group
			return nil
		}),
	)
	require.NoError(t, err)
	withFastIntervals(manager)

	require.NoError(t, manager.Start(ctx))
	t.Cleanup(func() { manager.Stop(ctx) })

	group, err := manager.PrepareJobGroup(ctx, orbital.JobGroup{
		Type: "cancel-job-group",
		Jobs: jobs,
	})
	require.NoError(t, err)

	select {
	case <-reached:
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for handler to be reached")
	}

	require.NoError(t, manager.CancelJobGroup(ctx, group.ID))

	select {
	case g := <-jobGroupTerminated:
		assert.Equal(t, group.ID, g.ID)
		assert.Equal(t, orbital.JobGroupStatusCanceled, g.Status)
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for job group cancellation event")
	}

	finalGroup, found, err := manager.GetJobGroup(ctx, group.ID)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, orbital.JobGroupStatusCanceled, finalGroup.Status)
	// job statuses did not change
	assert.Equal(t, orbital.JobStatusProcessing, finalGroup.Jobs[0].Status)
	assert.Equal(t, orbital.JobStatusScheduled, finalGroup.Jobs[1].Status)
	assert.Equal(t, int32(1), jobGroupCanceledCalls.Load())
}

type jobData struct {
	Target string `json:"target"`
}

func resolveTargetFromJobData() orbital.TaskResolveFunc {
	return func(_ context.Context, job orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
		var d jobData
		if err := json.Unmarshal(job.Data, &d); err != nil {
			return nil, err
		}
		return orbital.CompleteTaskResolver().WithTaskInfo([]orbital.TaskInfo{
			{Data: job.Data, Type: job.Type, Target: d.Target},
		}), nil
	}
}

func requireEncode(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func withFastIntervals(manager *orbital.Manager) {
	manager.Config.ConfirmJobWorkerConfig.ExecInterval = 100 * time.Millisecond
	manager.Config.CreateTasksWorkerConfig.ExecInterval = 100 * time.Millisecond
	manager.Config.ReconcileWorkerConfig.ExecInterval = 100 * time.Millisecond
	manager.Config.NotifyWorkerConfig.ExecInterval = 100 * time.Millisecond
	manager.Config.ScheduleJobGroupWorkerConfig.ExecInterval = 100 * time.Millisecond
	manager.Config.NotifyJobGroupWorkerConfig.ExecInterval = 100 * time.Millisecond
	manager.Config.ConfirmJobAfter = 50 * time.Millisecond
}
