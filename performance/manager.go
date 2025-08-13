package performance

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/client/embedded"
)

var (
	ErrClient      = errors.New("client error")
	ErrJobConfirm  = errors.New("job confirm error")
	ErrTaskResolve = errors.New("task resolve error")
)

func initManager(repo *orbital.Repository, cfg Parameters, termination *termination) (*orbital.Manager, error) {
	targetToClient := make(map[string]orbital.Initiator, cfg.TargetsNum)
	for i := range cfg.TargetsNum {
		target := targetName(i)
		client, err := operatorMock(cfg.OperatorMock)
		if err != nil {
			return nil, fmt.Errorf("failed to create client for target %s: %w", target, err)
		}

		targetToClient[target] = client
	}

	manager, err := orbital.NewManager(
		repo,
		taskResolveFunc(cfg.TargetsNum, cfg.TaskResolveFunc),

		orbital.WithJobConfirmFunc(jobConfirmFunc(cfg.JobConfirmFunc)),

		orbital.WithTargetClients(targetToClient),

		orbital.WithJobDoneEventFunc(jobTerminateFunc(&termination.jobDones, termination)),
		orbital.WithJobFailedEventFunc(jobTerminateFunc(&termination.jobFailures, termination)),
		orbital.WithJobCanceledEventFunc(jobTerminateFunc(&termination.jobCancellations, termination)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create orbital manager: %w", err)
	}
	manager.Config = cfg.Manager
	return manager, nil
}

func targetName(i int) string {
	return fmt.Sprintf("target-%d", i)
}

func operatorMock(cfg OperatorMockConfig) (*embedded.Client, error) {
	return embedded.NewClient(
		func(_ context.Context, request orbital.TaskRequest) (orbital.TaskResponse, error) {
			errRand := rand.Float64()
			if errRand <= cfg.ErrorRate {
				return orbital.TaskResponse{}, ErrClient
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
				response.ReconcileAfterSec = int64(randIntN(cfg.ReconcileAfterAverageSec * 2))
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
		},
	)
}

func taskResolveFunc(targetsNum int, cfg TaskResolveFuncConfig) orbital.TaskResolveFunc {
	return func(_ context.Context, _ orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
		errRand := rand.Float64()
		if errRand <= cfg.ErrorRate {
			return orbital.TaskResolverResult{}, ErrTaskResolve
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

func jobConfirmFunc(cfg JobConfirmFuncConfig) orbital.JobConfirmFunc {
	return func(_ context.Context, _ orbital.Job) (orbital.JobConfirmResult, error) {
		errRand := rand.Float64()
		if errRand <= cfg.ErrorRate {
			return orbital.JobConfirmResult{}, ErrJobConfirm
		}

		timeRand := randIntN(cfg.LatencyAverageSec * 2)
		time.Sleep(time.Duration(timeRand) * time.Second)

		cancelRand := rand.Float64()
		if cancelRand <= cfg.CancelRate {
			return orbital.JobConfirmResult{
				IsCanceled: true,
			}, nil
		}

		return orbital.JobConfirmResult{
			Done: true,
		}, nil
	}
}

func jobTerminateFunc(terminatedJobs *atomic.Int64, t *termination) orbital.JobTerminatedEventFunc {
	return func(_ context.Context, _ orbital.Job) error {
		terminatedJobs.Add(1)
		total := t.jobDones.Load() + t.jobFailures.Load() + t.jobCancellations.Load()
		if total >= int64(t.expectedJobs) {
			select {
			case <-t.finished:
				// Already closed
			default:
				close(t.finished)
			}
		}
		return nil
	}
}

func randIntN(n int) int {
	if n <= 0 {
		return 0
	}
	return rand.IntN(n)
}
