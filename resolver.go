package orbital

import (
	"context"
)

const (
	ContinueTaskResolverResult TaskResolverResultType = iota
	CancelTaskResolverResult
	CompleteTaskResolverResult
)

type (
	// TaskResolveFunc resolves tasks for a job, potentially in multiple iterations.
	//
	// Return one of:
	//   - ContinueTaskResolver() to continue resolving (with optional tasks and cursor)
	//   - CompleteTaskResolver() to finish resolving (with optional final tasks)
	//   - CancelTaskResolver(reason) to abort the job
	//
	// Errors returned from this function will be treated as recoverable meaning the resolver can retry later.
	//
	// Example:
	//
	//   func myResolver(ctx context.Context, job Job, cursor TaskResolverCursor) (TaskResolverResult, error) {
	//       tasks := []TaskInfo{...} // resolve some tasks
	//       if moreTasksExist {
	//           return orbital.ContinueTaskResolver().WithTaskInfo(tasks).WithCursor("next-page-token"), nil
	//       }
	//       return orbital.CompleteTaskResolver().WithTaskInfo(tasks), nil
	//   }
	TaskResolveFunc func(ctx context.Context, job Job, cursor TaskResolverCursor) (TaskResolverResult, error)

	// TaskResolverResult represents the outcome of a task resolution attempt.
	TaskResolverResult interface {
		TaskResolverResultType() TaskResolverResultType
	}

	// TaskResolverResultType is the type for the task resolver result.
	TaskResolverResultType int

	// TaskResolverCursor is an opaque token used to resume or paginate task resolution.
	// The resolver function controls its format and meaning.
	TaskResolverCursor string
)

// TaskResolverProcessing indicates the task resolver should continue resolving tasks.
type TaskResolverProcessing struct {
	taskInfo []TaskInfo
	cursor   TaskResolverCursor
}

// TaskResolverResultType returns the type of the task resolver result.
func (r TaskResolverProcessing) TaskResolverResultType() TaskResolverResultType {
	return ContinueTaskResolverResult
}

// WithTaskInfo sets the task info for the result.
func (r TaskResolverProcessing) WithTaskInfo(info []TaskInfo) TaskResolverProcessing {
	r.taskInfo = info
	return r
}

// WithCursor sets the cursor for the result.
func (r TaskResolverProcessing) WithCursor(cursor TaskResolverCursor) TaskResolverProcessing {
	r.cursor = cursor
	return r
}

// ContinueTaskResolver creates a result indicating more resolution iterations are needed.
//
// Example:
//
//	return orbital.ContinueTaskResolver().
//	    WithTaskInfo(batchOfTasks).
//	    WithCursor("page-2-token"), nil
func ContinueTaskResolver() TaskResolverProcessing {
	return TaskResolverProcessing{}
}

// TaskResolverCanceled indicates the task resolver should cancel the job.
type TaskResolverCanceled struct {
	reason string
}

// TaskResolverResultType returns the type of the task resolver result.
func (r TaskResolverCanceled) TaskResolverResultType() TaskResolverResultType {
	return CancelTaskResolverResult
}

// CancelTaskResolver creates a result indicating the job should be canceled for the given reason.
//
// Example:
//
//	return orbital.CancelTaskResolver("invalid job"), nil
func CancelTaskResolver(reason string) TaskResolverCanceled {
	return TaskResolverCanceled{
		reason: reason,
	}
}

// TaskResolverDone indicates the task resolver is done resolving tasks.
type TaskResolverDone struct {
	taskInfo []TaskInfo
}

// TaskResolverResultType returns the type of the task resolver result.
func (r TaskResolverDone) TaskResolverResultType() TaskResolverResultType {
	return CompleteTaskResolverResult
}

// WithTaskInfo sets the task info for the result.
func (r TaskResolverDone) WithTaskInfo(info []TaskInfo) TaskResolverDone {
	r.taskInfo = info
	return r
}

// CompleteTaskResolver creates a result indicating task resolution is complete.
//
// Example:
//
//	return orbital.CompleteTaskResolver().
//	    WithTaskInfo(finalBatchOfTasks), nil
func CompleteTaskResolver() TaskResolverDone {
	return TaskResolverDone{}
}
