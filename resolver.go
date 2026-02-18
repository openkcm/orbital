package orbital

import (
	"context"
)

const (
	continueTaskResolverResult TaskResolverResultType = iota
	cancelTaskResolverResult
	completeTaskResolverResult
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
		Type() TaskResolverResultType
	}

	// TaskResolverResultType is the type for the task resolver result.
	TaskResolverResultType int

	// TaskResolverCursor is an opaque token used to resume or paginate task resolution.
	// The resolver function controls its format and meaning.
	TaskResolverCursor string
)

// taskResolverProcessing indicates the task resolver should continue resolving tasks.
type taskResolverProcessing struct {
	taskInfo []TaskInfo
	cursor   TaskResolverCursor
}

// Type returns the type of the task resolver result.
func (r taskResolverProcessing) Type() TaskResolverResultType {
	return continueTaskResolverResult
}

// WithTaskInfo sets the task info for the result.
func (r taskResolverProcessing) WithTaskInfo(info []TaskInfo) taskResolverProcessing {
	r.taskInfo = info
	return r
}

// WithCursor sets the cursor for the result.
func (r taskResolverProcessing) WithCursor(cursor TaskResolverCursor) taskResolverProcessing {
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
func ContinueTaskResolver() taskResolverProcessing {
	return taskResolverProcessing{}
}

// taskResolverCanceled indicates the task resolver should cancel the job.
type taskResolverCanceled struct {
	reason string
}

// Type returns the type of the task resolver result.
func (r taskResolverCanceled) Type() TaskResolverResultType {
	return cancelTaskResolverResult
}

// CancelTaskResolver creates a result indicating the job should be canceled for the given reason.
//
// Example:
//
//	return orbital.CancelTaskResolver("invalid job"), nil
func CancelTaskResolver(reason string) taskResolverCanceled {
	return taskResolverCanceled{
		reason: reason,
	}
}

// taskResolverDone indicates the task resolver is done resolving tasks.
type taskResolverDone struct {
	taskInfo []TaskInfo
}

// Type returns the type of the task resolver result.
func (r taskResolverDone) Type() TaskResolverResultType {
	return completeTaskResolverResult
}

// WithTaskInfo sets the task info for the result.
func (r taskResolverDone) WithTaskInfo(info []TaskInfo) taskResolverDone {
	r.taskInfo = info
	return r
}

// CompleteTaskResolver creates a result indicating task resolution is complete.
//
// Example:
//
//	return orbital.CompleteTaskResolver().
//	    WithTaskInfo(finalBatchOfTasks), nil
func CompleteTaskResolver() taskResolverDone {
	return taskResolverDone{}
}
