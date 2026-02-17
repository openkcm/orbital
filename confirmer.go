package orbital

import (
	"context"
)

const (
	continueJobConfirmerResult JobConfirmerResultType = iota
	cancelJobConfirmerResult
	completeJobConfirmerResult
)

type (
	// JobConfirmFunc validates a job's readiness before task resolution.
	// It can verify that the underlying resource is available and in the expected state.
	//
	// Return one of:
	//   - ContinueJobConfirmer() to continue confirming
	//   - CompleteJobConfirmer() to mark the job as confirmed and ready for resolving tasks
	//   - CancelJobConfirmer(reason) to cancel the job with a reason
	//
	// Errors returned from this function will be treated as recoverable meaning the confirmer can retry later.
	//
	// Example:
	//
	//	 func myConfirmer(ctx context.Context, job Job) (JobConfirmerResult, error) {
	//	     if resourceIsAvailableAndValid() {
	//	         return orbital.CompleteJobConfirmer(), nil
	//	     }
	//	     return orbital.ContinueJobConfirmer(), nil
	//	 }
	JobConfirmFunc func(ctx context.Context, job Job) (JobConfirmerResult, error)

	// JobConfirmerResult represents the outcome of a job confirmation attempt.
	JobConfirmerResult interface {
		Type() JobConfirmerResultType
	}

	// JobConfirmerResultType is the type for the job confirmer result.
	JobConfirmerResultType int
)

// jobConfirmerProcessing indicates the job confirmer should continue confirming the job.
type jobConfirmerProcessing struct{}

// Type returns the type of the job confirmer result.
func (r jobConfirmerProcessing) Type() JobConfirmerResultType {
	return continueJobConfirmerResult
}

// ContinueJobConfirmer creates a result indicating that the job confirmer should continue confirming the job.
//
// Example:
//
//	return orbital.ContinueJobConfirmer(), nil
func ContinueJobConfirmer() jobConfirmerProcessing {
	return jobConfirmerProcessing{}
}

// jobConfirmerCanceled indicates the job confirmer should cancel the job with a reason.
type jobConfirmerCanceled struct {
	reason string
}

// Type returns the type of the job confirmer result.
func (r jobConfirmerCanceled) Type() JobConfirmerResultType {
	return cancelJobConfirmerResult
}

// CancelJobConfirmer creates a result indicating that the job confirmer should cancel the job with a reason.
//
// Example:
//
//	return orbital.CancelJobConfirmer("resource not available"), nil
func CancelJobConfirmer(reason string) jobConfirmerCanceled {
	return jobConfirmerCanceled{reason: reason}
}

// jobConfirmerDone indicates the job confirmer should mark the job as confirmed and ready for resolving tasks.
type jobConfirmerDone struct{}

// Type returns the type of the job confirmer result.
func (r jobConfirmerDone) Type() JobConfirmerResultType {
	return completeJobConfirmerResult
}

// CompleteJobConfirmer creates a result indicating that the job confirmer should mark the job as confirmed and ready for resolving tasks.
//
// Example:
//
//	return orbital.CompleteJobConfirmer(), nil
func CompleteJobConfirmer() jobConfirmerDone {
	return jobConfirmerDone{}
}
