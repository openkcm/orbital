package orbital

import (
	"context"
	"time"

	"github.com/google/uuid"

	slogctx "github.com/veqryn/slog-context"

	"github.com/openkcm/orbital/internal/clock"
)

const (
	handlerResultProcessing handlerResultType = "PROCESSING"
	handlerResultDone       handlerResultType = "DONE"
	handlerResultFailed     handlerResultType = "FAILED"
)

type (
	// Handler processes a handler request and populates the handler response.
	// Per default, the handler response will continue processing and the working state will be preserved.
	Handler func(ctx context.Context, request HandlerRequest, resp *HandlerResponse)

	// HandlerRequest contains information extracted from orbital.TaskRequest
	// that are relevant for the operator's processing.
	HandlerRequest struct {
		TaskID               uuid.UUID
		TaskType             string
		TaskData             []byte
		TaskRawWorkingState  []byte
		TaskCreatedAt        time.Time
		TaskLastReconciledAt time.Time
	}

	// HandlerResponse is used by the handler to indicate the result of processing.
	HandlerResponse struct {
		reconcileAfterSec uint64
		rawWorkingState   []byte
		result            handlerResultType
		errorMessage      string

		workingState *WorkingState
	}

	handlerResultType string
)

// WorkingState returns the WorkingState from the HandlerResponse.
// It returns an error if the decoding of the rawWorkingState fails.
//
// The WorkingState is automatically encoded back into the orbital.TaskResponse.
// If the working state is not decoded,
// if the changes are discarded,
// or if there is an error during encoding,
// the rawWorkingState field will take precedence.
func (r *HandlerResponse) WorkingState() (*WorkingState, error) {
	if r.workingState != nil {
		return r.workingState, nil
	}

	if len(r.rawWorkingState) == 0 {
		workingState := &WorkingState{
			s: make(map[string]any),
		}
		r.workingState = workingState
		return workingState, nil
	}

	workingState, err := decodeWorkingState(r.rawWorkingState)
	if err != nil {
		return nil, err
	}
	r.workingState = workingState

	return workingState, nil
}

// UseRawWorkingState allows the handler to set the rawWorkingState directly.
//
// Note: If the WorkingState is used in the handler,
// the changes in the WorkingState take precedence over the rawWorkingState.
func (r *HandlerResponse) UseRawWorkingState(raw []byte) {
	r.rawWorkingState = raw
}

// ContinueAndWaitFor indicates that the handler has processed the request and wants to continue processing after a defined duration.
//
// Note: Duration will be converted to seconds and rounded down.
func (r *HandlerResponse) ContinueAndWaitFor(duration time.Duration) {
	r.reconcileAfterSec = uint64(duration.Seconds())
	r.result = handlerResultProcessing
}

// Fail indicates that the handler has processed the request and wants to mark the task as failed with a reason.
//
// Note: This will terminate the processing of the task.
func (r *HandlerResponse) Fail(reason string) {
	r.errorMessage = reason
	r.result = handlerResultFailed
}

// Complete indicates that the handler has processed the request and wants to mark the task as completed.
//
// Note: This will terminate the processing of the task.
func (r *HandlerResponse) Complete() {
	r.result = handlerResultDone
}

func executeHandler(ctx context.Context, h Handler, req TaskRequest) TaskResponse {
	resp := req.prepareResponse()

	hReq := HandlerRequest{
		TaskID:               req.TaskID,
		TaskType:             req.Type,
		TaskData:             req.Data,
		TaskCreatedAt:        clock.TimeFromUnixNano(req.TaskCreatedAt),
		TaskLastReconciledAt: clock.TimeFromUnixNano(req.TaskLastReconciledAt),
		TaskRawWorkingState:  req.WorkingState,
	}
	hResp := HandlerResponse{
		result:          handlerResultProcessing,
		rawWorkingState: req.WorkingState,
	}

	start := time.Now()
	h(ctx, hReq, &hResp)
	slogctx.Debug(ctx, "task handler finished", "processingTime", time.Since(start))

	resp.Status = string(hResp.result)
	resp.ErrorMessage = hResp.errorMessage
	resp.ReconcileAfterSec = hResp.reconcileAfterSec
	resp.WorkingState = hResp.rawWorkingState
	if hResp.workingState != nil && hResp.workingState.s != nil {
		encodedState, err := hResp.workingState.encode()
		if err != nil {
			slogctx.Warn(ctx, "failed to encode working state", "error", err)
			return resp
		}
		resp.WorkingState = encodedState
	}

	return resp
}
