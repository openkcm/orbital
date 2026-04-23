package orbital

import (
	"context"
	"errors"
	"fmt"
)

type (
	// Runner drives a transport: it decides where task requests come from
	// and where task responses go. Run blocks until the transport shuts
	// down (typically when ctx is cancelled) and returns any terminal error.
	Runner interface {
		Run(ctx context.Context) error
	}

	// ProcessFunc is the shared pipeline handed to a Runner at construction.
	// It performs signature verification, handler dispatch, and response
	// signing. Sentinel errors (ErrSignatureInvalid, ErrResponseSigning) let
	// sync transports map to transport-level failures; unknown task types
	// stay in-band as a FAILED TaskResponse with a nil error.
	ProcessFunc func(ctx context.Context, req TaskRequest) (TaskResponse, error)

	// Operator composes a Processor (request handling) and a Runner
	// (transport). The Processor and Runner are created independently
	// to avoid circular dependencies between transport and processing.
	Operator struct {
		*Processor

		runner Runner
	}
)

var (
	ErrOperatorInvalidConfig = errors.New("invalid operator configuration")
	ErrProcessorNil          = errors.New("processor cannot be nil")
	ErrRunnerNil             = errors.New("runner cannot be nil")
)

// NewOperator creates a new Operator from a Processor and a Runner.
// Both must be non-nil.
func NewOperator(processor *Processor, runner Runner) (*Operator, error) {
	if processor == nil {
		return nil, fmt.Errorf("%w: %w", ErrOperatorInvalidConfig, ErrProcessorNil)
	}
	if runner == nil {
		return nil, fmt.Errorf("%w: %w", ErrOperatorInvalidConfig, ErrRunnerNil)
	}

	return &Operator{
		Processor: processor,
		runner:    runner,
	}, nil
}

// ListenAndRespond starts the configured Runner. It blocks until the
// Runner exits (typically when ctx is cancelled) and returns any error.
func (o *Operator) ListenAndRespond(ctx context.Context) error {
	return o.runner.Run(ctx)
}
