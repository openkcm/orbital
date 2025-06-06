package orbital

import "time"

// WorkerConfig defines the configuration for a worker process.
type WorkerConfig struct {
	// NoOfWorkers specifies the number of concurrent workers.
	NoOfWorkers int
	// ExecInterval is the interval between executions.
	ExecInterval time.Duration
	// Timeout is the maximum duration allowed for a single execution.
	Timeout time.Duration
}
