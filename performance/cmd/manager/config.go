package main

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/openkcm/orbital"
)

const (
	defPostgresHost     = "localhost"
	defPostgresPort     = "5432"
	defPostgresUser     = "postgres"
	defPostgresPassword = "secret"
	defPostgresDB       = "orbital"
	defPostgresSSLMode  = "disable"
)

const (
	defConfirmJobDelay  = 1 * time.Second
	defNoOfWorker       = 5
	defWorkTimeout      = 10 * time.Second
	defTaskLimitNum     = 500
	defWorkExecInterval = 1 * time.Microsecond
)

const (
	defTaskResolveFuncLatencySec     = 0
	defTaskResolveFuncErrorRate      = 0.0
	defTaskResolveFuncUnfinishedRate = 0.0
	defTaskResolveFuncCancelRate     = 0.0
)

const (
	defJobConfirmFuncLatencySec = 0
	defJobConfirmFuncErrorRate  = 0.0
	defJobConfirmFuncCancelRate = 0.0
)

const (
	defOperatorMockLatencySec        = 0
	defOperatorMockReconcileAfterSec = 0
	defOperatorMockUnfinishedRate    = 0.0
	defOperatorMockErrorRate         = 0.0
	defOperatorMockTaskFailureRate   = 0.0
)

type EnvConfig struct {
	postgres       postgresConfig
	prometheusPort string
}

type TestConfig struct {
	TestName string
	Timeout  time.Duration

	JobsNum    int
	TargetsNum int

	Manager         orbital.Config
	JobConfirmFunc  JobConfirmFuncConfig
	TaskResolveFunc TaskResolveFuncConfig

	OperatorMock OperatorMockConfig
}

type postgresConfig struct {
	host     string
	port     string
	user     string
	password string
	dbname   string
	sslmode  string
}

type JobConfirmFuncConfig struct {
	LatencyAverageSec int
	ErrorRate         float64
	CancelRate        float64
}

type TaskResolveFuncConfig struct {
	LatencyAverageSec int
	ErrorRate         float64
	UnfinishedRate    float64
	CancelRate        float64
}

type OperatorMockConfig struct {
	LatencyAverageSec        int
	ReconcileAfterAverageSec int
	UnfinishedRate           float64
	ErrorRate                float64
	TaskFailureRate          float64
}

func newEnvConfig() EnvConfig {
	config := EnvConfig{}
	config.postgres = getPostgresConfig()
	config.prometheusPort = getEnvOrDefault("PROMETHEUS_PORT", "8080")
	return config
}

func getPostgresConfig() postgresConfig {
	return postgresConfig{
		host:     getEnvOrDefault("POSTGRES_HOST", defPostgresHost),
		port:     getEnvOrDefault("POSTGRES_PORT", defPostgresPort),
		user:     getEnvOrDefault("POSTGRES_USER", defPostgresUser),
		password: getEnvOrDefault("POSTGRES_PASSWORD", defPostgresPassword),
		dbname:   getEnvOrDefault("POSTGRES_DB", defPostgresDB),
		sslmode:  getEnvOrDefault("POSTGRES_SSLMODE", defPostgresSSLMode),
	}
}

func getManagerConfig() orbital.Config {
	return orbital.Config{
		TaskLimitNum:       getEnvOrDefaultInt("TASK_LIMIT_NUM", defTaskLimitNum),
		ConfirmJobInterval: getEnvOrDefaultDuration("CONFIRM_JOB_INTERVAL", defConfirmJobDelay),
		ConfirmJobWorkerConfig: orbital.WorkerConfig{
			NoOfWorkers:  getEnvOrDefaultInt("CONFIRM_JOB_WORKER_NUM", defNoOfWorker),
			Timeout:      getEnvOrDefaultDuration("CONFIRM_JOB_WORK_TIMEOUT", defWorkTimeout),
			ExecInterval: getEnvOrDefaultDuration("CONFIRM_JOB_WORK_EXEC_INTERVAL", defWorkExecInterval),
		},
		CreateTasksWorkerConfig: orbital.WorkerConfig{
			NoOfWorkers:  getEnvOrDefaultInt("CREATE_TASKS_WORKER_NUM", defNoOfWorker),
			Timeout:      getEnvOrDefaultDuration("CREATE_TASKS_WORK_TIMEOUT", defWorkTimeout),
			ExecInterval: getEnvOrDefaultDuration("CREATE_TASKS_WORK_EXEC_INTERVAL", defWorkExecInterval),
		},
		ReconcileWorkerConfig: orbital.WorkerConfig{
			NoOfWorkers:  getEnvOrDefaultInt("RECONCILE_WORKER_NUM", defNoOfWorker),
			Timeout:      getEnvOrDefaultDuration("RECONCILE_WORK_TIMEOUT", defWorkTimeout),
			ExecInterval: getEnvOrDefaultDuration("RECONCILE_WORK_EXEC_INTERVAL", defWorkExecInterval),
		},
		NotifyWorkerConfig: orbital.WorkerConfig{
			NoOfWorkers:  getEnvOrDefaultInt("NOTIFY_WORKER_NUM", defNoOfWorker),
			Timeout:      getEnvOrDefaultDuration("NOTIFY_WORK_TIMEOUT", defWorkTimeout),
			ExecInterval: getEnvOrDefaultDuration("NOTIFY_WORK_EXEC_INTERVAL", defWorkExecInterval),
		},
	}
}

func getJobConfirmFuncConfig() JobConfirmFuncConfig {
	return JobConfirmFuncConfig{
		LatencyAverageSec: getEnvOrDefaultInt("JOB_CONFIRM_FUNC_LATENCY_SEC", defJobConfirmFuncLatencySec),
		ErrorRate:         getEnvOrDefaultFloat("JOB_CONFIRM_FUNC_ERROR_RATE", defJobConfirmFuncErrorRate),
		CancelRate:        getEnvOrDefaultFloat("JOB_CONFIRM_FUNC_CANCEL_RATE", defJobConfirmFuncCancelRate),
	}
}

func getTaskResolveFuncConfig() TaskResolveFuncConfig {
	return TaskResolveFuncConfig{
		LatencyAverageSec: getEnvOrDefaultInt("TASK_RESOLVE_FUNC_LATENCY_SEC", defTaskResolveFuncLatencySec),
		ErrorRate:         getEnvOrDefaultFloat("TASK_RESOLVE_FUNC_ERROR_RATE", defTaskResolveFuncErrorRate),
		UnfinishedRate:    getEnvOrDefaultFloat("TASK_RESOLVE_FUNC_UNFINISHED_RATE", defTaskResolveFuncUnfinishedRate),
		CancelRate:        getEnvOrDefaultFloat("TASK_RESOLVE_FUNC_CANCEL_RATE", defTaskResolveFuncCancelRate),
	}
}

func getOperatorMockConfig() OperatorMockConfig {
	return OperatorMockConfig{
		LatencyAverageSec:        getEnvOrDefaultInt("OPERATOR_MOCK_LATENCY_SEC", defOperatorMockLatencySec),
		ReconcileAfterAverageSec: getEnvOrDefaultInt("OPERATOR_MOCK_RECONCILE_AFTER_SEC", defOperatorMockReconcileAfterSec),
		UnfinishedRate:           getEnvOrDefaultFloat("OPERATOR_MOCK_UNFINISHED_RATE", defOperatorMockUnfinishedRate),
		ErrorRate:                getEnvOrDefaultFloat("OPERATOR_MOCK_ERROR_RATE", defOperatorMockErrorRate),
		TaskFailureRate:          getEnvOrDefaultFloat("OPERATOR_MOCK_TASK_FAILURE_RATE", defOperatorMockTaskFailureRate),
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func getEnvOrDefaultInt(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	valueInt, err := strconv.Atoi(value)
	if err != nil {
		log.Fatalf("Invalid value for %s: %s", key, value)
	}
	return valueInt
}

func getEnvOrDefaultFloat(key string, defaultValue float64) float64 {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	valueFloat, err := strconv.ParseFloat(value, 64)
	if err != nil {
		log.Fatalf("Invalid value for %s: %s", key, value)
	}
	return valueFloat
}

func getEnvOrDefaultDuration(key string, defaultValue time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	valueDuration, err := time.ParseDuration(value)
	if err != nil {
		log.Fatalf("Invalid value for %s: %s", key, value)
	}
	return valueDuration
}
