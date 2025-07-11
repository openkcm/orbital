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
	defWorkTimeout      = 1 * time.Second
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

type setupConfig struct {
	postgres       postgresConfig
	prometheusPort string

	timeout time.Duration

	jobsNum    int
	targetsNum int

	manager         orbital.Config
	jobConfirmFunc  jobConfirmFuncConfig
	taskResolveFunc taskResolveFuncConfig

	operatorMock operatorMockConfig
}

type postgresConfig struct {
	host     string
	port     string
	user     string
	password string
	dbname   string
	sslmode  string
}

type jobConfirmFuncConfig struct {
	latencyAverageSec int
	errorRate         float64
	cancelRate        float64
}

type taskResolveFuncConfig struct {
	latencyAverageSec int
	errorRate         float64
	unfinishedRate    float64
	cancelRate        float64
}

type operatorMockConfig struct {
	latencyAverageSec        int
	reconcileAfterAverageSec int
	unfinishedRate           float64
	errorRate                float64
	taskFailureRate          float64
}

func newConfig() setupConfig {
	config := setupConfig{}

	config.postgres = getPostgresConfig()
	config.prometheusPort = getEnvOrDefault("PROMETHEUS_PORT", "8080")

	config.timeout = getEnvOrDefaultDuration("TIMEOUT", 10*time.Minute)

	config.jobsNum = getEnvOrDefaultInt("JOBS_NUM", 10)
	config.targetsNum = getEnvOrDefaultInt("TARGETS_NUM", 3)

	config.manager = getManagerConfig()
	config.jobConfirmFunc = getJobConfirmFuncConfig()
	config.taskResolveFunc = getTaskResolveFuncConfig()

	config.operatorMock = getOperatorMockConfig()

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

func getJobConfirmFuncConfig() jobConfirmFuncConfig {
	return jobConfirmFuncConfig{
		latencyAverageSec: getEnvOrDefaultInt("JOB_CONFIRM_FUNC_LATENCY_SEC", defJobConfirmFuncLatencySec),
		errorRate:         getEnvOrDefaultFloat("JOB_CONFIRM_FUNC_ERROR_RATE", defJobConfirmFuncErrorRate),
		cancelRate:        getEnvOrDefaultFloat("JOB_CONFIRM_FUNC_CANCEL_RATE", defJobConfirmFuncCancelRate),
	}
}

func getTaskResolveFuncConfig() taskResolveFuncConfig {
	return taskResolveFuncConfig{
		latencyAverageSec: getEnvOrDefaultInt("TASK_RESOLVE_FUNC_LATENCY_SEC", defTaskResolveFuncLatencySec),
		errorRate:         getEnvOrDefaultFloat("TASK_RESOLVE_FUNC_ERROR_RATE", defTaskResolveFuncErrorRate),
		unfinishedRate:    getEnvOrDefaultFloat("TASK_RESOLVE_FUNC_UNFINISHED_RATE", defTaskResolveFuncUnfinishedRate),
		cancelRate:        getEnvOrDefaultFloat("TASK_RESOLVE_FUNC_CANCEL_RATE", defTaskResolveFuncCancelRate),
	}
}

func getOperatorMockConfig() operatorMockConfig {
	return operatorMockConfig{
		latencyAverageSec:        getEnvOrDefaultInt("OPERATOR_MOCK_LATENCY_SEC", defOperatorMockLatencySec),
		reconcileAfterAverageSec: getEnvOrDefaultInt("OPERATOR_MOCK_RECONCILE_AFTER_SEC", defOperatorMockReconcileAfterSec),
		unfinishedRate:           getEnvOrDefaultFloat("OPERATOR_MOCK_UNFINISHED_RATE", defOperatorMockUnfinishedRate),
		errorRate:                getEnvOrDefaultFloat("OPERATOR_MOCK_ERROR_RATE", defOperatorMockErrorRate),
		taskFailureRate:          getEnvOrDefaultFloat("OPERATOR_MOCK_TASK_FAILURE_RATE", defOperatorMockTaskFailureRate),
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
