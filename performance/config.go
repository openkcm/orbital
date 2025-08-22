package performance

import (
	"encoding/json"
	"os"
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

	defServerPort = "8080"
)

type EnvConfig struct {
	Postgres   PostgresConfig
	ServerPort string
}

type Parameters struct {
	TestName string
	Timeout  time.Duration

	JobsNum    int
	TargetsNum int

	Manager         orbital.Config
	JobConfirmFunc  JobConfirmFuncConfig
	TaskResolveFunc TaskResolveFuncConfig

	OperatorMock OperatorMockConfig
}

type PostgresConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
	SSLMode  string
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

func NewEnvConfig() EnvConfig {
	return EnvConfig{
		Postgres:   getPostgresConfig(),
		ServerPort: getEnvOrDefault("SERVER_PORT", defServerPort),
	}
}

func parametersFromJSON(b []byte) (Parameters, error) {
	params := Parameters{}
	err := json.Unmarshal(b, &params)
	if err != nil {
		return Parameters{}, err
	}
	return params, nil
}

func getPostgresConfig() PostgresConfig {
	return PostgresConfig{
		Host:     getEnvOrDefault("POSTGRES_HOST", defPostgresHost),
		Port:     getEnvOrDefault("POSTGRES_PORT", defPostgresPort),
		User:     getEnvOrDefault("POSTGRES_USER", defPostgresUser),
		Password: getEnvOrDefault("POSTGRES_PASSWORD", defPostgresPassword),
		DBName:   getEnvOrDefault("POSTGRES_DB", defPostgresDB),
		SSLMode:  getEnvOrDefault("POSTGRES_SSLMODE", defPostgresSSLMode),
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
