package orbital_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	_ "github.com/lib/pq"

	stdsql "database/sql"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/store/sql"
)

const (
	host     = "localhost"
	user     = "postgres"
	password = "secret"
	dbname   = "orbital"
	sslmode  = "disable"
)

var port = "5432"

func TestMain(m *testing.M) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx, "postgres:17-alpine",
		postgres.WithDatabase(dbname),
		postgres.WithUsername(user),
		postgres.WithPassword(password),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Println("Failed to start PostgreSQL container:", err)
		os.Exit(1)
	}

	mappedPort, err := pgContainer.MappedPort(ctx, nat.Port(port))
	if err != nil {
		log.Println("Failed to get mapped port for PostgreSQL container:", err)
		os.Exit(1)
	}
	port = mappedPort.Port()

	code := m.Run()

	if err := pgContainer.Terminate(ctx); err != nil {
		log.Println("Failed to terminate PostgreSQL container:", err)
		os.Exit(1)
	}

	os.Exit(code)
}

func clearTables(t *testing.T, db *stdsql.DB) {
	t.Helper()
	ctx := t.Context()
	assert.NoError(t, clearJobTable(ctx, db))
	assert.NoError(t, clearTasksTable(ctx, db))
	assert.NoError(t, clearJobCursorTable(ctx, db))
	assert.NoError(t, clearJobEventTable(ctx, db))
}

func clearJobTable(ctx context.Context, db *stdsql.DB) error {
	_, err := db.ExecContext(ctx, "DELETE FROM jobs")
	return err
}

func clearTasksTable(ctx context.Context, db *stdsql.DB) error {
	_, err := db.ExecContext(ctx, "DELETE FROM tasks")
	return err
}

func clearJobCursorTable(ctx context.Context, db *stdsql.DB) error {
	_, err := db.ExecContext(ctx, "DELETE FROM job_cursor")
	return err
}

func clearJobEventTable(ctx context.Context, db *stdsql.DB) error {
	_, err := db.ExecContext(ctx, "DELETE FROM job_event")
	return err
}

func createSQLStore(t *testing.T) (*stdsql.DB, *sql.SQL) {
	t.Helper()
	ctx := t.Context()
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		host, port, user, password, dbname, sslmode)
	db, err := stdsql.Open("postgres", connStr)
	require.NoError(t, err)

	t.Cleanup(func() {
		db.Close()
	})

	store, err := sql.New(ctx, db)
	require.NoError(t, err)

	return db, store
}

func mockTaskResolveFunc() orbital.TaskResolveFunc {
	return func(_ context.Context, _ orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
		return orbital.TaskResolverResult{}, nil
	}
}

func mockTerminatedFunc() orbital.JobTerminatedEventFunc {
	return func(_ context.Context, _ orbital.Job) error {
		return nil
	}
}
