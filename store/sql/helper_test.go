package sql_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	_ "github.com/lib/pq"

	stdsql "database/sql"

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

	mappedPort, err := pgContainer.MappedPort(ctx, "5432")
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
	_, err := db.ExecContext(ctx, "DELETE FROM jobs")
	assert.NoError(t, err)
	_, err = db.ExecContext(ctx, "DELETE FROM tasks")
	assert.NoError(t, err)
	_, err = db.ExecContext(ctx, "DELETE FROM job_cursor")
	assert.NoError(t, err)
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
