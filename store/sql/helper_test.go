//go:build integration
// +build integration

package sql_test

import (
	stdsql "database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/lib/pq"

	"github.com/openkcm/orbital/store/sql"
)

var (
	host     = os.Getenv("DB_HOST")
	port     = os.Getenv("DB_PORT")
	user     = os.Getenv("DB_USER")
	password = os.Getenv("DB_PASS")
	dbname   = os.Getenv("DB_NAME")
	sslmode  = "disable"
)

func init() {
	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "5432"
	}
	if user == "" {
		user = "postgres"
	}
	if password == "" {
		password = "secret"
	}
	if dbname == "" {
		dbname = "orbital"
	}
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
