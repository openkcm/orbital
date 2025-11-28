package sql

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/lib/pq"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/store/query"
)

// SQL is a SQL store implementation of the orbital.Store interface.
type SQL struct {
	db *sql.DB

	tx *sql.Tx
}

var _ orbital.Store = &SQL{}

const pqErrCodeUniqueViolation = "23505" // see https://www.postgresql.org/docs/17/errcodes-appendix.html

var (
	ErrNoColumn = errors.New("no column found")
	ErrNoEntity = errors.New("no entities provided")
)

// New initializes a new SQL store and sets up required tables.
//
//nolint:funlen
func New(ctx context.Context, db *sql.DB) (*SQL, error) {
	s := &SQL{
		db: db,
	}
	stmt := `
   		CREATE TABLE IF NOT EXISTS jobs(
   			id UUID PRIMARY KEY,
   			data BYTEA,
   			type VARCHAR(100) NOT NULL,
   			status VARCHAR(100) NOT NULL,
     		error_message VARCHAR(250),
   			updated_at BIGINT NOT NULL,
   			created_at BIGINT NOT NULL
   		);
		ALTER TABLE jobs 
			ADD COLUMN IF NOT EXISTS external_id VARCHAR(100);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_active_job
			ON jobs (external_id, type)
			WHERE status IN ('` + strings.Join(orbital.TransientStatuses().StringSlice(), "', '") + `');
		DO $$
		BEGIN
			IF EXISTS(
				SELECT * FROM information_schema.columns 
				WHERE table_schema = 'public'
				AND table_name = 'jobs'
				AND column_name = 'error_message'
				AND data_type = 'character varying'
				AND character_maximum_length = 250
			) 
			THEN
				ALTER TABLE jobs ALTER COLUMN error_message TYPE TEXT;
			END IF;
		END $$;
		CREATE TABLE IF NOT EXISTS tasks(
   			id UUID PRIMARY KEY,
   			job_id UUID NOT NULL,
			type VARCHAR(100),
   			data BYTEA,
   			working_state BYTEA,
   			last_reconciled_at BIGINT,
   			reconcile_count BIGINT,
   			reconcile_after_sec BIGINT,
			total_sent_count BIGINT,
			total_received_count BIGINT,
   			etag VARCHAR(100),
   			status VARCHAR(100) NOT NULL,
   			target TEXT NOT NULL,
     		error_message VARCHAR(250),
   			updated_at BIGINT NOT NULL,
   			created_at BIGINT NOT NULL
   		);
		DO $$
		BEGIN
			IF EXISTS(
				SELECT * FROM information_schema.columns 
				WHERE table_schema = 'public'
				AND table_name = 'tasks'
				AND column_name = 'error_message'
				AND data_type = 'character varying'
				AND character_maximum_length = 250
			) 
			THEN
				ALTER TABLE tasks ALTER COLUMN error_message TYPE TEXT;
			END IF;
		END $$;
   		CREATE TABLE IF NOT EXISTS job_cursor(
   			id UUID PRIMARY KEY,
   			cursor VARCHAR(100) NOT NULL,
   			updated_at BIGINT NOT NULL,
   			created_at BIGINT NOT NULL
   		);
   		CREATE TABLE IF NOT EXISTS job_event(
			id UUID PRIMARY KEY,
			is_notified BOOLEAN NOT NULL,
			updated_at BIGINT NOT NULL,
			created_at BIGINT NOT NULL
        );`
	return s, s.execContext(ctx, stmt)
}

// Create implements orbital.Store.
func (s *SQL) Create(ctx context.Context, rs ...orbital.Entity) ([]orbital.Entity, error) {
	if len(rs) == 0 {
		return nil, ErrNoEntity
	}
	stm, params, err := insertQuery(rs...)
	if err != nil {
		return nil, err
	}
	err = s.execContext(ctx, stm, params...)
	if err != nil {
		var pgErr *pq.Error
		if errors.As(err, &pgErr) && pgErr.Code == pqErrCodeUniqueViolation {
			return nil, orbital.ErrEntityUniqueViolation
		}
		return nil, err
	}
	return rs, err
}

// Update implements orbital.Store.
func (s *SQL) Update(ctx context.Context, rs ...orbital.Entity) ([]orbital.Entity, error) {
	if len(rs) == 0 {
		return nil, ErrNoEntity
	}
	stm, params := updateQuery(rs...)
	err := s.execContext(ctx, stm, params...)
	return rs, err
}

// Find implements orbital.Store.
func (s *SQL) Find(ctx context.Context, q query.Query) (orbital.FindResult, error) {
	var out orbital.Entity
	stmt, params := selectQuery(q)
	rows, err := s.queryContext(ctx, stmt, params...)
	if err != nil {
		return orbital.FindResult{}, err
	}
	defer rows.Close()

	rp, ok, err := mapRow(rows)
	if err != nil {
		return orbital.FindResult{}, err
	}
	if !ok {
		return orbital.FindResult{}, nil
	}

	out, err = orbital.TransformToEntity(q.EntityName, rp)
	if err != nil {
		return orbital.FindResult{}, err
	}
	return orbital.FindResult{
		Entity: out,
		Exists: true,
	}, nil
}

// List implements orbital.Store.
func (s *SQL) List(ctx context.Context, q query.Query) (orbital.ListResult, error) {
	stmt, params := selectQuery(q)
	rows, err := s.queryContext(ctx, stmt, params...)
	if err != nil {
		return orbital.ListResult{}, err
	}
	defer rows.Close()

	rp, ok, err := mapRows(rows)
	if err != nil {
		return orbital.ListResult{}, err
	}
	if !ok {
		return orbital.ListResult{}, nil
	}
	out, err := orbital.TransformToEntities(q.EntityName, rp...)
	if err != nil {
		return orbital.ListResult{}, err
	}
	cursor := query.Cursor{}
	if len(out) == q.Limit {
		last := out[len(out)-1]
		cursor.Timestamp = last.CreatedAt
		cursor.ID = last.ID
	}
	return orbital.ListResult{
		Entities: out,
		Cursor:   cursor,
		Exists:   true,
	}, nil
}

// Transaction implements orbital.Store.
func (s *SQL) Transaction(ctx context.Context, txFunc orbital.TransactionFunc) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	err = txFunc(ctx, orbital.Repository{Store: &SQL{db: s.db, tx: tx}})
	if err != nil {
		tx.Rollback() //nolint:errcheck
		return err
	}

	return tx.Commit()
}

// execContext executes a SQL statement with the given context and arguments.
func (s *SQL) execContext(ctx context.Context, stmt string, args ...any) error {
	if s.tx != nil {
		_, err := s.tx.ExecContext(ctx, stmt, args...)
		return err
	}
	_, err := s.db.ExecContext(ctx, stmt, args...)
	return err
}

// queryContext executes a SQL query with the given context and arguments.
func (s *SQL) queryContext(ctx context.Context, stmt string, args ...any) (*sql.Rows, error) {
	if s.tx != nil {
		return s.tx.QueryContext(ctx, stmt, args...)
	}
	return s.db.QueryContext(ctx, stmt, args...)
}

func mapRows(rows *sql.Rows) ([]map[string]any, bool, error) {
	fields, err := rows.Columns()
	if err != nil {
		return nil, false, err
	}
	fieldsCount := len(fields)
	results := make([]map[string]any, 0)
	for rows.Next() {
		dests := make([]any, fieldsCount)
		row := make(map[string]any, fieldsCount)

		for i := range dests {
			dests[i] = &dests[i]
		}
		err := rows.Scan(dests...)
		if err != nil {
			return nil, false, err
		}
		for i, dest := range dests {
			row[fields[i]] = dest
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	return results, len(results) > 0, nil
}

func mapRow(rows *sql.Rows) (map[string]any, bool, error) {
	fields, err := rows.Columns()
	if err != nil {
		return nil, false, err
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}

	fieldsCount := len(fields)
	row := make(map[string]any, fieldsCount)
	dests := make([]any, fieldsCount)

	for i := range dests {
		dests[i] = &dests[i]
	}
	err = rows.Scan(dests...)
	if err != nil {
		return nil, false, err
	}
	for i, dest := range dests {
		row[fields[i]] = dest
	}
	return row, true, nil
}

// selectQuery builds a SELECT query using the unified builder.
func selectQuery(q query.Query) (string, []any) {
	builder := newSelectQueryBuilder()
	return builder.build(q)
}

// insertQuery builds an INSERT query using the unified builder.
func insertQuery(entities ...orbital.Entity) (string, []any, error) {
	if err := validateEntities(entities); err != nil {
		return "", nil, err
	}

	builder := newInsertQueryBuilder()
	return builder.build(entities...)
}

// updateQuery builds UPDATE queries using the unified builder.
func updateQuery(entities ...orbital.Entity) (string, []any) {
	builder := newUpdateQueryBuilder()
	return builder.build(entities...)
}

// validateEntities checks if the provided entities are valid and contain all required columns.
func validateEntities(entities []orbital.Entity) error {
	if len(entities) == 0 {
		return ErrNoEntity
	}

	referenceColumns := make(map[string]struct{})
	for col := range entities[0].Values {
		referenceColumns[col] = struct{}{}
	}

	for _, entity := range entities {
		for col := range referenceColumns {
			if _, ok := entity.Values[col]; !ok {
				return ErrNoColumn
			}
		}
	}

	return nil
}
