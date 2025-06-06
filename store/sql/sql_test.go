//go:build integration
// +build integration

package sql_test

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/store/query"
	"github.com/openkcm/orbital/store/sql"
)

var errRandom = errors.New("random error")

func TestNew(t *testing.T) {
	t.Run("should not return error", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, _ := createSQLStore(t)

		// when
		_, err := sql.New(ctx, db)

		// then
		assert.NoError(t, err)
	})

	t.Run("should create entities", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, _ := createSQLStore(t)

		// when
		_, err := sql.New(ctx, db)

		// then
		assert.NoError(t, err)

		var count int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pg_tables WHERE tablename='jobs'").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 1, count)

		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pg_tables WHERE tablename='tasks'").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 1, count)

		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pg_tables WHERE tablename='job_cursor'").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})
}

func TestColumnsAndStruct(t *testing.T) {
	ctx := t.Context()
	db, _ := createSQLStore(t)

	_, err := sql.New(ctx, db)
	assert.NoError(t, err)

	validateColumnsAndEntity(t, db, orbital.Job{})
	validateColumnsAndEntity(t, db, orbital.Task{})
	validateColumnsAndEntity(t, db, orbital.JobCursor{})
}

func TestCreate(t *testing.T) {
	// given
	id := uuid.New()
	utcTime := utcUnix()

	t.Run("should return error if entity is empty", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)

		// when
		_, err := store.Create(ctx, []orbital.Entity{}...)

		// then
		assert.Error(t, err)
	})

	t.Run("should return error if entity name is unknown", func(t *testing.T) {
		// given
		entity := orbital.Entity{
			Name:      "unknown",
			ID:        id,
			CreatedAt: utcTime,
			UpdatedAt: utcTime,
			Values: map[string]any{
				"id":         id.String(),
				"cursor":     "cursor",
				"created_at": utcTime,
				"updated_at": utcTime,
			},
		}
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)

		// when
		entities, err := store.Create(ctx, entity)

		// then
		assert.Error(t, err)
		assert.Nil(t, entities)
	})

	t.Run("should create entity", func(t *testing.T) {
		// given
		entity := orbital.Entity{
			Name:      "job_cursor",
			ID:        id,
			CreatedAt: utcTime,
			UpdatedAt: utcTime,
			Values: map[string]any{
				"id":         []uint8(id.String()),
				"cursor":     "cursor",
				"created_at": utcTime,
				"updated_at": utcTime,
			},
		}
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)

		// when
		entities, err := store.Create(ctx, entity)

		// then
		assert.NoError(t, err)
		assert.Len(t, entities, 1)
		assert.Equal(t, entity, entities[0])

		result, err := store.List(ctx, query.Query{
			EntityName: query.EntityNameJobCursor,
		})
		assert.NoError(t, err)
		assert.True(t, result.Exists)
		assert.Len(t, result.Entities, 1)
		assert.Equal(t, entities, result.Entities)
	})

	t.Run("should create multiple entity", func(t *testing.T) {
		// given
		id2 := uuid.New()
		entitys := []orbital.Entity{
			{
				Name:      "job_cursor",
				ID:        id,
				CreatedAt: utcTime,
				UpdatedAt: utcTime,
				Values: map[string]any{
					"id":         []uint8(id.String()),
					"cursor":     "cursor",
					"created_at": utcTime,
					"updated_at": utcTime,
				},
			},
			{
				Name:      "job_cursor",
				ID:        id2,
				CreatedAt: utcTime + 1,
				UpdatedAt: utcTime + 1,
				Values: map[string]any{
					"id":         []uint8(id2.String()),
					"cursor":     "cursor-1",
					"created_at": utcTime + 1,
					"updated_at": utcTime + 1,
				},
			},
		}
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		ctx := t.Context()

		// when
		entities, err := store.Create(ctx, entitys...)

		// then
		assert.NoError(t, err)
		assert.Len(t, entities, 2)
		assert.Equal(t, entitys, entities)

		result, err := store.List(ctx, query.Query{
			EntityName: query.EntityNameJobCursor,
		})
		assert.NoError(t, err)
		assert.True(t, result.Exists)
		assert.Len(t, result.Entities, 2)
		assert.Equal(t, entities, result.Entities)
	})

	t.Run("should return error if entities values are missing differ", func(t *testing.T) {
		// given
		id2 := uuid.New()
		entitys := []orbital.Entity{
			{
				Name:      "job_cursor",
				ID:        id,
				CreatedAt: utcTime,
				UpdatedAt: utcTime,
				Values: map[string]any{
					"id":         id,
					"cursor":     "cursor",
					"created_at": utcTime,
					"updated_at": utcTime,
				},
			},
			{
				Name:      "job_cursor",
				ID:        id2,
				CreatedAt: utcTime,
				UpdatedAt: utcTime,
				Values: map[string]any{
					"id":         id2,
					"created_at": utcTime,
					"updated_at": utcTime,
				},
			},
		}
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)

		// when
		entities, err := store.Create(ctx, entitys...)

		// then
		assert.Equal(t, sql.ErrNoColumn, err)
		assert.Nil(t, entities)
	})
}

func TestUpdate(t *testing.T) {
	t.Run("should return error if entity is empty", func(t *testing.T) {
		// given
		ctx := t.Context()
		_, store := createSQLStore(t)

		// when
		_, err := store.Update(ctx, []orbital.Entity{}...)

		// then
		assert.Equal(t, sql.ErrNoEntity, err)
	})

	t.Run("should not return error if there are no entity to updated", func(t *testing.T) {
		// given
		id := uuid.New()
		utcTime := utcUnix()
		ctx := t.Context()
		_, store := createSQLStore(t)

		entityUpdated := orbital.Entity{
			Name:      "job_cursor",
			ID:        id,
			CreatedAt: utcTime,
			UpdatedAt: utcTime,
			Values: map[string]any{
				"id":         id.String(),
				"cursor":     "cursor-2",
				"created_at": utcTime,
				"updated_at": utcTime,
			},
		}

		// when
		entities, err := store.Update(ctx, entityUpdated)

		// then
		assert.NoError(t, err)
		assert.Len(t, entities, 1)
	})

	t.Run("should update the entity", func(t *testing.T) {
		// given
		id := uuid.New()
		utcTime := utcUnix()
		entity := orbital.Entity{
			Name:      "job_cursor",
			ID:        id,
			CreatedAt: utcTime,
			UpdatedAt: utcTime,
			Values: map[string]any{
				"id":         []uint8(id.String()),
				"cursor":     "cursor",
				"created_at": utcTime,
				"updated_at": utcTime,
			},
		}
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)

		_, err := store.Create(ctx, entity)
		assert.NoError(t, err)

		entityUpdated := orbital.Entity{
			Name:      "job_cursor",
			ID:        id,
			CreatedAt: utcTime,
			UpdatedAt: utcTime,
			Values: map[string]any{
				"id":         []uint8(id.String()),
				"cursor":     "cursor-2",
				"created_at": utcTime,
				"updated_at": utcTime,
			},
		}

		// when
		time.Sleep(2 * time.Second) // Ensure the updated_at timestamp is different
		entities, err := store.Update(ctx, entityUpdated)

		// then
		assert.NoError(t, err)
		assert.Len(t, entities, 1)
		assert.Equal(t, entityUpdated.ID, entities[0].ID)
		assert.Equal(t, entityUpdated.CreatedAt, entities[0].CreatedAt)
		assert.Less(t, entityUpdated.UpdatedAt, entities[0].UpdatedAt)

		result, err := store.List(ctx, query.Query{
			EntityName: query.EntityNameJobCursor,
		})
		assert.NoError(t, err)
		assert.True(t, result.Exists)
		assert.Len(t, result.Entities, 1)
		assert.Equal(t, entities, result.Entities)
	})
}

func TestFind(t *testing.T) {
	t.Run("should return false if there are no rows found", func(t *testing.T) {
		// given
		ctx := t.Context()
		_, store := createSQLStore(t)

		// when
		result, err := store.Find(ctx, query.Query{
			EntityName: query.EntityNameJobCursor,
		})

		// then
		assert.NoError(t, err)
		assert.False(t, result.Exists)
	})

	t.Run("should return 1st entity in the database", func(t *testing.T) {
		// given
		id := uuid.New()
		id2 := uuid.New()
		utcTime := utcUnix()
		expEntitys := []orbital.Entity{
			{
				Name:      "job_cursor",
				ID:        id,
				CreatedAt: utcTime,
				UpdatedAt: utcTime,
				Values: map[string]any{
					"id":         []uint8(id.String()),
					"cursor":     "cursor",
					"created_at": utcTime,
					"updated_at": utcTime,
				},
			},

			{
				Name:      "job_cursor",
				ID:        id2,
				CreatedAt: utcTime + 1,
				UpdatedAt: utcTime + 1,
				Values: map[string]any{
					"id":         []uint8(id2.String()),
					"cursor":     "cursor-2",
					"created_at": utcTime + 1,
					"updated_at": utcTime + 1,
				},
			},
		}

		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		_, err := store.Create(ctx, expEntitys...)
		assert.NoError(t, err)

		// when
		result, err := store.Find(ctx, query.Query{
			EntityName: query.EntityNameJobCursor,
		})

		// then
		assert.NoError(t, err)
		assert.True(t, result.Exists)
		assert.Equal(t, expEntitys[0], result.Entity)
	})

	t.Run("should return entity with ID", func(t *testing.T) {
		// given
		id := uuid.New()
		id2 := uuid.New()
		utcTime := utcUnix()
		expEntitys := []orbital.Entity{
			{
				Name:      "job_cursor",
				ID:        id,
				CreatedAt: utcTime,
				UpdatedAt: utcTime,
				Values: map[string]any{
					"id":         []uint8(id.String()),
					"cursor":     "cursor",
					"created_at": utcTime,
					"updated_at": utcTime,
				},
			},
			{
				Name:      "job_cursor",
				ID:        id2,
				CreatedAt: utcTime,
				UpdatedAt: utcTime,
				Values: map[string]any{
					"id":         []uint8(id2.String()),
					"cursor":     "cursor-2",
					"created_at": utcTime,
					"updated_at": utcTime,
				},
			},
		}

		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		_, err := store.Create(ctx, expEntitys...)
		assert.NoError(t, err)

		// when
		result, err := store.Find(
			ctx,
			query.Query{
				EntityName: query.EntityNameJobCursor,
				Clauses:    []query.Clause{query.ClauseWithID(id2)},
			},
		)

		// then
		assert.NoError(t, err)
		assert.True(t, result.Exists)
		assert.Equal(t, expEntitys[1], result.Entity)
	})
}

func TestList(t *testing.T) {
	// given
	utcTime := utcUnix()
	expEntities := make([]orbital.Entity, 3)
	for i := range 3 {
		id := uuid.New()
		expEntities[i] = orbital.Entity{
			ID:        id,
			Name:      "job_cursor",
			CreatedAt: utcTime,
			UpdatedAt: utcTime,
			Values: map[string]any{
				"id":         []uint8(id.String()),
				"cursor":     "cursor",
				"created_at": utcTime,
				"updated_at": utcTime,
			},
		}
	}
	// mirrors the order of the entities when queried
	sort.Slice(expEntities, func(i, j int) bool {
		return expEntities[i].ID.String() < expEntities[j].ID.String()
	})

	ctx := t.Context()
	db, store := createSQLStore(t)
	defer clearTables(t, db)
	_, err := store.Create(ctx, expEntities...)
	assert.NoError(t, err)

	t.Run("should return all entities", func(t *testing.T) {
		// given
		ctx := t.Context()
		q := query.Query{
			EntityName: query.EntityNameJobCursor,
		}

		// when
		result, err := store.List(ctx, q)

		// then
		assert.NoError(t, err)
		assert.True(t, result.Exists)
		assert.Len(t, result.Entities, len(expEntities))
		assert.Equal(t, expEntities, result.Entities)
	})

	cursor := query.Cursor{}
	t.Run("should return cursor", func(t *testing.T) {
		// given
		ctx := t.Context()
		q := query.Query{EntityName: query.EntityNameJobCursor, Limit: 1}
		assert.LessOrEqual(t, q.Limit, len(expEntities))

		// when
		result, err := store.List(ctx, q)

		// then
		assert.NoError(t, err)
		assert.True(t, result.Exists)
		assert.Len(t, result.Entities, 1)
		assert.Equal(t, expEntities[:1], result.Entities)
		assert.Equal(t, expEntities[0].CreatedAt, result.Cursor.Timestamp)
		assert.Equal(t, expEntities[0].ID, result.Cursor.ID)

		cursor = result.Cursor
	})

	t.Run("should query with cursor", func(t *testing.T) {
		// given
		ctx := t.Context()
		q := query.Query{EntityName: query.EntityNameJobCursor, Cursor: cursor}

		// when
		result, err := store.List(ctx, q)

		// then
		assert.NoError(t, err)
		assert.True(t, result.Exists)
		assert.Len(t, result.Entities, 2)
		assert.Equal(t, expEntities[1:], result.Entities)
		assert.Equal(t, int64(0), result.Cursor.Timestamp)
		assert.Equal(t, uuid.Nil, result.Cursor.ID)
	})

	t.Run("should return entities ordered by created_at", func(t *testing.T) {
		// given
		ctx := t.Context()
		q := query.Query{
			EntityName: query.EntityNameJobCursor,
		}
		q.OrderBy = []query.OrderBy{
			{
				Field:       "created_at",
				IsAscending: false,
			},
		}
		q.Limit = 1

		// when
		result, err := store.List(ctx, q)

		sort.Slice(expEntities, func(i, j int) bool {
			return expEntities[i].CreatedAt < expEntities[j].CreatedAt
		})
		// then
		assert.NoError(t, err)
		assert.True(t, result.Exists)
		assert.Len(t, result.Entities, 1)
		assert.Equal(t, expEntities[0], result.Entities[0])
	})

	t.Run("should return entities using IN clause", func(t *testing.T) {
		// given
		ctx := t.Context()
		q := query.Query{
			EntityName: query.EntityNameJobCursor,
		}
		q.Clauses = []query.Clause{
			{
				Operator: "IN",
				Field:    "id",
				Value: []any{
					expEntities[0].ID.String(),
					expEntities[1].ID.String(),
					expEntities[2].ID.String(),
				},
			},
		}

		// when
		result, err := store.List(ctx, q)

		sort.Slice(expEntities, func(i, j int) bool {
			return expEntities[i].CreatedAt < expEntities[j].CreatedAt
		})
		// then
		assert.NoError(t, err)
		assert.True(t, result.Exists)
		assert.Len(t, result.Entities, len(expEntities))
		assert.Equal(t, expEntities, result.Entities)
	})
}

type callParams struct {
	isTransactional bool
	queryToExecute  query.Query
}

func TestWithRetrievalModeQueue(t *testing.T) {
	// given
	utcTime := utcUnix()
	records := make([]orbital.Entity, 0, 10)
	for i := range 10 {
		id := uuid.New()
		utcTime++
		entity := orbital.Entity{
			Name:      "job_cursor",
			ID:        id,
			CreatedAt: utcTime,
			UpdatedAt: utcTime,
			Values: map[string]any{
				"id":         []uint8(id.String()),
				"cursor":     fmt.Sprintf("cursor-%v", i),
				"created_at": utcTime,
				"updated_at": utcTime,
			},
		}
		records = append(records, entity)
	}
	ctx := t.Context()
	db, store := createSQLStore(t)
	defer clearTables(t, db)
	_, err := store.Create(ctx, records...)
	assert.NoError(t, err)

	t.Run("should do  `FOR UPDATE SKIP LOCKED` for", func(t *testing.T) {
		// given
		tts := []struct {
			firstCallParams        callParams
			secondCallParams       callParams
			expEntitysInFirstCall  []orbital.Entity
			expEntitysInSecondCall []orbital.Entity
		}{
			{
				firstCallParams: callParams{
					queryToExecute: query.Query{
						RetrievalModeQueue: true,
						Limit:              3,
					},
				},
				secondCallParams: callParams{
					isTransactional: true,
					queryToExecute: query.Query{
						RetrievalModeQueue: true,
						Limit:              5,
					},
				},
				expEntitysInFirstCall:  records[:3],
				expEntitysInSecondCall: records[3:8],
			},
			{
				firstCallParams: callParams{
					queryToExecute: query.Query{RetrievalModeQueue: true, Limit: 5},
				},
				secondCallParams: callParams{
					isTransactional: true,
					queryToExecute:  query.Query{RetrievalModeQueue: true, Limit: 1},
				},
				expEntitysInFirstCall:  records[:5],
				expEntitysInSecondCall: records[5:6],
			},
			{
				firstCallParams: callParams{
					queryToExecute: query.Query{RetrievalModeQueue: true, Limit: 1},
				},
				secondCallParams: callParams{
					isTransactional: true,
					queryToExecute:  query.Query{RetrievalModeQueue: true, Limit: 100},
				},
				expEntitysInFirstCall:  records[:1],
				expEntitysInSecondCall: records[1:],
			},
			{
				firstCallParams: callParams{
					queryToExecute: query.Query{RetrievalModeQueue: true, Limit: 100},
				},
				secondCallParams: callParams{
					isTransactional: true,
					queryToExecute:  query.Query{RetrievalModeQueue: true, Limit: 10},
				},
				expEntitysInFirstCall:  records[:10],
				expEntitysInSecondCall: nil,
			},
			{
				firstCallParams: callParams{
					queryToExecute: query.Query{RetrievalModeQueue: true, Limit: 3},
				},
				secondCallParams: callParams{
					isTransactional: false,
					queryToExecute:  query.Query{RetrievalModeQueue: true, Limit: 5},
				},
				expEntitysInFirstCall:  records[:3],
				expEntitysInSecondCall: records[3:8],
			},
			{
				firstCallParams: callParams{
					queryToExecute: query.Query{RetrievalModeQueue: true, Limit: 5},
				},
				secondCallParams: callParams{
					isTransactional: false,
					queryToExecute:  query.Query{RetrievalModeQueue: true, Limit: 1},
				},
				expEntitysInFirstCall:  records[:5],
				expEntitysInSecondCall: records[5:6],
			},
			{
				firstCallParams: callParams{
					queryToExecute: query.Query{RetrievalModeQueue: true, Limit: 1},
				},
				secondCallParams: callParams{
					isTransactional: false,
					queryToExecute:  query.Query{RetrievalModeQueue: true, Limit: 100},
				},
				expEntitysInFirstCall:  records[:1],
				expEntitysInSecondCall: records[1:],
			},
			{
				firstCallParams: callParams{
					queryToExecute: query.Query{RetrievalModeQueue: true, Limit: 100},
				},
				secondCallParams: callParams{
					isTransactional: false,
					queryToExecute:  query.Query{RetrievalModeQueue: true, Limit: 100},
				},
				expEntitysInFirstCall:  records[:10],
				expEntitysInSecondCall: nil,
			},
			{
				firstCallParams: callParams{
					queryToExecute: query.Query{RetrievalModeQueue: true, Limit: 3},
				},
				secondCallParams: callParams{
					isTransactional: true,
					queryToExecute:  query.Query{Limit: 5},
				},
				expEntitysInFirstCall:  records[:3],
				expEntitysInSecondCall: records[:5],
			},
			{
				firstCallParams: callParams{
					queryToExecute: query.Query{RetrievalModeQueue: true, Limit: 5},
				},
				secondCallParams: callParams{
					isTransactional: true,
					queryToExecute:  query.Query{Limit: 1},
				},
				expEntitysInFirstCall:  records[:5],
				expEntitysInSecondCall: records[:1],
			},
			{
				firstCallParams: callParams{
					queryToExecute: query.Query{RetrievalModeQueue: true, Limit: 1},
				},
				secondCallParams: callParams{
					isTransactional: true,
					queryToExecute:  query.Query{Limit: 100},
				},
				expEntitysInFirstCall:  records[:1],
				expEntitysInSecondCall: records[:10],
			},
			{
				firstCallParams: callParams{
					queryToExecute: query.Query{RetrievalModeQueue: true, Limit: 100},
				},
				secondCallParams: callParams{
					isTransactional: true,
					queryToExecute:  query.Query{Limit: 10},
				},
				expEntitysInFirstCall:  records[:10],
				expEntitysInSecondCall: records[:10],
			},
			{
				firstCallParams: callParams{
					queryToExecute: query.Query{RetrievalModeQueue: true, Limit: 3},
				},
				secondCallParams: callParams{
					isTransactional: false,
					queryToExecute:  query.Query{Limit: 5},
				},
				expEntitysInFirstCall:  records[:3],
				expEntitysInSecondCall: records[:5],
			},
			{
				firstCallParams: callParams{
					queryToExecute: query.Query{RetrievalModeQueue: true, Limit: 5},
				},
				secondCallParams: callParams{
					isTransactional: false,
					queryToExecute:  query.Query{Limit: 1},
				},
				expEntitysInFirstCall:  records[:5],
				expEntitysInSecondCall: records[:1],
			},
			{
				firstCallParams: callParams{
					queryToExecute: query.Query{RetrievalModeQueue: true, Limit: 1},
				},
				secondCallParams: callParams{
					isTransactional: false,
					queryToExecute:  query.Query{Limit: 100},
				},
				expEntitysInFirstCall:  records[:1],
				expEntitysInSecondCall: records[:10],
			},
			{
				firstCallParams: callParams{
					queryToExecute: query.Query{RetrievalModeQueue: true, Limit: 100},
				},
				secondCallParams: callParams{
					isTransactional: false,
					queryToExecute:  query.Query{Limit: 100},
				},
				expEntitysInFirstCall:  records[:10],
				expEntitysInSecondCall: records[:10],
			},
		}

		for _, tt := range tts {
			firstCallQuery := tt.firstCallParams.queryToExecute
			secondCallQuery := tt.secondCallParams.queryToExecute
			firstCallQuery.EntityName = query.EntityNameJobCursor
			secondCallQuery.EntityName = query.EntityNameJobCursor

			testName := fmt.Sprintf("1stCall[limit:%v,isQueue:%v] 2ndCall[isTrans:%v,limit:%v,isQueue:%v]",
				firstCallQuery.Limit,
				firstCallQuery.RetrievalModeQueue,
				tt.secondCallParams.isTransactional,
				secondCallQuery.Limit,
				secondCallQuery.RetrievalModeQueue)

			t.Run(testName, func(t *testing.T) {
				transactor1 := make(chan string)
				defer close(transactor1)

				transactor2 := make(chan string)
				defer close(transactor2)

				actEntitysInFirstCall := []orbital.Entity{}
				actEntityInSecondCall := []orbital.Entity{}

				wg := sync.WaitGroup{}
				wg.Add(2)
				// when

				// 1st call
				go func() {
					defer wg.Done()
					assert.Equal(t, "transaction 1 start", <-transactor1)
					err := store.Transaction(ctx, func(ctx context.Context, r orbital.Repository) error {
						listResult, err := r.Store.List(ctx, firstCallQuery)
						assert.NoError(t, err)
						actEntitysInFirstCall = listResult.Entities
						transactor1 <- "transaction 1 wait"
						assert.Equal(t, "transaction 1 continue", <-transactor1)
						return nil
					})
					assert.NoError(t, err)
				}()

				// 2nd call
				go func() {
					defer wg.Done()
					assert.Equal(t, "transaction 2 start", <-transactor2)
					if tt.secondCallParams.isTransactional {
						err := store.Transaction(ctx, func(ctx context.Context, r orbital.Repository) error {
							listResult, err := r.Store.List(ctx, secondCallQuery)
							assert.NoError(t, err)
							actEntityInSecondCall = listResult.Entities
							transactor1 <- "transaction 1 continue"
							return nil
						})
						assert.NoError(t, err)
					} else {
						listResult, err := store.List(ctx, secondCallQuery)
						assert.NoError(t, err)
						actEntityInSecondCall = listResult.Entities
						transactor1 <- "transaction 1 continue"
					}
				}()

				transactor1 <- "transaction 1 start"
				assert.Equal(t, "transaction 1 wait", <-transactor1)
				transactor2 <- "transaction 2 start"
				wg.Wait()

				// then
				assert.Equal(t, tt.expEntitysInFirstCall, actEntitysInFirstCall)
				assert.Equal(t, tt.expEntitysInSecondCall, actEntityInSecondCall)
			})
		}
	})
}

func TestTransaction(t *testing.T) {
	// given
	id := uuid.New()
	utcTime := utcUnix()
	entity := orbital.Entity{
		Name:      "job_cursor",
		ID:        id,
		CreatedAt: utcTime,
		UpdatedAt: utcTime,
		Values: map[string]any{
			"id":         []uint8(id.String()),
			"cursor":     "cursor",
			"created_at": utcTime,
			"updated_at": utcTime,
		},
	}

	t.Run("should rollback if transaction returns error", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)

		// when
		err := store.Transaction(ctx, func(ctx context.Context, r orbital.Repository) error {
			_, err := r.Store.Create(ctx, entity)

			assert.NoError(t, err)
			return errRandom
		})

		// then
		assert.Error(t, err)
		result, err := store.Find(ctx, query.Query{
			EntityName: query.EntityNameJobCursor,
		})
		assert.NoError(t, err)
		assert.False(t, result.Exists)
	})

	t.Run("should commit if transaction returns nil", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)

		// when
		err := store.Transaction(ctx, func(ctx context.Context, r orbital.Repository) error {
			_, err := r.Store.Create(ctx, entity)

			assert.NoError(t, err)
			return nil
		})

		// then
		assert.NoError(t, err)
		result, err := store.Find(ctx, query.Query{
			EntityName: query.EntityNameJobCursor,
		})
		assert.NoError(t, err)
		assert.True(t, result.Exists)
		assert.Equal(t, entity, result.Entity)
	})
}

func validateColumnsAndEntity[T orbital.EntityTypes](t *testing.T, db *stdsql.DB, entity T) {
	t.Helper()
	ctx := t.Context()
	// checking if it can encoded
	encodedEntity, err := orbital.Encode(entity)
	assert.NoError(t, err)

	// fetching columns from database
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s WHERE false", encodedEntity.Name))
	assert.NoError(t, err)
	assert.NoError(t, rows.Err())
	defer rows.Close()

	// fetching columns from the result set
	columns, err := rows.Columns()
	assert.NoError(t, err)

	// getting the visible fields of the entity
	fields := reflect.VisibleFields(reflect.TypeOf(entity))
	assert.Len(t, fields, len(encodedEntity.Values))
	assert.Len(t, columns, len(encodedEntity.Values))

	// checking if the columns are present in the entity values
	for _, column := range columns {
		_, ok := encodedEntity.Values[column]
		assert.True(t, ok, "column %s not found in entity values", column)
	}
}

func utcUnix() int64 {
	return time.Now().UTC().Unix()
}
