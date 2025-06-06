package sql

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/store/query"
)

// baseQueryBuilder provides common functionality for all query builders.
type baseQueryBuilder struct {
	params     []any
	paramIndex int
}

// addParam adds a parameter to the list and returns its placeholder.
func (b *baseQueryBuilder) addParam(value any) string {
	b.params = append(b.params, value)
	placeholder := "$" + strconv.Itoa(b.paramIndex)
	b.paramIndex++
	return placeholder
}

// selectQueryBuilder builds SELECT queries by embedding baseQueryBuilder.
type selectQueryBuilder struct {
	baseQueryBuilder
}

// newSelectQueryBuilder creates a new SELECT query builder.
func newSelectQueryBuilder() *selectQueryBuilder {
	return &selectQueryBuilder{
		baseQueryBuilder: baseQueryBuilder{
			params:     []any{},
			paramIndex: 1,
		},
	}
}

// build constructs the complete SELECT query.
func (sqb *selectQueryBuilder) build(q query.Query) (string, []any) {
	parts := []string{
		"SELECT * FROM " + string(q.EntityName),
		sqb.buildWhere(q),
		sqb.buildOrderBy(q),
		sqb.buildLocking(q),
		sqb.buildLimit(q),
	}

	return strings.Join(slices.Collect(func(yield func(str string) bool) {
		for _, part := range parts {
			if part != "" {
				if !yield(part) {
					return
				}
			}
		}
	}), " "), sqb.params
}

// buildWhere constructs the WHERE clause including regular clauses and cursor.
func (sqb *selectQueryBuilder) buildWhere(q query.Query) string {
	var conditions []string

	for _, clause := range q.Clauses {
		if cond := sqb.buildClauseCondition(clause); cond != "" {
			conditions = append(conditions, cond)
		}
	}

	if cursorCond := sqb.buildCursorCondition(q.Cursor); cursorCond != "" {
		conditions = append(conditions, cursorCond)
	}

	if len(conditions) == 0 {
		return ""
	}

	return "WHERE " + strings.Join(conditions, " AND ")
}

// buildClauseCondition builds a single clause condition with proper operator handling.
func (sqb *selectQueryBuilder) buildClauseCondition(clause query.Clause) string {
	switch clause.Operator {
	case "IN":
		return sqb.buildInCondition(clause)
	default:
		return sqb.buildSimpleCondition(clause)
	}
}

// buildSimpleCondition builds conditions for simple operators (=, <=, etc.)
func (sqb *selectQueryBuilder) buildSimpleCondition(clause query.Clause) string {
	placeholder := sqb.addParam(clause.Value)
	return fmt.Sprintf("%s %s %s", clause.Field, clause.Operator, placeholder)
}

// buildInCondition builds the IN operator condition.
func (sqb *selectQueryBuilder) buildInCondition(clause query.Clause) string {
	values, ok := clause.Value.([]string)
	if !ok || len(values) == 0 {
		return ""
	}

	placeholders := make([]string, len(values))
	for i, val := range values {
		placeholders[i] = sqb.addParam(val)
	}

	return fmt.Sprintf("%s IN (%s)", clause.Field, strings.Join(placeholders, ", "))
}

// buildCursorCondition builds the cursor-based pagination condition.
func (sqb *selectQueryBuilder) buildCursorCondition(cursor query.Cursor) string {
	if cursor.Timestamp <= 0 || cursor.ID == uuid.Nil {
		return ""
	}

	timestampPlaceholder := sqb.addParam(cursor.Timestamp)
	idPlaceholder := sqb.addParam(cursor.ID)

	return fmt.Sprintf("(created_at > %s OR (created_at = %s AND id > %s))",
		timestampPlaceholder,
		timestampPlaceholder,
		idPlaceholder)
}

// buildOrderBy constructs the ORDER BY clause.
func (sqb *selectQueryBuilder) buildOrderBy(q query.Query) string {
	if len(q.OrderBy) == 0 {
		return "ORDER BY created_at ASC, id ASC"
	}

	orderParts := make([]string, len(q.OrderBy))
	for i, ob := range q.OrderBy {
		direction := "DESC"
		if ob.IsAscending {
			direction = "ASC"
		}
		orderParts[i] = fmt.Sprintf("%s %s", ob.Field, direction)
	}

	return "ORDER BY " + strings.Join(orderParts, ", ")
}

// buildLocking adds row locking clause if needed.
func (sqb *selectQueryBuilder) buildLocking(q query.Query) string {
	if q.RetrievalModeQueue {
		return "FOR UPDATE SKIP LOCKED"
	}
	return ""
}

// buildLimit adds the LIMIT clause if specified.
func (sqb *selectQueryBuilder) buildLimit(q query.Query) string {
	if q.Limit > 0 {
		return "LIMIT " + sqb.addParam(q.Limit)
	}
	return ""
}

// insertQueryBuilder builds INSERT queries by embedding baseQueryBuilder.
type insertQueryBuilder struct {
	baseQueryBuilder
}

// newInsertQueryBuilder creates a new INSERT query builder.
func newInsertQueryBuilder() *insertQueryBuilder {
	return &insertQueryBuilder{
		baseQueryBuilder: baseQueryBuilder{
			params:     []any{},
			paramIndex: 1,
		},
	}
}

// build constructs the complete INSERT query.
func (iqb *insertQueryBuilder) build(entities ...orbital.Entity) (string, []any, error) {
	if len(entities) == 0 {
		return "", nil, ErrNoEntity
	}

	iqb.initializeEntities(entities)

	columns, err := getSortedColumns(entities[0])
	if err != nil {
		return "", nil, err
	}

	parts := []string{
		iqb.buildInsertClause(entities[0].Name, columns),
		iqb.buildValuesClause(entities, columns),
	}

	return strings.Join(parts, " "), iqb.params, nil
}

// initializeEntities sets default values for all entities.
func (iqb *insertQueryBuilder) initializeEntities(entities []orbital.Entity) {
	for i := range entities {
		orbital.Init(&entities[i])
	}
}

// buildInsertClause builds the INSERT INTO table (columns) part.
func (iqb *insertQueryBuilder) buildInsertClause(tableName query.EntityName, columns []string) string {
	return fmt.Sprintf("INSERT INTO %s (%s)", tableName, strings.Join(columns, ", "))
}

// buildValuesClause builds the VALUES clause for all entities.
func (iqb *insertQueryBuilder) buildValuesClause(entities []orbital.Entity, columns []string) string {
	valueSets := make([]string, len(entities))

	for i, entity := range entities {
		placeholders := make([]string, len(columns))
		for j, col := range columns {
			value := entity.Values[col]
			placeholders[j] = iqb.addParam(value)
		}
		valueSets[i] = fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))
	}

	return "VALUES " + strings.Join(valueSets, ", ")
}

// updateQueryBuilder builds UPDATE queries by embedding baseQueryBuilder.
type updateQueryBuilder struct {
	baseQueryBuilder
}

// newUpdateQueryBuilder creates a new UPDATE query builder.
func newUpdateQueryBuilder() *updateQueryBuilder {
	return &updateQueryBuilder{
		baseQueryBuilder: baseQueryBuilder{
			params:     []any{},
			paramIndex: 1,
		},
	}
}

// build constructs UPDATE queries for multiple entities.
func (uqb *updateQueryBuilder) build(entities ...orbital.Entity) (string, []any) {
	if len(entities) == 0 {
		return "", nil
	}

	statements := make([]string, len(entities))

	for i, entity := range entities {
		entity.UpdatedAt = 0
		orbital.Init(&entity)
		entities[i] = entity

		statements[i] = uqb.buildSingleUpdate(entity)
	}

	return strings.Join(statements, "; "), uqb.params
}

// buildSingleUpdate builds a single UPDATE statement.
func (uqb *updateQueryBuilder) buildSingleUpdate(entity orbital.Entity) string {
	parts := []string{
		fmt.Sprintf("UPDATE %s", entity.Name),
		uqb.buildSetClause(entity),
		uqb.buildWhereClause(entity),
	}

	return strings.Join(parts, " ")
}

// buildSetClause builds the SET clause.
func (uqb *updateQueryBuilder) buildSetClause(entity orbital.Entity) string {
	columns := getUpdateColumns(entity)
	setClauses := make([]string, len(columns))

	for i, col := range columns {
		value := entity.Values[col]
		setClauses[i] = fmt.Sprintf("%s = %s", col, uqb.addParam(value))
	}

	return "SET " + strings.Join(setClauses, ", ")
}

// buildWhereClause builds WHERE clause using ID.
func (uqb *updateQueryBuilder) buildWhereClause(entity orbital.Entity) string {
	id := entity.Values["id"]
	return "WHERE id = " + uqb.addParam(id)
}

// getSortedColumns returns sorted column names from an entity.
func getSortedColumns(entity orbital.Entity) ([]string, error) {
	if len(entity.Values) == 0 {
		return nil, ErrNoColumn
	}

	columns := make([]string, 0, len(entity.Values))
	for col := range entity.Values {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	return columns, nil
}

// getUpdateColumns returns sorted columns excluding ID.
func getUpdateColumns(entity orbital.Entity) []string {
	var columns []string

	for col := range entity.Values {
		if col != "id" {
			columns = append(columns, col)
		}
	}

	sort.Strings(columns)
	return columns
}
