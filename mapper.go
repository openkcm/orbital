package orbital

import (
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/openkcm/orbital/internal/clock"
	"github.com/openkcm/orbital/store/query"
)

var (
	ErrInvalidEntityType = errors.New("invalid entity type")
	ErrMandatoryFields   = errors.New("mandatory fields")
)

// TransformToEntities transforms a list of maps into domain entities based on the provided entity name.
func TransformToEntities(entityName query.EntityName, objs ...map[string]any) ([]Entity, error) {
	switch entityName {
	case query.EntityNameJobs, query.EntityNameTasks, query.EntityNameJobCursor, query.EntityNameJobEvent:
		result := make([]Entity, 0, len(objs))
		for _, r := range objs {
			entity, err := TransformToEntity(entityName, r)
			if err != nil {
				return nil, err
			}
			result = append(result, entity)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("%w `%s` not supported", ErrInvalidEntityType, entityName)
	}
}

// TransformToEntity transforms a map into a domain entity based on the provided entity name.
func TransformToEntity(entityName query.EntityName, objs map[string]any) (Entity, error) {
	result := Entity{}

	id, err := resolveUUID(objs, "id")
	if err != nil {
		return result, err
	}
	result.ID = id

	keys := []string{"updated_at", "created_at"}
	for _, key := range keys {
		value, err := resolve[int64](objs, key)
		if err != nil {
			return result, err
		}
		switch key {
		case "updated_at":
			result.UpdatedAt = value
		case "created_at":
			result.CreatedAt = value
		}
	}

	result.Name = entityName
	result.Values = objs
	return result, nil
}

// Init ensures that the metadata of the entity is properly initialized.
// It sets default values for CreatedAt, UpdatedAt, ID and ExternalID if they are not already set.
func Init(e *Entity) {
	now := clock.NowUnixNano()

	if e.Values == nil {
		e.Values = make(map[string]any)
	}
	if e.CreatedAt == 0 {
		e.CreatedAt = now
		e.Values["created_at"] = now
	}
	if e.UpdatedAt == 0 {
		e.UpdatedAt = now
		e.Values["updated_at"] = now
	}
	if e.ID == uuid.Nil {
		e.ID = uuid.New()
		e.Values["id"] = e.ID
	}

	if e.Name != query.EntityNameJobs {
		return
	}
	// Set external_id to the job's ID if not already provided.
	extID, ok := e.Values["external_id"]
	if !ok || extID == nil || extID == "" {
		e.Values["external_id"] = e.ID.String()
	}
}

// Encodes converts a list of domain objects to store entities.
func Encodes[T EntityTypes](entityTypes ...T) ([]Entity, error) {
	result := make([]Entity, 0, len(entityTypes))
	for _, entityType := range entityTypes {
		entity, err := Encode(entityType)
		if err != nil {
			return nil, err
		}
		result = append(result, entity)
	}
	return result, nil
}

// Encode converts a domain object to a store entity.
//
//nolint:funlen
func Encode[T EntityTypes](entityType T) (Entity, error) {
	switch obj := any(entityType).(type) {
	case Job:
		return Entity{
			Name:      query.EntityNameJobs,
			ID:        obj.ID,
			UpdatedAt: obj.UpdatedAt,
			CreatedAt: obj.CreatedAt,
			Values: map[string]any{
				"id":            obj.ID,
				"external_id":   obj.ExternalID,
				"data":          obj.Data,
				"type":          obj.Type,
				"status":        obj.Status,
				"error_message": obj.ErrorMessage,
				"updated_at":    obj.UpdatedAt,
				"created_at":    obj.CreatedAt,
			},
		}, nil
	case Task:
		return Entity{
			Name:      query.EntityNameTasks,
			ID:        obj.ID,
			UpdatedAt: obj.UpdatedAt,
			CreatedAt: obj.CreatedAt,
			Values: map[string]any{
				"id":                   obj.ID,
				"job_id":               obj.JobID,
				"type":                 obj.Type,
				"data":                 obj.Data,
				"working_state":        obj.WorkingState,
				"last_reconciled_at":   obj.LastReconciledAt,
				"reconcile_count":      obj.ReconcileCount,
				"total_sent_count":     obj.TotalSentCount,
				"total_received_count": obj.TotalReceivedCount,
				"reconcile_after_sec":  obj.ReconcileAfterSec,
				"etag":                 obj.ETag,
				"status":               obj.Status,
				"target":               obj.Target,
				"error_message":        obj.ErrorMessage,
				"updated_at":           obj.UpdatedAt,
				"created_at":           obj.CreatedAt,
			},
		}, nil
	case JobCursor:
		return Entity{
			Name:      query.EntityNameJobCursor,
			ID:        obj.ID,
			UpdatedAt: obj.UpdatedAt,
			CreatedAt: obj.CreatedAt,
			Values: map[string]any{
				"id":         obj.ID,
				"cursor":     obj.Cursor,
				"updated_at": obj.UpdatedAt,
				"created_at": obj.CreatedAt,
			},
		}, nil
	case JobEvent:
		return Entity{
			Name:      query.EntityNameJobEvent,
			ID:        obj.ID,
			CreatedAt: obj.CreatedAt,
			UpdatedAt: obj.UpdatedAt,
			Values: map[string]any{
				"id":          obj.ID,
				"is_notified": obj.IsNotified,
				"updated_at":  obj.UpdatedAt,
				"created_at":  obj.CreatedAt,
			},
		}, nil
	default:
		return Entity{}, fmt.Errorf("%w `%T` not supported", ErrInvalidEntityType, obj)
	}
}

// Decodes converts a list of store entities to domain objects.
func Decodes[T EntityTypes](entities ...Entity) ([]T, error) {
	result := make([]T, 0, len(entities))
	for _, entity := range entities {
		out, err := Decode[T](entity)
		if err != nil {
			return nil, err
		}
		result = append(result, out)
	}
	return result, nil
}

// Decode converts a store entity to a domain object.
func Decode[T EntityTypes](entity Entity) (T, error) {
	var out T
	switch typ := any(out).(type) {
	case Job:
		return decodeJob[T](entity)
	case Task:
		return decodeTask[T](entity)
	case JobCursor:
		return decodeJobCursor[T](entity)
	case JobEvent:
		return decodeJobEvent[T](entity)
	default:
		return out, fmt.Errorf("%w `%T` not supported", ErrInvalidEntityType, typ)
	}
}

func decodeJobEvent[T EntityTypes](entity Entity) (T, error) {
	var out, empty T
	j, ok := any(&out).(*JobEvent)
	if !ok {
		return empty, fmt.Errorf("%w `%T` not supported", ErrInvalidEntityType, j)
	}
	j.ID = entity.ID
	j.UpdatedAt = entity.UpdatedAt
	j.CreatedAt = entity.CreatedAt

	values := entity.Values
	isNotified, err := resolve[bool](values, "is_notified")
	if err != nil {
		return empty, err
	}
	j.IsNotified = isNotified
	return out, nil
}

func decodeJobCursor[T EntityTypes](entity Entity) (T, error) {
	var out, empty T
	j, ok := any(&out).(*JobCursor)
	if !ok {
		return empty, fmt.Errorf("%w `%T` not supported", ErrInvalidEntityType, j)
	}
	j.ID = entity.ID
	j.UpdatedAt = entity.UpdatedAt
	j.CreatedAt = entity.CreatedAt

	values := entity.Values
	cursor, err := resolveType[TaskResolverCursor](values, "cursor")
	if err != nil {
		return empty, err
	}
	j.Cursor = cursor
	return out, nil
}

//nolint:cyclop
func decodeTask[T EntityTypes](e Entity) (T, error) {
	var out, empty T
	t, ok := any(&out).(*Task)
	if !ok {
		return empty, fmt.Errorf("%w `%T` not supported", ErrInvalidEntityType, t)
	}
	t.ID, t.CreatedAt, t.UpdatedAt = e.ID, e.CreatedAt, e.UpdatedAt
	vals := e.Values
	var err error
	if t.JobID, err = resolveUUID(vals, "job_id"); err != nil {
		return empty, err
	}
	if t.ETag, err = resolve[string](vals, "etag"); err != nil {
		return empty, err
	}
	if t.Target, err = resolve[string](vals, "target"); err != nil {
		return empty, err
	}
	if t.Status, err = resolveType[TaskStatus](vals, "status"); err != nil {
		return empty, err
	}
	if t.WorkingState, err = resolve[[]byte](vals, "working_state"); err != nil {
		return empty, err
	}
	if t.Type, err = resolve[string](vals, "type"); err != nil {
		return empty, err
	}
	if t.Data, err = resolve[[]byte](vals, "data"); err != nil {
		return empty, err
	}
	if t.LastReconciledAt, err = resolve[int64](vals, "last_reconciled_at"); err != nil {
		return empty, err
	}
	if t.ReconcileCount, err = resolveUint64(vals, "reconcile_count"); err != nil {
		return empty, err
	}
	if t.TotalSentCount, err = resolveUint64(vals, "total_sent_count"); err != nil {
		return empty, err
	}
	if t.TotalReceivedCount, err = resolveUint64(vals, "total_received_count"); err != nil {
		return empty, err
	}
	if t.ReconcileAfterSec, err = resolveUint64(vals, "reconcile_after_sec"); err != nil {
		return empty, err
	}
	if t.ErrorMessage, err = resolve[string](vals, "error_message"); err != nil {
		return empty, err
	}
	return out, nil
}

func decodeJob[T EntityTypes](e Entity) (T, error) {
	var out, empty T
	j, ok := any(&out).(*Job)
	if !ok {
		return empty, fmt.Errorf("%w `%T` not supported", ErrInvalidEntityType, j)
	}
	j.ID, j.CreatedAt, j.UpdatedAt = e.ID, e.CreatedAt, e.UpdatedAt
	vals := e.Values
	var err error
	if j.ExternalID, err = resolve[string](vals, "external_id"); err != nil {
		return empty, err
	}
	if j.Type, err = resolve[string](vals, "type"); err != nil {
		return empty, err
	}
	if j.Status, err = resolveType[JobStatus](vals, "status"); err != nil {
		return empty, err
	}
	if j.Data, err = resolve[[]byte](vals, "data"); err != nil {
		return empty, err
	}
	if j.ErrorMessage, err = resolve[string](vals, "error_message"); err != nil {
		return empty, err
	}
	return out, nil
}

func resolveUUID(maps map[string]any, key string) (uuid.UUID, error) {
	keyVal, ok := maps[key]
	if !ok {
		return uuid.Nil, fmt.Errorf("%w: %s not found", ErrMandatoryFields, key)
	}
	var err error
	uID := uuid.Nil
	switch val := keyVal.(type) {
	case []uint8:
		uID, err = uuid.Parse(string(val))
	case string:
		uID, err = uuid.Parse(val)
	case uuid.UUID:
		uID = val
	default:
		return uID, fmt.Errorf("%w `%s` not supported: (type %T)", ErrInvalidEntityType, key, val)
	}
	if err != nil {
		return uID, fmt.Errorf("%w %s uuid parsing failed: %w", ErrMandatoryFields, key, err)
	}
	return uID, nil
}

func resolve[T any](values map[string]any, key string) (T, error) {
	var empty T
	raw, ok := values[key]
	if !ok {
		return empty, fmt.Errorf("%w '%s' not found", ErrMandatoryFields, key)
	}
	if raw == nil {
		return empty, nil
	}
	v, ok := raw.(T)
	if !ok {
		return empty, fmt.Errorf("%w `%s` not supported: (type %T)", ErrInvalidEntityType, key, raw)
	}
	return v, nil
}

func resolveType[A ~string](values map[string]any, key string) (A, error) {
	var empty A
	raw, ok := values[key]
	if !ok {
		return empty, fmt.Errorf("%w '%s' not found", ErrMandatoryFields, key)
	}
	switch v := raw.(type) {
	case string:
		return A(v), nil
	case A:
		return v, nil
	default:
		return empty, fmt.Errorf("%w `%s` not supported: (type %T)", ErrInvalidEntityType, key, raw)
	}
}

func resolveUint64(values map[string]any, key string) (uint64, error) {
	raw, ok := values[key]
	if !ok {
		return 0, fmt.Errorf("%w '%s' not found", ErrMandatoryFields, key)
	}
	if raw == nil {
		return 0, nil
	}
	switch v := raw.(type) {
	case uint64:
		return v, nil
	case int64:
		if v < 0 {
			return 0, fmt.Errorf("%w '%s' cannot be negative", ErrInvalidEntityType, key)
		}
		return uint64(v), nil
	default:
		return 0, fmt.Errorf("%w '%s' not supported: (type %T)", ErrInvalidEntityType, key, raw)
	}
}
