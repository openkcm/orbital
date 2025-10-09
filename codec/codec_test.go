package codec_test

import (
	"github.com/google/uuid"

	"github.com/openkcm/orbital"
)

var expTaskRequest = orbital.TaskRequest{
	TaskID:       uuid.New(),
	Type:         "test",
	ExternalID:   "external-id",
	Data:         []byte("test data"),
	WorkingState: []byte("prev state"),
	ETag:         "etag",
	MetaData:     orbital.MetaData{"key1": "value1", "key2": "value"},
}

var expTaskRequestWithoutMeta = orbital.TaskRequest{
	TaskID:       uuid.New(),
	Type:         "test",
	ExternalID:   "external-id",
	Data:         []byte("test data"),
	WorkingState: []byte("prev state"),
	ETag:         "etag",
	MetaData:     nil,
}

var expTaskResponse = orbital.TaskResponse{
	TaskID:            uuid.New(),
	Type:              "test",
	ExternalID:        "external-id",
	Status:            string(orbital.ResultDone),
	WorkingState:      []byte("after state"),
	ETag:              "etag",
	ReconcileAfterSec: 10,
	ErrorMessage:      "no error",
	MetaData:          orbital.MetaData{"key1": "value1", "key2": "value2"},
}

var expTaskResponseWithoutMeta = orbital.TaskResponse{
	TaskID:            uuid.New(),
	Type:              "test",
	ExternalID:        "external-id",
	Status:            string(orbital.ResultDone),
	WorkingState:      []byte("after state"),
	ETag:              "etag",
	ReconcileAfterSec: 10,
	ErrorMessage:      "no error",
	MetaData:          nil,
}
