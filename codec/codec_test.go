package codec_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
)

var expTaskRequest = orbital.TaskRequest{
	TaskID:       uuid.New(),
	Type:         "test",
	Data:         []byte("test data"),
	WorkingState: []byte("prev state"),
	ETag:         "etag",
}

var expTaskResponse = orbital.TaskResponse{
	TaskID:            uuid.New(),
	Type:              "test",
	Status:            string(orbital.ResultDone),
	WorkingState:      []byte("after state"),
	ETag:              "etag",
	ReconcileAfterSec: 10,
	ErrorMessage:      "no error",
}

func assertTaskRequest(t *testing.T, actTaskRequest orbital.TaskRequest) {
	t.Helper()
	assert.Equal(t, expTaskRequest.TaskID, actTaskRequest.TaskID)
	assert.Equal(t, expTaskRequest.Type, actTaskRequest.Type)
	assert.Equal(t, expTaskRequest.Data, actTaskRequest.Data)
	assert.Equal(t, expTaskRequest.WorkingState, actTaskRequest.WorkingState)
	assert.Equal(t, expTaskRequest.ETag, actTaskRequest.ETag)
}

func assertTaskResponse(t *testing.T, actTaskResponse orbital.TaskResponse) {
	t.Helper()
	assert.Equal(t, expTaskResponse.TaskID, actTaskResponse.TaskID)
	assert.Equal(t, expTaskResponse.Type, actTaskResponse.Type)
	assert.Equal(t, expTaskResponse.Status, actTaskResponse.Status)
	assert.Equal(t, expTaskResponse.WorkingState, actTaskResponse.WorkingState)
	assert.Equal(t, expTaskResponse.ETag, actTaskResponse.ETag)
	assert.Equal(t, expTaskResponse.ErrorMessage, actTaskResponse.ErrorMessage)
	assert.Equal(t, expTaskResponse.ReconcileAfterSec, actTaskResponse.ReconcileAfterSec)
}
