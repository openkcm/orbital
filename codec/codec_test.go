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
	MetaData: orbital.MetaData{
		Signature: orbital.Signature{
			Value: "value",
			Type:  "type",
		},
		IsEncrypted: true,
	},
}

var expTaskRequestWithoutSig = orbital.TaskRequest{
	TaskID:       uuid.New(),
	Type:         "test",
	ExternalID:   "external-id",
	Data:         []byte("test data"),
	WorkingState: []byte("prev state"),
	ETag:         "etag",
	MetaData: orbital.MetaData{
		IsEncrypted: true,
	},
}

var expTaskRequestWithoutType = orbital.TaskRequest{
	TaskID:       uuid.New(),
	Type:         "test",
	ExternalID:   "external-id",
	Data:         []byte("test data"),
	WorkingState: []byte("prev state"),
	ETag:         "etag",
	MetaData: orbital.MetaData{
		Signature: orbital.Signature{
			Value: "value",
		},
		IsEncrypted: true,
	},
}

var expTaskRequestWithoutEncrypt = orbital.TaskRequest{
	TaskID:       uuid.New(),
	Type:         "test",
	ExternalID:   "external-id",
	Data:         []byte("test data"),
	WorkingState: []byte("prev state"),
	ETag:         "etag",
	MetaData: orbital.MetaData{
		Signature: orbital.Signature{
			Value: "value",
			Type:  "type",
		},
	},
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
	MetaData: orbital.MetaData{
		Signature: orbital.Signature{
			Value: "value",
			Type:  "type",
		},
		IsEncrypted: true,
	},
}

var expTaskResponseWithoutSig = orbital.TaskResponse{
	TaskID:            uuid.New(),
	Type:              "test",
	ExternalID:        "external-id",
	Status:            string(orbital.ResultDone),
	WorkingState:      []byte("after state"),
	ETag:              "etag",
	ReconcileAfterSec: 10,
	ErrorMessage:      "no error",
	MetaData: orbital.MetaData{
		IsEncrypted: false,
	},
}

var expTaskResponseWithoutEncrypt = orbital.TaskResponse{
	TaskID:            uuid.New(),
	Type:              "test",
	ExternalID:        "external-id",
	Status:            string(orbital.ResultDone),
	WorkingState:      []byte("after state"),
	ETag:              "etag",
	ReconcileAfterSec: 10,
	ErrorMessage:      "no error",
	MetaData: orbital.MetaData{
		Signature: orbital.Signature{
			Value: "value",
			Type:  "type",
		},
	},
}

var expTaskResponseWithoutType = orbital.TaskResponse{
	TaskID:            uuid.New(),
	Type:              "test",
	ExternalID:        "external-id",
	Status:            string(orbital.ResultDone),
	WorkingState:      []byte("after state"),
	ETag:              "etag",
	ReconcileAfterSec: 10,
	ErrorMessage:      "no error",
	MetaData: orbital.MetaData{
		Signature: orbital.Signature{
			Value: "value",
		},
		IsEncrypted: true,
	},
}
