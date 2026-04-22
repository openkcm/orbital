package codec

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/openkcm/orbital"
	orbitalpb "github.com/openkcm/orbital/proto/orbital/v1"
)

// ErrInvalidTaskID is returned when a protobuf message contains a task ID that cannot be parsed as a valid UUID.
var ErrInvalidTaskID = errors.New("invalid task ID")

// Proto is a codec that encodes and decodes TaskRequest and TaskResponse in Protobuf format.
type Proto struct{}

var _ orbital.Codec = Proto{}

// FromTaskRequestToProto converts a domain TaskRequest to its protobuf representation.
func FromTaskRequestToProto(req orbital.TaskRequest) *orbitalpb.TaskRequest {
	return &orbitalpb.TaskRequest{
		TaskId:               req.TaskID.String(),
		Type:                 req.Type,
		ExternalId:           req.ExternalID,
		WorkingState:         req.WorkingState,
		Etag:                 req.ETag,
		Data:                 req.Data,
		MetaData:             req.MetaData,
		TaskCreatedAt:        req.TaskCreatedAt,
		TaskLastReconciledAt: req.TaskLastReconciledAt,
	}
}

// FromProtoToTaskRequest converts a protobuf TaskRequest to its domain representation.
func FromProtoToTaskRequest(pReq *orbitalpb.TaskRequest) (orbital.TaskRequest, error) {
	id, err := uuid.Parse(pReq.GetTaskId())
	if err != nil {
		return orbital.TaskRequest{}, fmt.Errorf("%w: %w", ErrInvalidTaskID, err)
	}
	return orbital.TaskRequest{
		TaskID:               id,
		Type:                 pReq.GetType(),
		ExternalID:           pReq.GetExternalId(),
		WorkingState:         pReq.GetWorkingState(),
		ETag:                 pReq.GetEtag(),
		Data:                 pReq.GetData(),
		MetaData:             pReq.GetMetaData(),
		TaskCreatedAt:        pReq.GetTaskCreatedAt(),
		TaskLastReconciledAt: pReq.GetTaskLastReconciledAt(),
	}, nil
}

// FromTaskResponseToProto converts a domain TaskResponse to its protobuf representation.
func FromTaskResponseToProto(resp orbital.TaskResponse) *orbitalpb.TaskResponse {
	return &orbitalpb.TaskResponse{
		TaskId:            resp.TaskID.String(),
		Type:              resp.Type,
		ExternalId:        resp.ExternalID,
		WorkingState:      resp.WorkingState,
		Etag:              resp.ETag,
		Status:            orbitalpb.TaskStatus(orbitalpb.TaskStatus_value[resp.Status]),
		ErrorMessage:      &resp.ErrorMessage,
		ReconcileAfterSec: resp.ReconcileAfterSec,
		MetaData:          resp.MetaData,
	}
}

// FromProtoToTaskResponse converts a protobuf TaskResponse to its domain representation.
func FromProtoToTaskResponse(pRes *orbitalpb.TaskResponse) (orbital.TaskResponse, error) {
	id, err := uuid.Parse(pRes.GetTaskId())
	if err != nil {
		return orbital.TaskResponse{}, fmt.Errorf("%w: %w", ErrInvalidTaskID, err)
	}
	return orbital.TaskResponse{
		TaskID:            id,
		Type:              pRes.GetType(),
		ExternalID:        pRes.GetExternalId(),
		WorkingState:      pRes.GetWorkingState(),
		ETag:              pRes.GetEtag(),
		Status:            pRes.GetStatus().String(),
		ErrorMessage:      pRes.GetErrorMessage(),
		ReconcileAfterSec: pRes.GetReconcileAfterSec(),
		MetaData:          pRes.GetMetaData(),
	}, nil
}

// DecodeTaskRequest decodes Protobuf data into a TaskRequest.
func (p Proto) DecodeTaskRequest(bytes []byte) (orbital.TaskRequest, error) {
	pReq := orbitalpb.TaskRequest{}
	if err := proto.Unmarshal(bytes, &pReq); err != nil {
		return orbital.TaskRequest{}, err
	}
	return FromProtoToTaskRequest(&pReq)
}

// EncodeTaskRequest encodes a TaskRequest into Protobuf format.
func (p Proto) EncodeTaskRequest(request orbital.TaskRequest) ([]byte, error) {
	return proto.Marshal(FromTaskRequestToProto(request))
}

// DecodeTaskResponse decodes Protobuf data into a TaskResponse.
func (p Proto) DecodeTaskResponse(bytes []byte) (orbital.TaskResponse, error) {
	pRes := orbitalpb.TaskResponse{}
	if err := proto.Unmarshal(bytes, &pRes); err != nil {
		return orbital.TaskResponse{}, err
	}
	return FromProtoToTaskResponse(&pRes)
}

// EncodeTaskResponse encodes a TaskResponse into Protobuf format.
func (p Proto) EncodeTaskResponse(response orbital.TaskResponse) ([]byte, error) {
	return proto.Marshal(FromTaskResponseToProto(response))
}
