package codec

import (
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/openkcm/orbital"
	orbitalpb "github.com/openkcm/orbital/proto/orbital/v1"
)

// Proto is a codec that encodes and decodes TaskRequest and TaskResponse in Protobuf format.
type Proto struct{}

var _ orbital.Codec = Proto{}

// DecodeTaskRequest decodes Protobuf data into a TaskRequest.
func (p Proto) DecodeTaskRequest(bytes []byte) (orbital.TaskRequest, error) {
	empty := orbital.TaskRequest{}
	pReq := orbitalpb.TaskRequest{}
	err := proto.Unmarshal(bytes, &pReq)
	if err != nil {
		return empty, err
	}
	id, err := uuid.Parse(pReq.TaskId)
	if err != nil {
		return empty, err
	}
	req := orbital.TaskRequest{
		TaskID:       id,
		Type:         pReq.GetType(),
		ExternalID:   pReq.GetExternalId(),
		WorkingState: pReq.GetWorkingState(),
		ETag:         pReq.GetEtag(),
		Data:         pReq.GetData(),
	}
	meta := pReq.GetMetaData()
	if meta == nil {
		return req, nil
	}
	req.MetaData.IsEncrypted = meta.GetIsEncrypted()

	sig := meta.GetSignature()
	if sig == nil {
		return req, nil
	}

	req.MetaData.Signature.Value = sig.GetValue()
	req.MetaData.Signature.Type = sig.GetType()
	return req, nil
}

// EncodeTaskRequest encodes a TaskRequest into Protobuf format.
func (p Proto) EncodeTaskRequest(request orbital.TaskRequest) ([]byte, error) {
	return proto.Marshal(&orbitalpb.TaskRequest{
		TaskId:       request.TaskID.String(),
		Type:         request.Type,
		ExternalId:   request.ExternalID,
		WorkingState: request.WorkingState,
		Etag:         request.ETag,
		Data:         request.Data,
		MetaData: &orbitalpb.MetaData{
			Signature: &orbitalpb.Signature{
				Value: request.MetaData.Signature.Value,
				Type:  request.MetaData.Signature.Type,
			},
			IsEncrypted: request.MetaData.IsEncrypted,
		},
	})
}

// DecodeTaskResponse decodes Protobuf data into a TaskResponse.
func (p Proto) DecodeTaskResponse(bytes []byte) (orbital.TaskResponse, error) {
	empty := orbital.TaskResponse{}
	pRes := orbitalpb.TaskResponse{}
	err := proto.Unmarshal(bytes, &pRes)
	if err != nil {
		return empty, err
	}
	id, err := uuid.Parse(pRes.TaskId)
	if err != nil {
		return empty, err
	}
	resp := orbital.TaskResponse{
		TaskID:            id,
		Type:              pRes.GetType(),
		ExternalID:        pRes.GetExternalId(),
		WorkingState:      pRes.GetWorkingState(),
		ETag:              pRes.GetEtag(),
		Status:            pRes.GetStatus().String(),
		ErrorMessage:      pRes.GetErrorMessage(),
		ReconcileAfterSec: pRes.GetReconcileAfterSec(),
	}
	meta := pRes.GetMetaData()
	if meta == nil {
		return resp, nil
	}
	resp.MetaData.IsEncrypted = meta.GetIsEncrypted()

	sig := meta.GetSignature()
	if sig == nil {
		return resp, nil
	}
	resp.MetaData.Signature.Value = sig.GetValue()
	resp.MetaData.Signature.Type = sig.GetType()
	return resp, nil
}

// EncodeTaskResponse encodes a TaskResponse into Protobuf format.
func (p Proto) EncodeTaskResponse(response orbital.TaskResponse) ([]byte, error) {
	return proto.Marshal(&orbitalpb.TaskResponse{
		TaskId:            response.TaskID.String(),
		Type:              response.Type,
		ExternalId:        response.ExternalID,
		WorkingState:      response.WorkingState,
		Etag:              response.ETag,
		Status:            orbitalpb.TaskStatus(orbitalpb.TaskStatus_value[response.Status]),
		ErrorMessage:      &response.ErrorMessage,
		ReconcileAfterSec: response.ReconcileAfterSec,
		MetaData: &orbitalpb.MetaData{
			Signature: &orbitalpb.Signature{
				Value: response.MetaData.Signature.Value,
				Type:  response.MetaData.Signature.Type,
			},
			IsEncrypted: response.MetaData.IsEncrypted,
		},
	})
}
