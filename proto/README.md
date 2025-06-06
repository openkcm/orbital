# Code Generation for Protobuf Files

This project uses Protocol Buffers (.proto) to define messages sent during reconciliation.

## Prerequisites

Make sure you have the following tools installed:

- protoc (Protocol Buffers compiler)
- Go plugins:
  ```
  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
  ```

Ensure `$GOPATH/bin` is in your `$PATH` so the plugins are accessible.

## Generate Go Code

From the root of the repository, run:

```
protoc \
  --proto_path=proto \
  --go_out=. \
  --go_opt=paths=source_relative \
  --go_opt=Morbital/task_request.proto=github.com/openkcm/orbital/proto/orbital \
  --go_opt=Morbital/task_response.proto=github.com/openkcm/orbital/proto/orbital \
  orbital/task_request.proto \
  orbital/task_response.proto
```
Replace `github.com/openkcm/orbital/proto/orbital` with your import path

#### Notes
- Re-run this command whenever .proto files change. 
- Do not manually modify the generated .pb.go files — they are overwritten on regeneration.
