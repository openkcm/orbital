# Code Generation for Protobuf Files

This project uses Protocol Buffers (.proto) to define messages sent during reconciliation.

## Prerequisites

Make sure you have the following tools installed:

- **Install `buf`:** If you haven't already, install `buf` by following the instructions on
   the [Buf website](https://buf.build/docs/installation/).
   You can install buf on macOS or Linux using Homebrew:

```sh
   brew install bufbuild/buf/buf
```

- protoc (Protocol Buffers compiler)
- Go plugins:

  ```sh
  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
  go install golang.org/x/tools/cmd/goimports@latest
  ```

Ensure `$GOPATH/bin` is in your `$PATH` so the plugins are accessible.

## Generate Go Code

From the root of the repository, run:

```sh
make proto-generate
```

Replace `github.com/openkcm/orbital/proto/orbital` with your import path

#### Notes

- Re-run this command whenever .proto files change.
- Do not manually modify the generated .pb.go files — they are overwritten on regeneration.
