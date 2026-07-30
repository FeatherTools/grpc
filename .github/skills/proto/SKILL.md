---
name: proto
description: "Use when authoring or editing Protocol Buffer (.proto) files for gRPC service contracts that use the Feather core types and conventions."
---

# Protocol Buffers / gRPC Skill

Conventions for writing `.proto` gRPC service contracts that interoperate with
`Feather.Grpc` and the shared core types from
[grpc.contract.core](https://github.com/FeatherTools/grpc.contract.core)
(`Spot`, `Error`, `Timestamp`, `CorrelationId`, `Instance`, `Box`,
`SerializedForChunking`, …).

## File Structure

```
proto/
  <systemName>/                       # one folder per system (camelCase)
    <service_or_feature>.proto        # one file per logical service/feature
```

Group `.proto` files by system, one file per logical service or feature. Reference the
shared core types from `grpc.contract.core` rather than redefining them.

---

## File Header Template

```protobuf
syntax = "proto3";

package <system_name>;   // snake_case, matches folder name

import "feather/core.proto";
// add other imports as needed

option csharp_namespace = "<Namespace>";   // PascalCase
```

Add `option php_namespace`, `go_package`, etc. for whichever languages you generate.

---

## Naming Conventions

- Message names: `PascalCase`
- Request messages: `<MethodName>Request`
- Response messages: `<MethodName>Response`
- Field names: `snake_case`
- Enum values: `UPPER_SNAKE_CASE`
- Service names: `<ServiceName>Service` (or `<ServiceName>`)

---

## Standard Response Pattern

**All responses use `oneof result { Success / Error }`:**

```protobuf
message <Method>Response {
  oneof result {
    Success       success = 1;
    feather.Error error   = 2;
  }

  message Success {
    <ReturnType> <field_name> = 1;
  }
}
```

Never return bare fields at the top level of a response — always wrap them in the
`oneof result` pattern. On the F# side this maps cleanly onto
`HighLevel.Response.handle`, which produces either a `Success` or an `Error` response.

---

## Service Definition

```protobuf
service <ServiceName>Service {
  rpc <MethodName> (<MethodName>Request) returns (<MethodName>Response);
}
```

---

## Streaming Large Payloads

For large or compressible payloads, stream `feather.SerializedForChunking` chunks
instead of a single message. The sender serializes and chunks the payload; the receiver
reassembles it. On the F# side this is handled by `SerializedForChunking` (plain, gzip,
raw bytes or text) — see the `Feather.Grpc` README.

```protobuf
message SendDocumentRequest {
  feather.SerializedForChunking chunk = 1;
}

service DocumentService {
  rpc SendDocument (stream SendDocumentRequest) returns (SendDocumentResponse);
}
```

---

## Security Rules in Proto

- Do not put personal or sensitive data as plain fields — carry it as opaque `bytes`
  (e.g. an encrypted envelope) and decrypt at the domain boundary.
- Auth/identity info (tokens, JWTs) is extracted by server interceptors — do NOT include
  them in request messages. Add a comment `// Extracted from JWT by auth interceptor`
  where relevant.

---

## What does NOT belong in proto files

Proto files represent gRPC service contracts only — request/response messages and
service definitions. Domain-internal concerns (events, stream carriers, background
messages such as `*Event` / `*Stream`) must never be added to `.proto` files, even when
they reference types that do have proto equivalents.

---

## Workflow: Adding a new RPC method

1. Add `<Method>Request` and `<Method>Response` messages (Response uses the `oneof result` pattern).
2. Add the `rpc` entry to the `service` block.
3. Regenerate stubs for your target languages.
4. Lint the proto (e.g. `protolint`) to verify formatting.
