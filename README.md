# <img src="https://github.com/FeatherTools/.github/blob/main/profile/feather-logo-200.png" alt="FeatherTools Logo" width="100" height="100"> gRPC

[![NuGet](https://img.shields.io/nuget/v/Feather.Grpc.svg)](https://www.nuget.org/packages/Feather.Grpc)
[![NuGet Downloads](https://img.shields.io/nuget/dt/Feather.Grpc.svg)](https://www.nuget.org/packages/Feather.Grpc)
[![Checks](https://github.com/FeatherTools/grpc/actions/workflows/tests.yaml/badge.svg)](https://github.com/FeatherTools/grpc/actions/workflows/tests.yaml)

> Library with low and high level helpers for gRPC.

## Install

```sh
paket add Feather.Grpc
```

The conversions in this library map to the shared core proto types from
[grpc.contract.core](https://github.com/FeatherTools/grpc.contract.core)
(`Timestamp`, `CorrelationId`, `Spot`, `Instance`, `Box`, `Error`,
`SerializedForChunking`, …). Reference these in your own `.proto` definitions so
messages interoperate across services and languages.

> **Copilot skill**: this repo ships a `proto` skill at
> [.github/skills/proto/SKILL.md](.github/skills/proto/SKILL.md) with conventions for
> authoring `.proto` service contracts (naming, the `oneof result { Success / Error }`
> pattern, streaming) that align with `Feather.Grpc`.

## Usage

Low- and high-level helpers for building gRPC clients and servers in F#, with
streaming, chunking of large payloads, auth, error handling and metrics.

### Creating a client (in k8s environment)

Build a channel to a service instance and wrap it in the generated client:

```fsharp
open Feather.Grpc

// Application startup
let connectCalculatorClient (environment: Map<string, string>) (myService: string) = result {
    let! myServiceInstance = myService |> instance environment // Result<Instance, 'Error>

    return myServiceInstance |> Grpc.k8sSvcChannel Grpc.Port |> Math.Calculator.CalculatorClient
}

let! myServiceClient = "MY_SERVICE" |> connectCalculatorClient environment
```

### Unary calls

`AsyncResult.ofAsyncUnaryResponse` turns a gRPC unary call into an `AsyncResult<'Response, GrpcError>`,
so it composes inside an `asyncResult { … }` block (see [Feather/Error Handling](https://github.com/FeatherTools/error-handling)).
Convert contract types at the boundary with `ofContract` / `asContract`.

The example below is a `Calculator` service exposing a single `Divide` call.

Proto definition — reuse the shared core types (`Spot`, `Error`) from
[grpc.contract.core](https://github.com/FeatherTools/grpc.contract.core):

```proto
syntax = "proto3";

package calculator;
option csharp_namespace = "Math";

import "feather/core.proto";

message Input  { int32 value = 1; }
message Output { int32 value = 1; }

message DivideRequest {
    Input base    = 1;
    Input divider = 2;
}

message DivideResponse {
    oneof result {
        Success            success = 1;
        feather.core.Error error   = 2;
    }

    message Success {
        Output output = 1;
    }
}

service Calculator {
    rpc Divide (DivideRequest) returns (DivideResponse);
}
```

Domain library — the `ofContract` / `asContract` pairs convert between the generated
proto messages and your domain types:

```fsharp
namespace Domain

open Feather.Grpc

type Input = Input of int
type Output = Output of int

[<RequireQualifiedAccess>]
module Input =
    let ofContract (contract: Math.Input): Result<Input, ContractError> =
        // it could have some validation, ...
        contract.Value |> Input |> Ok

    let asContract (Input value): Math.Input =
        Math.Input(Value = value)

[<RequireQualifiedAccess>]
module Output =
    let ofContract (contract: Math.Output): Result<Output, ContractError> =
        // it could have some validation, ...
        contract.Value |> Output |> Ok

    let asContract (Output value): Math.Output =
        Math.Output(Value = value)
```

Calculator Service implementation
```fsharp
open Feather.Grpc
open Feather.Grpc.Metrics

// your app metrics, built once at startup via `GrpcMetrics.metrics currentInstance`
type AppMetrics = {
    // ... app specific metrics ...
    GrpcMetrics: GrpcMetrics
}

type ApplicationDependencies = {
    LoggerFactory: ILoggerFactory
    Metrics: AppMetrics
}

type CalculatorImplementation(app: ApplicationDependencies) =
    inherit Calculator.CalculatorBase()

    override _.Divide (request: DivideRequest, context: ServerCallContext): Task<DivideResponse> = task {
        let logger = app.LoggerFactory.CreateLogger("Divide")

        // pure operation, returning a success response or GrpcError
        let operation request = asyncResult {
            let! (Input value) = request.Base |> Input.ofContract |> Result.mapError GrpcError.ofContractError
            let! (Input divider) = request.Divider |> Input.ofContract |> Result.mapError GrpcError.ofContractError
            logger.LogDebug("Calculating: {value} / {divider}", value, divider)

            if divider = 0 then
                // GrpcError is automatically bind to an Result.Error by predefined helper
                return! GrpcError.create "DivisionByZero" (Some $"{value} / {divider}")

            let output = Output (value / divider)

            return DivideResponse.Types.Success(
                Output = (output |> Output.asContract)
            )
        }

        return!
            request
            |> operation
            |> HighLevel.Response.handle app.Metrics.GrpcMetrics "Divide" logger None
                (fun success -> DivideResponse(Success = success))
                (fun error -> DivideResponse(Error = error))
    }
```

Calling a service by its client
```fsharp
let example (myServiceClient: Calculator.CalculatorClient) (a: Domain.Input) (b: Domain.Input) = asyncResult {
    let! (response: DivideResponse) =
        myServiceClient.DivideAsync(
            DivideRequest(
                Base = Input.asContract a,
                Divider = Input.asContract b
            )
        )
        |> AsyncResult.ofAsyncUnaryResponse

    match response.ResultCase with
    | DivideResponse.ResultOneofCase.Success ->
        let! (Output value) = response.Success.Output |> Output.ofContract |> Result.mapError GrpcError.ofContractError

        return value

    | _ ->
        return! response.Error |> GrpcError.ofContract   // possibly division by zero
}
```

### Streaming large payloads

`SerializedForChunking` serializes a DTO once and splits it into ~256 KB chunks so it
can be streamed over gRPC (plain, gzip, raw bytes or text):

```fsharp
let ct = context.CancellationToken // core gRPC request context contains its cancellation, it should be used

// server side: stream a value out
value
|> HighLevel.Send.ServerStream.gzipValue ct writer serialize chunkToRequest

// client side: read the chunks back into a value
call
|> HighLevel.Read.Stream.Call.value ct handleResponse (SerializedForChunking.Dto.Gzip.fromChunks parse)
```

### Server auth interceptor

```fsharp
let authenticate: AuthInterceptor<MyContext> =
    fun context -> ... // Result<MyContext, AuthError>

let context = AuthInterceptor.validate authenticate serverCallContext
```

## Release
1. Increment version in `Grpc.fsproj`
2. Update `CHANGELOG.md`
3. Commit new version and tag it

## Development
### Requirements
- [dotnet core](https://dotnet.microsoft.com/learn/dotnet/hello-world-tutorial)

### Build
```bash
./build.sh build
```

### Tests
```bash
./build.sh -t tests
```
