namespace Feather.Grpc

open System
open Grpc.Core
open Feather.ErrorHandling

type ContractError =
    | InvalidData of string option

    with
        override this.ToString() =
            match this with
            | InvalidData None -> "Invalid data"
            | InvalidData (Some details) -> sprintf "Invalid data: %s" details

[<RequireQualifiedAccess>]
module ContractError =
    let ofExn (exn: exn): ContractError =
        InvalidData (Some <| sprintf "%s: %s" (exn.GetType().FullName) exn.Message)

    let ofError (error: string): ContractError =
        InvalidData (Some error)

    let ofFormattedError (format: 'Error -> string) e: ContractError =
        InvalidData (Some (format e))

    let map f = function
        | InvalidData details -> InvalidData (details |> Option.map f)

    let format (contractError: ContractError) = contractError.ToString()

type GrpcError =
    {
        Name: string
        Message: string option
    }

    with
        override this.ToString() =
            match this with
            | { Name = name; Message = Some message } -> sprintf "GrpcError[%s]: %s" name message
            | { Name = name } -> sprintf "GrpcError[%s]" name

[<RequireQualifiedAccess>]
module GrpcError =
    let create name message = {
        Name = name
        Message = message
    }

    let asContract (err: GrpcError): Feather.Contracts.Core.V1.Error =
        Feather.Contracts.Core.V1.Error(
            Name = err.Name,
            Message = (err.Message |> Option.defaultValue "")
        )

    let ofContract (contract: Feather.Contracts.Core.V1.Error): GrpcError =
        {
            Name = try contract.Name with _ -> "Contracts.Error.Null"
            Message =
                try
                    match contract.Message with
                    | null | "" -> None
                    | msg -> Some msg
                with _ -> None
        }

    let ofExn (error: exn) = {
        Name = error.GetType() |> string
        Message = Some error.Message
    }

    let ofErrorWithMessage message (error: obj) = {
        Name = error.GetType() |> string
        Message = Some message
    }

    let ofContractError (contractError: ContractError) =
        contractError
        |> ContractError.format
        |> Some
        |> create "ContractError"

    let ofError (error: obj) =
        match error with
        | :? exn as exn -> ofExn exn
        | :? ContractError as contractError -> ofContractError contractError
        | :? Feather.Contracts.Core.V1.Error as contract -> ofContract contract
        | error ->
            {
                Name = error.GetType() |> string
                Message = None
            }

    let ofFormattedError f error =
        { ofError error with Message = Some (f error) }

    let map f (err: GrpcError): GrpcError =
        { err with Name = f err.Name }

    let mapMessage f (err: GrpcError): GrpcError =
        { err with Message = err.Message |> Option.map f }

    let format (grpcError: GrpcError) = grpcError.ToString()

[<AutoOpen>]
module GrpcErrorExtensions =

    // Having Result<_> members as extensions gives them lower priority in
    // overload resolution between Result<_> and Async<Result<_,_>>.
    type ResultBuilder with
        member __.ReturnFrom (grpcError: GrpcError) : Result<'Success, GrpcError> =
            grpcError |> Result.Error

        member this.Bind(grpcError: GrpcError, f: 'SuccessA -> Result<'SuccessB, GrpcError>): Result<'SuccessB, GrpcError> =
            this.Bind (grpcError |> Result.Error, f)

    // Having Result<_> members as extensions gives them lower priority in
    // overload resolution between Result<_> and Async<Result<_,_>>.
    type AsyncResultBuilder with
        member __.ReturnFrom (grpcError: GrpcError) : AsyncResult<'Success, GrpcError> =
            grpcError |> AsyncResult.ofError

        member this.Bind(grpcError: GrpcError, f: 'SuccessA -> AsyncResult<'SuccessB, GrpcError>): AsyncResult<'SuccessB, GrpcError> =
            this.Bind (grpcError |> AsyncResult.ofError, f)

[<RequireQualifiedAccess>]
module AsyncResult =
    let ofAsyncUnaryResponse (xR: AsyncUnaryCall<'Response>): AsyncResult<'Response, GrpcError> =
        asyncResult {
            try return! xR.ResponseAsync
            with ex -> return! AsyncResult.ofError ex
        }
        |> AsyncResult.mapError GrpcError.ofExn

[<AutoOpen>]
module AsyncUnaryResponseExtensions =

    // Having Result<_> members as extensions gives them lower priority in
    // overload resolution between Result<_> and Async<Result<_,_>>.
    type AsyncResultBuilder with
        member __.ReturnFrom (xR: AsyncUnaryCall<'Response>) : AsyncResult<'Response, GrpcError> =
            xR |> AsyncResult.ofAsyncUnaryResponse

        member this.Bind(grpcError: AsyncUnaryCall<'ResponseA>, f: 'ResponseA -> AsyncResult<'ResponseB, GrpcError>): AsyncResult<'ResponseB, GrpcError> =
            this.Bind (grpcError |> AsyncResult.ofAsyncUnaryResponse, f)
