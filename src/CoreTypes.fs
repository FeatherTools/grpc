namespace Feather.Grpc

open System
open Grpc.Core
open Feather.ErrorHandling
open Alma.Authorization.Common
open Alma.Authorization

type Timestamp = Timestamp of DateTimeOffset

[<RequireQualifiedAccess>]
module Timestamp =
    let now () =
        DateTimeOffset.UtcNow
        |> Timestamp

    let value (Timestamp timestamp) = timestamp

    let ofContract (contract: Feather.Contracts.Timestamp): Result<Timestamp, ContractError> =
        try contract.UnixTimestamp |> DateTimeOffset.FromUnixTimeMilliseconds |> Timestamp |> Ok
        with e -> Error (ContractError.ofExn e)

    let asContract (Timestamp timestamp): Feather.Contracts.Timestamp =
        Feather.Contracts.Timestamp (UnixTimestamp = timestamp.ToUnixTimeMilliseconds())

type CorrelationId = CorrelationId of Guid

[<RequireQualifiedAccess>]
module CorrelationId =
    let create (): CorrelationId =
        CorrelationId (Guid.NewGuid())

    let ofJWTId jwt =
        match JWT.Raw jwt with
        | JWT.HasPayloadValue "jti" (JWT.JWTValue.String jti) ->
            jti
            |> Guid.tryParse
            |> Option.map CorrelationId
        | _ -> None

    let uuid (CorrelationId id): Guid =
        id

    let value = uuid >> string

    let asContract (CorrelationId id): Feather.Contracts.CorrelationId =
        Feather.Contracts.CorrelationId (Id = id.ToString())

    let ofContract (contract: Feather.Contracts.CorrelationId): Result<CorrelationId, ContractError> =
        contract.Id
        |> Guid.tryParse
        |> Option.map CorrelationId
        |> Result.ofOption (ContractError.ofError $"Invalid CorrelationId: {contract.Id}")

open Alma.ServiceIdentification

[<RequireQualifiedAccess>]
module Spot =
    let ofContract (contract: Feather.Contracts.Spot): Result<Spot, ContractError> =
        Create.Spot(string contract.Zone, string contract.Bucket)
        |> Result.mapError (ContractError.ofFormattedError (sprintf "Invalid Spot(%A, %A): %A" contract.Zone contract.Bucket))

    let asContract (spot: Spot): Feather.Contracts.Spot =
        Feather.Contracts.Spot (Zone = (spot.Zone |> Zone.value), Bucket = (spot.Bucket |> Bucket.value))

[<RequireQualifiedAccess>]
module Instance =
    let ofContract (contract: Feather.Contracts.Instance): Result<Instance, ContractError> =
        contract.Instance_
        |> Create.Instance
        |> Result.mapError (ContractError.ofFormattedError (sprintf "Invalid Instance: %A"))

    let asContract instance: Feather.Contracts.Instance =
        Feather.Contracts.Instance (Instance_ = (instance |> Instance.concat "-"))

[<RequireQualifiedAccess>]
module Box =
    let ofContract (contract: Feather.Contracts.Box): Result<Box, ContractError> =
        result {
            let! instance = contract.Instance |> Instance.ofContract |> Result.mapError (ContractError.map (sprintf "Box.Instance: %s"))
            let! spot = contract.Spot |> Spot.ofContract |> Result.mapError (ContractError.map (sprintf "Box.Spot: %s"))

            return Create.Box(instance, spot)
        }

    let asContract (box: Box): Feather.Contracts.Box =
        Feather.Contracts.Box(
            Instance = (box |> Box.instance |> Instance.asContract),
            Spot = (box |> Box.spot |> Spot.asContract)
        )
