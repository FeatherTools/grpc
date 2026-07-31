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

    let ofContract (contract: Feather.Contracts.Core.V1.Timestamp): Result<Timestamp, ContractError> =
        try contract.UnixTimestamp |> DateTimeOffset.FromUnixTimeMilliseconds |> Timestamp |> Ok
        with e -> Error (ContractError.ofExn e)

    let asContract (Timestamp timestamp): Feather.Contracts.Core.V1.Timestamp =
        Feather.Contracts.Core.V1.Timestamp (UnixTimestamp = timestamp.ToUnixTimeMilliseconds())

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

    let asContract (CorrelationId id): Feather.Contracts.Core.V1.CorrelationId =
        Feather.Contracts.Core.V1.CorrelationId (Id = id.ToString())

    let ofContract (contract: Feather.Contracts.Core.V1.CorrelationId): Result<CorrelationId, ContractError> =
        contract.Id
        |> Guid.tryParse
        |> Option.map CorrelationId
        |> Result.ofOption (ContractError.ofError $"Invalid CorrelationId: {contract.Id}")

open Alma.ServiceIdentification

[<RequireQualifiedAccess>]
module Spot =
    let ofContract (contract: Feather.Contracts.Core.V1.Spot): Result<Spot, ContractError> =
        Create.Spot(string contract.Zone, string contract.Bucket)
        |> Result.mapError (ContractError.ofFormattedError (sprintf "Invalid Spot(%A, %A): %A" contract.Zone contract.Bucket))

    let asContract (spot: Spot): Feather.Contracts.Core.V1.Spot =
        Feather.Contracts.Core.V1.Spot (Zone = (spot.Zone |> Zone.value), Bucket = (spot.Bucket |> Bucket.value))

[<RequireQualifiedAccess>]
module Instance =
    let ofContract (contract: Feather.Contracts.Core.V1.Instance): Result<Instance, ContractError> =
        contract.Instance_
        |> Create.Instance
        |> Result.mapError (ContractError.ofFormattedError (sprintf "Invalid Instance: %A"))

    let asContract instance: Feather.Contracts.Core.V1.Instance =
        Feather.Contracts.Core.V1.Instance (Instance_ = (instance |> Instance.concat "-"))

[<RequireQualifiedAccess>]
module Box =
    let ofContract (contract: Feather.Contracts.Core.V1.Box): Result<Box, ContractError> =
        result {
            let! instance = contract.Instance |> Instance.ofContract |> Result.mapError (ContractError.map (sprintf "Box.Instance: %s"))
            let! spot = contract.Spot |> Spot.ofContract |> Result.mapError (ContractError.map (sprintf "Box.Spot: %s"))

            return Create.Box(instance, spot)
        }

    let asContract (box: Box): Feather.Contracts.Core.V1.Box =
        Feather.Contracts.Core.V1.Box(
            Instance = (box |> Box.instance |> Instance.asContract),
            Spot = (box |> Box.spot |> Spot.asContract)
        )
