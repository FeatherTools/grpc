module Feather.Grpc.Test.CoreTypes

open System
open System.IO
open Expecto
open Alma.ServiceIdentification
open Alma.Authorization
open Alma.Authorization.Common
open Feather.ErrorHandling
open Feather.Cryptography
open FSharp.Control
open Feather.Grpc

//
// ContractError
//

[<Tests>]
let contractErrorTests =
    testList "ContractError" [

        testCase "ofError - formats message" <| fun _ ->
            let err = ContractError.ofError "something went wrong"
            Expect.equal (ContractError.format err) "Invalid data: something went wrong" "formats with detail"

        testCase "InvalidData None - formats without detail" <| fun _ ->
            let err = InvalidData None
            Expect.equal (ContractError.format err) "Invalid data" "formats without detail"

        testCase "ofExn - includes exception type and message" <| fun _ ->
            let exn = Exception "boom"
            let err = ContractError.ofExn exn
            let formatted = ContractError.format err
            Expect.isTrue (formatted.Contains "boom") "includes exception message"
            Expect.isTrue (formatted.Contains "Exception") "includes exception type"

        testCase "ofFormattedError - uses custom formatter" <| fun _ ->
            let err = ContractError.ofFormattedError (sprintf "code=%d") 42
            Expect.equal (ContractError.format err) "Invalid data: code=42" "uses formatter"
    ]

//
// Timestamp
//

[<Tests>]
let timestampTests =
    testList "Timestamp" [

        testCase "now - returns UTC timestamp" <| fun _ ->
            let before = DateTimeOffset.UtcNow
            let ts = Timestamp.now ()
            let after = DateTimeOffset.UtcNow
            let value = Timestamp.value ts
            Expect.isTrue (value >= before) "timestamp is not before call"
            Expect.isTrue (value <= after) "timestamp is not after call"

        testCase "value - unwraps DateTimeOffset" <| fun _ ->
            let dto = DateTimeOffset(2024, 6, 1, 12, 0, 0, TimeSpan.Zero)
            let ts = Timestamp dto
            Expect.equal (Timestamp.value ts) dto "value returns inner DateTimeOffset"

        testCase "asContract - stores milliseconds" <| fun _ ->
            let dto = DateTimeOffset.FromUnixTimeMilliseconds 1_700_000_000_000L
            let contract = Timestamp dto |> Timestamp.asContract
            Expect.equal contract.UnixTimestamp 1_700_000_000_000L "stores milliseconds"

        testCase "ofContract - restores from milliseconds" <| fun _ ->
            let contract = Feather.Contracts.Core.V1.Timestamp(UnixTimestamp = 1_700_000_000_000L)
            let result = Timestamp.ofContract contract
            let expected = DateTimeOffset.FromUnixTimeMilliseconds 1_700_000_000_000L
            Expect.equal result (Ok (Timestamp expected)) "restores DateTimeOffset"

        testCase "asContract / ofContract round-trip - millisecond precision preserved" <| fun _ ->
            // Normalise to ms first — asContract truncates sub-millisecond precision
            let ms = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            let ts = DateTimeOffset.FromUnixTimeMilliseconds ms |> Timestamp
            let result = ts |> Timestamp.asContract |> Timestamp.ofContract
            Expect.equal result (Ok ts) "round-trip preserves millisecond-precision timestamp"

        testCase "asContract / ofContract round-trip - sub-millisecond is truncated" <| fun _ ->
            // DateTimeOffset with ticks that are not whole milliseconds
            let dto = DateTimeOffset(2024, 1, 1, 0, 0, 0, 0, TimeSpan.Zero).AddTicks 9999L
            let ts = Timestamp dto
            let result = ts |> Timestamp.asContract |> Timestamp.ofContract
            // Sub-millisecond part is lost after round-trip
            let truncated = DateTimeOffset.FromUnixTimeMilliseconds(dto.ToUnixTimeMilliseconds()) |> Timestamp
            Expect.equal result (Ok truncated) "sub-millisecond ticks are truncated to milliseconds"

        testCase "asContract / ofContract round-trip - epoch zero" <| fun _ ->
            let ts = DateTimeOffset.FromUnixTimeMilliseconds 0L |> Timestamp
            let result = ts |> Timestamp.asContract |> Timestamp.ofContract
            Expect.equal result (Ok ts) "round-trips unix epoch"

        testCase "asContract / ofContract round-trip - negative (pre-epoch)" <| fun _ ->
            let ts = DateTimeOffset.FromUnixTimeMilliseconds -1_000L |> Timestamp
            let result = ts |> Timestamp.asContract |> Timestamp.ofContract
            Expect.equal result (Ok ts) "round-trips pre-epoch timestamp"

        // Fixed point in time shared with PHP tests (TimestampTest::UNIX_MS) to verify cross-language sync.
        // PHP: private const int UNIX_MS = 1705314600123; // 2024-01-15T10:30:00.123Z
        testCase "asContract - fixed string timestamp produces known unix ms" <| fun _ ->
            let dto = DateTimeOffset.Parse("2024-01-15T10:30:00.123+00:00")
            let contract = Timestamp dto |> Timestamp.asContract
            Expect.equal contract.UnixTimestamp 1_705_314_600_123L "fixed timestamp matches PHP TimestampTest::UNIX_MS"

        testCase "ofContract - returns error for null contract" <| fun _ ->
            let result = Timestamp.ofContract null
            Expect.isError result "null contract yields error"
    ]

//
// CorrelationId
//

[<Tests>]
let correlationIdTests =
    testList "CorrelationId" [

        testCase "create - produces unique ids" <| fun _ ->
            let id1 = CorrelationId.create ()
            let id2 = CorrelationId.create ()
            Expect.notEqual id1 id2 "each created CorrelationId is unique"

        testCase "value - returns guid string" <| fun _ ->
            let guid = Guid.NewGuid()
            let id = CorrelationId guid
            Expect.equal (CorrelationId.value id) (guid.ToString()) "value returns guid string"

        testCase "uuid - unwraps guid" <| fun _ ->
            let guid = Guid.NewGuid()
            let id = CorrelationId guid
            Expect.equal (CorrelationId.uuid id) guid "uuid returns inner Guid"

        testCase "asContract / ofContract round-trip" <| fun _ ->
            let id = CorrelationId.create ()
            let result = id |> CorrelationId.asContract |> CorrelationId.ofContract
            Expect.equal result (Ok id) "round-trip preserves CorrelationId"

        testCase "ofContract - invalid guid" <| fun _ ->
            let contract = Feather.Contracts.Core.V1.CorrelationId(Id = "not-a-guid")
            Expect.isError (CorrelationId.ofContract contract) "should reject invalid guid"

        testCase "ofContract - empty string" <| fun _ ->
            let contract = Feather.Contracts.Core.V1.CorrelationId(Id = "")
            Expect.isError (CorrelationId.ofContract contract) "should reject empty string"
    ]

//
// Spot
//

[<Tests>]
let spotTests =
    testList "Spot" [

        testCase "asContract / ofContract round-trip" <| fun _ ->
            let spot = { Zone = Zone "eu"; Bucket = Bucket "prod" }
            let result = spot |> Spot.asContract |> Spot.ofContract
            Expect.equal result (Ok spot) "round-trip preserves Spot"

        testCase "asContract - stores zone and bucket" <| fun _ ->
            let spot = { Zone = Zone "us"; Bucket = Bucket "staging" }
            let contract = Spot.asContract spot
            Expect.equal contract.Zone "us" "zone matches"
            Expect.equal contract.Bucket "staging" "bucket matches"

        testCase "ofContract - invalid zone" <| fun _ ->
            let contract = Feather.Contracts.Core.V1.Spot(Zone = "", Bucket = "prod")
            Expect.isError (Spot.ofContract contract) "should reject empty zone"

        testCase "ofContract - invalid bucket" <| fun _ ->
            let contract = Feather.Contracts.Core.V1.Spot(Zone = "eu", Bucket = "")
            Expect.isError (Spot.ofContract contract) "should reject empty bucket"
    ]
