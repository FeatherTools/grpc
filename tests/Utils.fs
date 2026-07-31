module Feather.Grpc.Test.Utils

open System
open System.IO
open Expecto
open Feather.Grpc

let provideValidDateTimeOffsetStringsDefinedInPHP () =
    [
        "UTC offset", "2026-09-20T12:44:27+00:00", "2026-09-20 12:44:27"
        "positive offset", "2026-01-15T08:30:00+05:30", "2026-01-15 08:30:00"
        "negative offset", "2025-12-31T23:59:59-07:00", "2025-12-31 23:59:59"
        "midnight", "2026-03-01T00:00:00+00:00", "2026-03-01 00:00:00"
        "end of day", "2026-06-30T23:59:59+02:00", "2026-06-30 23:59:59"
        "valid ISO 8601", "2024-06-01T12:00:00+00:00", "2024-06-01 12:00:00"
        "actual value", "2026-07-22T19:59:49.9850000+00:00", "2026-07-22 19:59:49"
    ]

[<Tests>]
let dateTimeOffsetTests =
    testList "DateTimeOffset" [
        testCase "tryParse - string with non-zero offset returns Some" <| fun _ ->
            let result = DateTimeOffset.tryParse "2024-06-01T14:00:00+02:00"
            Expect.isSome result "parses offset string"
            Expect.equal result.Value.UtcDateTime (DateTime(2024, 6, 1, 12, 0, 0, DateTimeKind.Utc)) "UTC equivalent is correct"

        testCase "tryParse - empty string returns None" <| fun _ ->
            Expect.isNone (DateTimeOffset.tryParse "") "empty string returns None"

        testCase "tryParse - garbage string returns None" <| fun _ ->
            Expect.isNone (DateTimeOffset.tryParse "not-a-date") "garbage returns None"

        testCase "serialize - produces 'o' format string" <| fun _ ->
            let dt = DateTimeOffset(2024, 6, 1, 12, 0, 0, TimeSpan.Zero)
            Expect.equal (DateTimeOffset.serialize dt) "2024-06-01T12:00:00.0000000+00:00" "ISO 8601 round-trip format"

        testCase "serialize / tryParse round-trip" <| fun _ ->
            let dt = DateTimeOffset(2025, 12, 31, 23, 59, 59, 999, TimeSpan.Zero)
            let result = dt |> DateTimeOffset.serialize |> DateTimeOffset.tryParse
            Expect.equal result (Some dt) "round-trip preserves value"

        yield!
            provideValidDateTimeOffsetStringsDefinedInPHP ()
            |> List.map (fun (description, value, expectedLocal) ->
                testCase ("tryParse - " + description) <| fun _ ->
                    let result = DateTimeOffset.tryParse value
                    Expect.isSome result $"parses {description}"
                    let actual = result.Value.DateTime.ToString("yyyy-MM-dd HH:mm:ss")
                    Expect.equal actual expectedLocal $"local datetime matches for {description}"
            )
    ]
