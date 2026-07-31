module Feather.Grpc.Test.Serialization

open System
open System.IO
open Expecto
open Feather.Contracts
open Feather.Cryptography
open Feather.ErrorHandling
open Alma.Serializer
open FSharp.Control
open Feather.Grpc

let private debug = false
let private sleepMultiplier = if debug then 1 else 0

//
// SerializedForChunking
//
// Uses a minimal Name type defined here only — principle tests, not domain-specific.
// Mirror these in the PHP library for interoperability verification.
//

[<AutoOpen>]
module NameFixture =
    open FSharp.Data

    type NameDto = { FirstName: string; LastName: string }

    [<RequireQualifiedAccess>]
    module NameDto =
        let serialize (dto: NameDto) =
            dto |> Serialize.toJson

        type private DtoSchema = JsonProvider<"""{"first_name":"name","last_name":"name"}""">

        let parse (json: string): Result<NameDto, ContractError> = result {
            let! parsed =
                try DtoSchema.Parse json |> Ok
                with e -> Result.Error (ContractError.ofExn e)

            return {
                FirstName = parsed.FirstName
                LastName  = parsed.LastName
            }
        }

    let smallDto  = { FirstName = "Jan"; LastName = "Novák" }
    let smallJson = NameDto.serialize smallDto

[<Tests>]
let serializedForChunkingTests =
    testList "SerializedForChunking" [

        // ---- wrap / unwrap ----

        testCase "wrap / unwrap round-trip" <| fun _ ->
            let result = smallDto |> SerializedForChunking.wrap |> SerializedForChunking.unwrap
            Expect.equal result smallDto "unwrap returns original dto"

        // ---- toChunks ----

        testCase "toChunks - chunk content is UTF-8 encoded JSON" <| fun _ ->
            let chunks =
                smallDto
                |> SerializedForChunking.wrap
                |> SerializedForChunking.Chunks.toChunks NameDto.serialize

            Expect.equal chunks.Length 1 "small JSON fits in one chunk"

            let decoded = chunks[0].Content.ToByteArray() |> System.Text.Encoding.UTF8.GetString
            Expect.equal decoded smallJson "chunk bytes decode to expected JSON"

        // ---- fromChunks ----

        testCase "fromChunks - empty list returns error" <| fun _ ->
            let result = SerializedForChunking.Chunks.fromChunks NameDto.parse []
            Expect.isError result "empty chunk list should fail parse"

        testCase "fromChunks - malformed JSON returns error" <| fun _ ->
            let badBytes = System.Text.Encoding.UTF8.GetBytes "{not valid json}"
            let result = SerializedForChunking.Chunks.fromChunks NameDto.parse [ SerializedForChunking.ofBytes badBytes ]
            Expect.isError result "malformed JSON should return error"

        testCase "fromChunks - wrong parse function returns error" <| fun _ ->
            let chunks =
                smallDto
                |> SerializedForChunking.wrap
                |> SerializedForChunking.Chunks.toChunks NameDto.serialize

            let wrongParse (_: string): Result<NameDto, ContractError> =
                Result.Error (ContractError.ofError "wrong parser")

            let result = SerializedForChunking.Chunks.fromChunks wrongParse chunks
            Expect.isError result "mismatched parser returns error"

        // ---- full toChunks → fromChunks round-trip ----

        testCase "fromChunks - single chunk round-trips" <| fun _ ->
            let result =
                // dto -> chunks
                smallDto
                |> SerializedForChunking.wrap
                |> SerializedForChunking.Chunks.toChunks NameDto.serialize
                // chunks -> dto
                |> SerializedForChunking.Chunks.fromChunks NameDto.parse
                |> Result.map SerializedForChunking.unwrap

            Expect.equal result (Ok smallDto) "round-trip via single chunk"

        // ---- interop anchor — same values verified in PHP tests ----
        // JSON:  {"first_name":"Jan","last_name":"Doe"}
        // Bytes: [| 123;34;102;105;114;115;116;95;110;97;109;101;34;58;34;74;97;110;34;44;
        //           34;108;97;115;116;95;110;97;109;101;34;58;34;68;111;101;34;125 |]
        // (pure ASCII — each character is exactly one byte)

        testCase "interop - known bytes deserialize to expected dto" <| fun _ ->
            let knownBytes =
                [| 123uy;34uy;102uy;105uy;114uy;115uy;116uy;95uy;110uy;97uy;109uy;101uy;34uy;58uy;34uy;74uy;97uy;110uy;34uy;44uy
                   34uy;108uy;97uy;115uy;116uy;95uy;110uy;97uy;109uy;101uy;34uy;58uy;34uy;68uy;111uy;101uy;34uy;125uy |]
            let result =
                SerializedForChunking.Chunks.fromChunks NameDto.parse [ SerializedForChunking.ofBytes knownBytes ]
                |> Result.map SerializedForChunking.unwrap
            Expect.equal result (Ok { FirstName = "Jan"; LastName = "Doe" }) "known bytes deserialize correctly"

        testCase "interop - known dto serializes to expected bytes" <| fun _ ->
            let knownBytes =
                [| 123uy;34uy;102uy;105uy;114uy;115uy;116uy;95uy;110uy;97uy;109uy;101uy;34uy;58uy;34uy;74uy;97uy;110uy;34uy;44uy
                   34uy;108uy;97uy;115uy;116uy;95uy;110uy;97uy;109uy;101uy;34uy;58uy;34uy;68uy;111uy;101uy;34uy;125uy |]
            let actualBytes =
                { FirstName = "Jan"; LastName = "Doe" }
                |> SerializedForChunking.wrap
                |> SerializedForChunking.Chunks.toChunks NameDto.serialize
                |> List.exactlyOne
                |> fun c -> c.Content.ToByteArray()
            Expect.equal actualBytes knownBytes "dto serializes to known bytes"
    ]

//
// SerializedForChunking — large-file streaming tests
//
// Uses tests/fixtures/plain-file.md (~519 KB) to exercise multi-chunk paths.
// Both streaming modes are verified end-to-end.
//

[<AutoOpen>]
module TextPageFixture =
    open FSharp.Data
    open FSharp.Control

    type TextPageDto = {
        FileName: string
        Content: string
    }

    [<RequireQualifiedAccess>]
    module TextPageDto =
        let serialize (dto: TextPageDto) = dto |> Serialize.toJson

        type private DtoSchema = JsonProvider<"""{"content":"text","file_name":"name"}""">

        let parse (json: string) : Result<TextPageDto, ContractError> = result {
            let! parsed =
                try DtoSchema.Parse json |> Ok
                with e -> Result.Error (ContractError.ofExn e)

            return {
                FileName = parsed.FileName
                Content = parsed.Content
            }
        }

    let fixturePath (name: string) =
        Path.Combine(__SOURCE_DIRECTORY__, "fixtures", name)

[<Tests>]
let serializedForChunkingLargeFileTests =
    testList "SerializedForChunking - large file" [

        // ---- raw text streaming ----
        // Each chunk is a self-contained UTF-8 slice; streaming stops even if you
        // drop the connection after the first chunk.

        // ---- AI simulated response ----
        // Each partial AI message is independently streamed as its own proto chunk —
        // the client can display it immediately without waiting for the full response.
        // This mirrors: AI engine --(gRPC raw stream)--> server --(WebSocket)--> browser.
        //
        // Producer and consumer run in parallel via a Channel<T>, which represents
        // the gRPC stream buffer. The consumer receives and processes each chunk
        // the instant it is written — before the next message is even produced.

        testCaseAsync "AI simulated response" <| async {
            do! Async.Sleep (6000 * sleepMultiplier) // ensure timestamp differences are visible in logs

            let grpcStream = System.Threading.Channels.Channel.CreateUnbounded<Feather.Contracts.SerializedForChunking>()

            // AI engine: produces 10 partial messages, each encoded to one proto chunk,
            // written into the channel at its own pace.
            let producer = async {
                do!
                    [1..10]
                    |> List.map (fun i -> async {
                        do! Async.Sleep (300 * sleepMultiplier)
                        let msg = sprintf "Partial message %d: the AI is generating this response incrementally. " i
                        if debug then printfn "[AI engine] +%d ms  emitting message %d" (i * 300) i
                        return msg
                    })
                    |> AsyncSeq.ofSeqAsync
                    |> AsyncSeq.collect (SerializedForChunking.wrap >> SerializedForChunking.Parts.Text.toStream)
                    |> AsyncSeq.iterAsync (fun chunk ->
                        grpcStream.Writer.WriteAsync(chunk).AsTask() |> Async.AwaitTask)
                grpcStream.Writer.Complete()
            }

            // gRPC stream as AsyncSeq — yields chunks as they are written by the producer.
            let wireFromChannel : AsyncSeq<Feather.Contracts.SerializedForChunking> =
                asyncSeq {
                    let mutable running = true
                    while running do
                        let! hasMore = grpcStream.Reader.WaitToReadAsync().AsTask() |> Async.AwaitTask
                        if hasMore then
                            let mutable chunk = Unchecked.defaultof<_>
                            while grpcStream.Reader.TryRead(&chunk) do
                                yield chunk
                        else
                            running <- false
                }

            // Start producer as a child — it runs concurrently while we consume.
            let! producerTask = Async.StartChild producer

            // Client: receives and forwards each chunk the moment it arrives.
            let! received =
                wireFromChannel
                |> SerializedForChunking.Parts.Text.fromStream
                |> AsyncSeq.mapiAsync (fun i wrapped -> async {
                    let part = SerializedForChunking.unwrap wrapped
                    if debug then printfn "[Client WS] OnPartialMessage [%d]: %s" i (part.TrimEnd())
                    return part
                })
                |> AsyncSeq.toListAsync

            // Ensure producer completed without error.
            do! producerTask

            Expect.equal received.Length 10 "one proto chunk per partial AI message"

            let expected =
                [1..10]
                |> List.map (sprintf "Partial message %d: the AI is generating this response incrementally. ")

            Expect.equal received expected "each partial message received in order and intact"
        }

        testCaseAsync "toRawTextStream / fromRawTextStream - large file round-trips" <| async {
            do! Async.Sleep (100 * sleepMultiplier) // ensure timestamp differences are visible in logs
            let text  = File.ReadAllText(fixturePath "plain-file.md")
            let! chunks =
                text
                |> SerializedForChunking.wrap
                |> SerializedForChunking.Parts.Text.toStream
                |> AsyncSeq.mapiAsync (fun i chunk -> async {
                    do! Async.Sleep (300 * sleepMultiplier) // simulate network delay
                    if debug then printfn "[Part][%d] Received chunk of size %d bytes" i (chunk.CalculateSize())

                    let part =
                        chunk.Content.ToByteArray()
                        |> Encode.bytesToString

                    if debug then printfn "[Part][%d] Chunk content preview: %s" i part[0..50]

                    return chunk
                })
                |> AsyncSeq.toListAsync

            Expect.isGreaterThan chunks.Length 1 "file > 256 KB should produce multiple chunks"

            let! parts =
                chunks
                |> AsyncSeq.ofSeq
                |> SerializedForChunking.Parts.Text.fromStream
                |> AsyncSeq.mapiAsync (fun i wrapped -> async {
                    let chunk = SerializedForChunking.unwrap wrapped
                    do! Async.Sleep (300 * sleepMultiplier) // simulate processing delay
                    if debug then printfn "[Part][%d] Processed part of size %d characters" i chunk.Length

                    if debug then printfn "[Part][%d] Chunk content preview: %s" i chunk[0..50]

                    return chunk
                })
                |> AsyncSeq.toListAsync

            let reassembled = String.concat "" parts
            Expect.equal reassembled text "raw stream round-trip preserves full content"
            if debug then printfn "---"
        }

        // ---- JSON streaming ----
        // All chunks collected and reassembled before parsing — standard mode.

        testCaseAsync "toStream / fromStream - large file content round-trips" <| async {
            do! Async.Sleep (3000 * sleepMultiplier) // ensure timestamp differences are visible in logs
            let text = File.ReadAllText(fixturePath "plain-file.md")
            let dto = { FileName = "plain-file.md"; Content = text }

            let! chunks =
                dto
                |> SerializedForChunking.wrap
                |> SerializedForChunking.Dto.toStream TextPageDto.serialize
                |> AsyncSeq.mapiAsync (fun i chunk -> async {
                    do! Async.Sleep (300 * sleepMultiplier) // simulate network delay
                    if debug then printfn "[Chunk][%d] Received chunk of size %d bytes" i (chunk.CalculateSize())

                    let part =
                        chunk.Content.ToByteArray()
                        |> Encode.bytesToString

                    if debug then printfn "[Chunk][%d] Chunk content preview: %s" i part[0..50]

                    return chunk
                })
                |> AsyncSeq.toListAsync

            Expect.isGreaterThan chunks.Length 1 "large JSON payload should produce multiple chunks"

            let! result =
                chunks
                |> AsyncSeq.ofSeq
                |> AsyncSeq.mapiAsync (fun i chunk -> async {
                    do! Async.Sleep (300 * sleepMultiplier) // simulate processing delay
                    if debug then printfn "[Chunk][%d] Processing chunk of size %d bytes" i (chunk.CalculateSize())

                    let part =
                        chunk.Content.ToByteArray()
                        |> Encode.bytesToString

                    if debug then printfn "[Chunk][%d] Chunk content preview: %s" i part[0..50]

                    return chunk
                })
                |> SerializedForChunking.Dto.fromStream TextPageDto.parse

            Expect.equal (result |> Result.map SerializedForChunking.unwrap) (Ok dto) "JSON stream round-trip preserves content"
            if debug then printfn "---"
        }
    ]

//
// SerializedForChunking — binary file (image.jpg) streaming tests
//
// image.jpg is ~1.2 MB — already compressed (JPEG), so Gzip.toStream is expected
// to produce the same or more chunks than plain Dto.toStream.
// The key assertion is that the reassembled bytes exactly equal the original.
//

[<AutoOpen>]
module ImageFixture =
    open FSharp.Data

    // Binary content is base64-encoded by JsonSerializer (byte[] → JSON string).
    type ImageDto = { FileName: string; Data: byte[] }

    [<RequireQualifiedAccess>]
    module ImageDto =
        let serialize (dto: ImageDto) = dto |> Serialize.toJson

        type private DtoSchema = JsonProvider<"""{"file_name":"name","data":"base64=="}""">

        let parse (json: string) : Result<ImageDto, ContractError> = result {
            let! parsed =
                try DtoSchema.Parse json |> Ok
                with e -> Result.Error (ContractError.ofExn e)

            return {
                FileName = parsed.FileName
                Data     = parsed.Data |> Convert.FromBase64String
            }
        }

[<Tests>]
let serializedForChunkingImageTests =
    testList "SerializedForChunking - binary image" [

        // ---- Dto.toStream / Dto.fromStream ----
        // image.jpg is base64-encoded inside JSON — expect multiple chunks (>= 5 for 1.2 MB).

        testCaseAsync "Dto.toStream / fromStream - image.jpg round-trips" <| async {
            let bytes = File.ReadAllBytes(fixturePath "image.jpg")
            let dto = { FileName = "image.jpg"; Data = bytes }

            let! chunks =
                dto
                |> SerializedForChunking.wrap
                |> SerializedForChunking.Dto.toStream ImageDto.serialize
                |> AsyncSeq.toListAsync

            if debug then printfn "[Dto] image.jpg → %d chunks" chunks.Length
            Expect.isGreaterThan chunks.Length 1 "1.2 MB base64 JSON should produce multiple chunks"

            let! result =
                chunks
                |> AsyncSeq.ofSeq
                |> SerializedForChunking.Dto.fromStream ImageDto.parse

            let roundTripped = result |> Result.map SerializedForChunking.unwrap
            Expect.equal (roundTripped |> Result.map (fun d -> d.FileName)) (Ok "image.jpg") "filename preserved"
            Expect.equal (roundTripped |> Result.map (fun d -> d.Data)) (Ok bytes) "binary content preserved byte-for-byte"
        }

        // ---- Dto.Gzip.toStream / Dto.Gzip.fromStream ----
        // JPEG is already compressed — gzip will not reduce size meaningfully.
        // The test verifies correctness of the gzip path, not compression ratio.

        testCaseAsync "Dto.Gzip.toStream / fromStream - image.jpg round-trips" <| async {
            let bytes = File.ReadAllBytes(fixturePath "image.jpg")
            let dto = { FileName = "image.jpg"; Data = bytes }

            let! chunksPlain =
                dto
                |> SerializedForChunking.wrap
                |> SerializedForChunking.Dto.toStream ImageDto.serialize
                |> AsyncSeq.toListAsync

            let! chunksGzip =
                dto
                |> SerializedForChunking.wrap
                |> SerializedForChunking.Dto.Gzip.toStream ImageDto.serialize
                |> AsyncSeq.toListAsync

            if debug then printfn "[Gzip] image.jpg → %d chunks (plain: %d) — JPEG is pre-compressed, ratio near 1x" chunksGzip.Length chunksPlain.Length

            let! result =
                chunksGzip
                |> AsyncSeq.ofSeq
                |> SerializedForChunking.Dto.Gzip.fromStream ImageDto.parse

            let roundTripped = result |> Result.map SerializedForChunking.unwrap
            Expect.equal (roundTripped |> Result.map (fun d -> d.FileName)) (Ok "image.jpg") "filename preserved"
            Expect.equal (roundTripped |> Result.map (fun d -> d.Data)) (Ok bytes) "binary content preserved byte-for-byte after gzip round-trip"
        }
    ]
