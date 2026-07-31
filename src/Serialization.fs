namespace Feather.Grpc

/// Marks content that is serialized at the domain boundary into raw bytes before being
/// transported as gRPC stream chunks. gRPC sees only opaque bytes; deserialization
/// happens at the receiving domain boundary.
type SerializedForChunking<'Dto> = SerializedForChunking of 'Dto

type ParseDto<'Dto> = string -> Result<'Dto, ContractError>
type SerializeDto<'Dto> = 'Dto -> string

[<RequireQualifiedAccess>]
module SerializedForChunking =
    open FSharp.Control
    open Feather.ErrorHandling
    open Feather.Cryptography
    open Feather.Grpc

    let wrap (dto: 'Dto): SerializedForChunking<'Dto> =
        SerializedForChunking dto

    let unwrap (SerializedForChunking dto: SerializedForChunking<'Dto>): 'Dto =
        dto

    let map f (SerializedForChunking dto) =
        SerializedForChunking (f dto)

    /// Extracts and validates raw bytes from a single proto chunk.
    /// Returns an error if the chunk is null or empty.
    let ofContract (contract: Feather.Contracts.Core.V1.SerializedForChunking): Result<byte[], ContractError> =
        if isNull contract then
            Error (ContractError.ofError "SerializedForChunking: contract is null")
        else
            Ok <| contract.Content.ToByteArray()

    /// Extracts raw bytes from a proto chunk without validation.
    /// Use only where the chunk is known to be non-empty (e.g. just produced by asContract).
    let bytesOf (contract: Feather.Contracts.Core.V1.SerializedForChunking): byte[] =
        contract.Content.ToByteArray()

    /// Wraps raw bytes into a single proto chunk.
    let ofBytes (bytes: byte[]) : Feather.Contracts.Core.V1.SerializedForChunking =
        Feather.Contracts.Core.V1.SerializedForChunking(
            Content = Google.Protobuf.ByteString.CopyFrom bytes
        )

    let asContract (SerializedForChunking bytes) : Feather.Contracts.Core.V1.SerializedForChunking =
        ofBytes bytes

    [<RequireQualifiedAccess>]
    module private Serialize =
        let toBytes (serialize: SerializeDto<'Dto>) (SerializedForChunking dto) : byte[] =
            dto
            |> serialize
            |> Encode.stringToBytes

    [<RequireQualifiedAccess>]
    module private Parse =
        let fromBytes (parse: ParseDto<'Dto>) (bytes: byte[]) : Result<SerializedForChunking<'Dto>, ContractError> =
            try
                match bytes with
                | null | [||] -> Error (ContractError.ofError "No content")
                | bytes ->
                    bytes
                    |> Encode.bytesToString
                    |> parse
                    |> Result.map SerializedForChunking
            with e ->
                Error (ContractError.ofExn e)

    [<RequireQualifiedAccess>]
    module private Chunk =
        let bytes (bytes: byte[]) =
            bytes
            |> Array.chunkBySize Grpc.DefaultChunkSize
            |> Array.toList
            |> List.map ofBytes

        let concat (chunks: Feather.Contracts.Core.V1.SerializedForChunking list): Result<byte[], ContractError> =
            chunks
            |> List.map ofContract
            |> Result.sequence
            |> Result.map (List.toArray >> Array.concat)

    [<AutoOpen>]
    module internal Chunks =
        /// Serializes a dto to JSON via `serialize` (typically `toDto >> Serialize.toJson`),
        /// encodes as UTF-8, and splits into 256 KB proto chunks ready to be streamed.
        ///
        /// Usage:
        ///   dto
        ///   |> SerializedForChunking.toChunks (MyType.toDto >> Serialize.toJson)
        ///   |> Grpc.writeAll ctx writer
        let toChunks (serialize: SerializeDto<'Dto>) (dto: SerializedForChunking<'Dto>): Feather.Contracts.Core.V1.SerializedForChunking list =
            dto
            |> Serialize.toBytes serialize
            |> Chunk.bytes

        /// Reassembles byte chunks, decodes UTF-8 JSON, and parses back into a dto via `parse`.
        ///
        /// Usage:
        ///   receivedChunks
        ///   |> List.map SerializedForChunking.ofContract
        ///   |> SerializedForChunking.fromChunks MyType.parse
        let fromChunks (parse: ParseDto<'Dto>) (chunks: Feather.Contracts.Core.V1.SerializedForChunking list): Result<SerializedForChunking<'Dto>, ContractError> =
            chunks
            |> Chunk.concat
            |> Result.bind (Parse.fromBytes parse)

    [<RequireQualifiedAccess>]
    module Dto =
        type private ToStream<'Dto> = SerializeDto<'Dto> -> SerializedForChunking<'Dto> -> AsyncSeq<Feather.Contracts.Core.V1.SerializedForChunking>
        type private FromStream<'Dto> = ParseDto<'Dto> -> AsyncSeq<Feather.Contracts.Core.V1.SerializedForChunking> -> AsyncResult<SerializedForChunking<'Dto>, ContractError>

        /// Serializes a dto and produces an AsyncSeq of proto chunks ready to be streamed via gRPC.
        /// Usage:
        ///   dto
        ///   |> SerializedForChunking.toStream (MyType.toDto >> Serialize.toJson)
        ///   |> Grpc.writeAll ctx writer
        let toStream (serialize: SerializeDto<'Dto>) dto: AsyncSeq<Feather.Contracts.Core.V1.SerializedForChunking> =
            dto
            |> Serialize.toBytes serialize
            |> Chunk.bytes
            |> AsyncSeq.ofSeq

        /// Collects all chunks from a gRPC stream, reassembles and parses them in one step.
        ///
        /// Usage:
        ///   reader
        ///   |> Grpc.readAll ctx
        ///   |> SerializedForChunking.fromStream MyType.parse
        let fromStream: FromStream<'Dto> = fun parse stream -> asyncResult {
            let! chunks =
                stream
                |> AsyncSeq.toListAsync
                |> AsyncResult.ofAsyncCatch ContractError.ofExn

            let! bytes = chunks |> Chunk.concat
            return! bytes |> Parse.fromBytes parse
        }

        [<RequireQualifiedAccess>]
        module Gzip =
            /// Serializes a dto to JSON, gzip-compresses the full payload, then streams the
            /// compressed bytes as 256 KB proto chunks.
            /// Use for large or compressible DTOs (DNA sequences, document content, binary blobs).
            ///
            /// Pair with fromStreamGzip on the receiving end.
            let toStream: ToStream<'Dto> = fun serialize dto ->
                dto
                |> Serialize.toBytes serialize
                |> Gzip.compress
                |> Chunk.bytes
                |> AsyncSeq.ofSeq

            let fromChunks (parse: ParseDto<'Dto>) chunks = asyncResult {
                let! bytes = chunks |> Chunk.concat

                return! bytes |> Gzip.decompress |> Parse.fromBytes parse
            }

            /// Collects all chunks, reassembles, gunzips, then parses the decompressed JSON.
            ///
            /// Pair with toStreamGzip on the sending end.
            let fromStream: FromStream<'Dto> = fun parse stream -> asyncResult {
                let! chunks =
                    stream
                    |> AsyncSeq.toListAsync
                    |> AsyncResult.ofAsyncCatch ContractError.ofExn

                return! fromChunks parse chunks
            }

    [<RequireQualifiedAccess>]
    module Parts =
        type private ToStream<'Parts> = SerializedForChunking<'Parts> -> AsyncSeq<Feather.Contracts.Core.V1.SerializedForChunking>
        type private FromStream<'Parts> = AsyncSeq<Feather.Contracts.Core.V1.SerializedForChunking> -> AsyncSeq<SerializedForChunking<'Parts>>

        /// Splits raw bytes into proto chunks and streams them.
        /// Usage:
        ///   bytes
        ///   |> SerializedForChunking.toRawStream
        ///   |> Grpc.writeAll ctx writer
        let toStream: ToStream<byte[]> = fun (SerializedForChunking bytes) ->
            bytes
            |> Chunk.bytes
            |> AsyncSeq.ofSeq

        /// Yields each chunk's raw bytes as they arrive — no reassembly.
        /// Usage:
        ///   reader
        ///   |> Grpc.readAll ctx
        ///   |> SerializedForChunking.fromRawStream
        ///   |> AsyncSeq.iter processChunk
        let fromStream: FromStream<byte[]> = fun stream ->
            stream |> AsyncSeq.map (bytesOf >> SerializedForChunking)

        [<RequireQualifiedAccess>]
        module Text =
            /// UTF-8 encodes a string, splits into proto chunks and streams them.
            /// Convenience wrapper over toStream for plain-text content.
            let toStream: ToStream<string> =
                map Encode.stringToBytes >> toStream

            /// Yields each chunk decoded as a UTF-8 string as it arrives.
            /// Convenience wrapper over fromRawStream for plain-text content.
            let fromStream: FromStream<string> =
                fromStream >> AsyncSeq.map (map Encode.bytesToString)
