namespace Feather.Grpc

[<AutoOpen>]
module internal Utils =
    open System

    [<RequireQualifiedAccess>]
    module Guid =
        let tryParse = function
            | null | "" -> None
            | str ->
                match Guid.TryParse str with
                | true, guid -> Some guid
                | false, _ -> None

    [<RequireQualifiedAccess>]
    module DateTimeOffset =
        let tryParse (s: string) =
            match DateTimeOffset.TryParse(s) with
            | true, dt -> Some dt
            | false, _ -> None

        let serialize (dt: DateTimeOffset) =
            dt.ToString("o") // ISO 8601 format

    [<RequireQualifiedAccess>]
    module Gzip =
        open System.IO
        open System.IO.Compression

        let compress (input: byte[]) : byte[] =
            use outputStream = new MemoryStream()
            use gzipStream = new GZipStream(outputStream, CompressionMode.Compress)
            gzipStream.Write(input, 0, input.Length)
            gzipStream.Close()
            outputStream.ToArray()

        let decompress (input: byte[]) : byte[] =
            use inputStream = new MemoryStream(input)
            use gzipStream = new GZipStream(inputStream, CompressionMode.Decompress)
            use outputStream = new MemoryStream()
            gzipStream.CopyTo(outputStream)
            outputStream.ToArray()

    [<RequireQualifiedAccess>]
    module Async =
        let startWithCancellation cancellation xA =
            Async.Start(xA, cancellation)
