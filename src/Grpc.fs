namespace Feather.Grpc

open Grpc.Core

[<RequireQualifiedAccess>]
module Grpc =
    open System
    open System.Net.Http
    open Alma.WebApplication
    open Grpc.Net.Client

    let [<Literal>] Port = 9090

    let jwtAuthHeaders (Alma.Authorization.Common.JWT token) =
        let m = Metadata()
        m.Add("authorization", sprintf "Bearer %s" token)
        m

    let channelWithHttpClient httpClient grpcPort url =
        let grpcUrl url = sprintf "%s:%d" url grpcPort |> Uri
        let serviceGrpcUrl = url |> grpcUrl

        let httpClient = httpClient |> Option.defaultWith (fun () -> new HttpClient(new HttpClientHandler()))
        httpClient.BaseAddress <- serviceGrpcUrl

        GrpcChannel.ForAddress(serviceGrpcUrl, GrpcChannelOptions(HttpClient = httpClient))

    let private grpcChannel grpcPort url =
        let grpcUrl url = sprintf "%s:%d" url grpcPort |> Uri
        let serviceGrpcUrl = url |> grpcUrl

        GrpcChannel.ForAddress(serviceGrpcUrl)

    let k8sSvcChannel grpcPort serviceInstance =
        serviceInstance
        |> Instance.k8sLocalServiceUrl
        |> grpcChannel grpcPort

    let localChannel grpcPort =
        "http://localhost"
        |> grpcChannel grpcPort

    let localIpChannel grpcPort =
        "http://127.0.0.1"
        |> grpcChannel grpcPort

    open FSharp.Control

    [<Literal>]
    let DefaultChunkSize = 256 * 1024  // 256 KB

    // -- AsyncSeq conversions (pure, no context) --

    /// IAsyncStreamReader → AsyncSeq (requires explicit cancellation token)
    let toAsyncSeq (ct: System.Threading.CancellationToken) (reader: IAsyncStreamReader<'a>): AsyncSeq<'a> =
        reader.ReadAllAsync ct

    /// AsyncSeq → write to IServerStreamWriter (requires explicit cancellation token)
    let ofAsyncSeq (ct: System.Threading.CancellationToken) (writer: IServerStreamWriter<'a>) (items: AsyncSeq<'a>): Async<unit> =
        items
        |> AsyncSeq.iterAsync (fun item ->
            writer.WriteAsync(item, ct) |> Async.AwaitTask
        )

    // -- Context-aware helpers (wrap the above, enforcing cancellation via ServerCallContext) --

    /// Read all messages from a gRPC server stream into an AsyncSeq.
    /// Cancellation should be driven by the ServerCallContext so callers can't forget the token.
    let readAll cancellation (reader: IAsyncStreamReader<'a>): AsyncSeq<'a> =
        reader |> toAsyncSeq cancellation

    /// Write every item in an AsyncSeq to a gRPC server writer.
    /// Cancellation should be driven by the ServerCallContext.
    let writeAll cancellation (writer: IServerStreamWriter<'a>) (items: AsyncSeq<'a>): Async<unit> =
        items |> ofAsyncSeq cancellation writer

    let sendAsyncSeq (ct: System.Threading.CancellationToken) (writer: IClientStreamWriter<'a>) (items: AsyncSeq<'a>): Async<unit> =
        items
        |> AsyncSeq.iterAsync (fun item ->
            writer.WriteAsync(item, ct) |> Async.AwaitTask
        )

    let sendAll cancellation (writer: IClientStreamWriter<'a>) (items: AsyncSeq<'a>): Async<unit> =
        items |> sendAsyncSeq cancellation writer
