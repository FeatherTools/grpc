namespace Feather.Grpc

module HighLevel =
    open FSharp.Control
    open Grpc.Core
    open Feather.ErrorHandling
    open Feather.Grpc

    module Response =
        open Microsoft.Extensions.Logging
        open Metrics

        let handle grpcMetrics (action: string) (logger: ILogger) spot (responseSuccess: 'Success -> 'Response) responseError (operation: AsyncResult<'Success, GrpcError>) = task {
            logger.LogDebug $"Processing {action} operation"
            match! operation with
            | Ok success -> return responseSuccess success
            | Error error ->
                logger.LogError("{action} operation failed, {error}", action, error)
                error |> grpcMetrics.IncrementGrpcErrorCount spot

                return responseError (error |> GrpcError.asContract)
        }

        let handleDuplex grpcMetrics (action: string) (logger: ILogger) spot responseError (writer: IServerStreamWriter<'Response>) operation = task {
            logger.LogDebug $"Processing {action} operation"
            match! operation with
            | Ok () -> return ()
            | Error error ->
                logger.LogError("{action} failed, {error}", action, error)
                error |> grpcMetrics.IncrementGrpcErrorCount spot

                do! writer.WriteAsync(responseError (error |> GrpcError.asContract))
        }

    module Read =
        module Unary =
            let value = AsyncResult.ofAsyncUnaryResponse

        module Stream =
            module AsyncSeq =
                let value fromStream (f: 'Request -> 'Chunk) (toDomain: 'Dto -> Result<'Value, ContractError>) (stream: AsyncSeq<'Request>) = asyncResult {
                    let! input =
                        stream
                        |> AsyncSeq.map f
                        |> fromStream
                        |> AsyncResult.mapError GrpcError.ofContractError

                    return!
                        input
                        |> SerializedForChunking.unwrap
                        |> toDomain
                        |> Result.mapError GrpcError.ofContractError
                }

                let valueAndChunks fromStream (f: 'Request -> 'Chunk) (toDomain: 'Dto -> Result<'Value, ContractError>) (stream: AsyncSeq<'Request>) = asyncResult {
                    let! input =
                        stream
                        |> AsyncSeq.map f
                        |> fromStream
                        |> AsyncResult.mapError GrpcError.ofContractError

                    let! value =
                        input
                        |> SerializedForChunking.unwrap
                        |> toDomain
                        |> Result.mapError GrpcError.ofContractError

                    return input, value
                }

            module Request =
                let asSeq cancellation (request: IAsyncStreamReader<'Request>): AsyncSeq<'Request> =
                    request |> Grpc.readAll cancellation

                let firstAndContinue cancellation (request: IAsyncStreamReader<'Request>) = asyncResult {
                    let! hasFirst =
                        request.MoveNext cancellation
                        |> AsyncResult.ofTaskCatch GrpcError.ofExn

                    if not hasFirst then
                        return! GrpcError.create "NoContent" (Some "No content received in SaveFileContent request") |> Error

                    let firstRequest = request.Current

                    return
                        firstRequest,
                        asyncSeq {
                            yield firstRequest
                            yield! request |> Grpc.toAsyncSeq cancellation
                        }
                }

                let dto cancellation fromStream (requestToChunk: 'Request -> 'Chunk) (toDomain: 'Dto -> Result<'Value, ContractError>) (request: IAsyncStreamReader<'Request>) =
                    request
                    |> asSeq cancellation
                    |> AsyncSeq.value fromStream requestToChunk toDomain

                let value cancellation fromStream (requestToChunk: 'Request -> 'Chunk) (request: IAsyncStreamReader<'Request>) =
                    dto cancellation fromStream requestToChunk Ok request

                let gzipValue cancellation (parse: string -> Result<'Value, ContractError>) (requestToChunk: 'Request -> _) =
                    value cancellation (SerializedForChunking.Dto.Gzip.fromStream parse) requestToChunk

                let dtoAndChunks cancellation fromStream (requestToChunk: 'Request -> 'Chunk) (toDomain: 'Dto -> Result<'Value, ContractError>) (request: IAsyncStreamReader<'Request>) =
                    request
                    |> asSeq cancellation
                    |> AsyncSeq.valueAndChunks fromStream requestToChunk toDomain

            module Response =
                let private chunksToValue fromChunks responseChunks = asyncResult {
                    let! chunks =
                        responseChunks
                        |> List.rev
                        |> Result.sequence

                    return!
                        chunks
                        |> fromChunks
                        |> AsyncResult.mapError GrpcError.ofContractError
                        |> AsyncResult.map SerializedForChunking.unwrap
                }

                let private ignoreValue responseChunks =
                    responseChunks
                    |> List.rev
                    |> Result.sequence
                    |> Result.map ignore

                let value cancellation (handleResponse: 'Response -> Result<'Chunk, GrpcError>) (fromChunks: 'Chunk list -> AsyncResult<SerializedForChunking<'Value>, ContractError>) (response: IAsyncStreamReader<'Response>) = asyncResult {
                    let! responseChunks =
                        response
                        |> Grpc.readAll cancellation
                        |> AsyncSeq.fold (fun acc response -> handleResponse response :: acc) []
                        |> AsyncResult.ofAsyncCatch GrpcError.ofExn

                    return! chunksToValue fromChunks responseChunks
                }

                let private handleAsync cancellation (handleResponse: 'Response -> AsyncResult<'Chunk, GrpcError>) (response: IAsyncStreamReader<'Response>) =
                    response
                    |> Grpc.readAll cancellation
                    |> AsyncSeq.foldAsync (fun acc response -> async {
                        let! chunkResult = handleResponse response

                        return chunkResult :: acc
                    }) []
                    |> AsyncResult.ofAsyncCatch GrpcError.ofExn

                let valueAsync cancellation (handleResponse: 'Response -> AsyncResult<'Chunk, GrpcError>) (fromChunks: 'Chunk list -> AsyncResult<SerializedForChunking<'Value>, ContractError>) (response: IAsyncStreamReader<'Response>) = asyncResult {
                    let! responseChunks = handleAsync cancellation handleResponse response
                    return! chunksToValue fromChunks responseChunks
                }

                let ignoreValueAsync cancellation (handleResponse: 'Response -> AsyncResult<'Chunk, GrpcError>) (response: IAsyncStreamReader<'Response>) = asyncResult {
                    let! responseChunks = handleAsync cancellation handleResponse response
                    return! ignoreValue responseChunks
                }

            module Call =
                // todo - is it needed?
                let firstAndContinue cancellation (call: AsyncServerStreamingCall<'Request>) =
                    Request.firstAndContinue cancellation call.ResponseStream

                let value cancellation (handleResponse: 'Response -> Result<'Chunk, GrpcError>) (fromChunks: 'Chunk list -> AsyncResult<SerializedForChunking<'Value>, ContractError>) (call: AsyncServerStreamingCall<'Response>) =
                    call.ResponseStream
                    |> Response.value cancellation handleResponse fromChunks

        module DuplexStream =
            module Chunks =
                let value cancellation (request: AsyncDuplexStreamingCall<'Request', 'Response>) (handleResponse: 'Response -> Result<'Chunk, GrpcError>) (fromChunks: 'Chunk list -> AsyncResult<SerializedForChunking<'Value>, ContractError>) =
                    request.ResponseStream
                    |> Stream.Response.value cancellation handleResponse fromChunks

                let valueAsync cancellation (request: AsyncDuplexStreamingCall<'Request', 'Response>) (handleResponse: 'Response -> AsyncResult<'Chunk, GrpcError>) (fromChunks: 'Chunk list -> AsyncResult<SerializedForChunking<'Value>, ContractError>) =
                    request.ResponseStream
                    |> Stream.Response.valueAsync cancellation handleResponse fromChunks

                let ignoreValueAsync cancellation (request: AsyncDuplexStreamingCall<'Request, 'Response>) (handleResponse: 'Response -> AsyncResult<'Chunk, GrpcError>) =
                    request.ResponseStream
                    |> Stream.Response.ignoreValueAsync cancellation handleResponse

    module Send =
        module Stream =
            let asyncSeq (writer: IAsyncStreamWriter<'Request>) (chunkToRequest: 'Chunk -> 'Request) dataStream =
                dataStream
                |> AsyncSeq.iterAsync (chunkToRequest >> writer.WriteAsync >> Async.AwaitTask)
                |> AsyncResult.ofAsyncCatch GrpcError.ofExn

        module ClientStream =
            let asyncSeq (request: AsyncClientStreamingCall<'Request, 'Response>) (chunkToRequest: 'Chunk -> 'Request) dataStream = asyncResult {
                do! dataStream |> Stream.asyncSeq request.RequestStream chunkToRequest
                do! request.RequestStream.CompleteAsync() |> AsyncResult.ofEmptyTaskCatch GrpcError.ofExn

                return! request.ResponseAsync |> AsyncResult.ofTaskCatch GrpcError.ofExn
            }

        module ServerStream =
            let dto cancellation (writer: IServerStreamWriter<'Response>) (dtoFromDomain: 'Value -> 'Dto) (toStream: SerializedForChunking<'Dto> -> AsyncSeq<'Chunk>) (chunkToRequest: 'Chunk -> 'Response) value =
                value
                |> dtoFromDomain
                |> SerializedForChunking.wrap
                |> toStream
                |> AsyncSeq.map chunkToRequest
                |> Grpc.writeAll cancellation writer
                |> AsyncResult.ofAsyncCatch GrpcError.ofExn

            let gzipDto cancellation (writer: IServerStreamWriter<'Response>) (dtoFromDomain: 'Value -> 'Dto) serialize chunkToRequest =
                dto cancellation writer dtoFromDomain (SerializedForChunking.Dto.Gzip.toStream serialize) chunkToRequest

            let value cancellation (writer: IServerStreamWriter<'Response>) (toStream: SerializedForChunking<'Value> -> AsyncSeq<'Chunk>) (chunkToRequest: 'Chunk -> 'Response) =
                dto cancellation writer id toStream chunkToRequest

            let gzipValue cancellation (writer: IServerStreamWriter<'Response>) (serialize: 'Value -> string) chunkToRequest =
                value cancellation writer (SerializedForChunking.Dto.Gzip.toStream serialize) chunkToRequest

        module DuplexStream =
            let asyncSeq (request: AsyncDuplexStreamingCall<'Request, 'Response>) (chunkToRequest: 'Chunk -> 'Request) dataStream = asyncResult {
                do! dataStream |> Stream.asyncSeq request.RequestStream chunkToRequest
                do! request.RequestStream.CompleteAsync() |> AsyncResult.ofEmptyTaskCatch GrpcError.ofExn
            }

            let dto (request: AsyncDuplexStreamingCall<'Request, 'Response>) (dtoFromDomain: 'Value -> 'Dto) toStream (chunkToRequest: 'Chunk -> 'Request) value =
                value
                |> dtoFromDomain
                |> SerializedForChunking.wrap
                |> toStream
                |> asyncSeq request chunkToRequest

    module Duplex =
        let private startImmediately cancellation logError seq =
            seq
            |> AsyncResult.teeError logError
            |> Async.Ignore
            |> Async.startWithCancellation cancellation

        let streamImmediately cancellation (request: AsyncDuplexStreamingCall<'Request, 'Response>) logError dtoFromDomain toStream chunkToRequest handleResponse fromChunks value = asyncResult {
            value
            |> Send.DuplexStream.dto request dtoFromDomain toStream chunkToRequest
            |> startImmediately cancellation logError

            return! Read.DuplexStream.Chunks.value cancellation request handleResponse fromChunks
        }

        let stream cancellation (request: AsyncDuplexStreamingCall<'Request, 'Response>) dtoFromDomain toStream chunkToRequest handleResponse fromChunks value = asyncResult {
            do! value |> Send.DuplexStream.dto request dtoFromDomain toStream chunkToRequest

            return! Read.DuplexStream.Chunks.value cancellation request handleResponse fromChunks
        }

        let streamValue cancellation (request: AsyncDuplexStreamingCall<'Request, 'Response>) toStream chunkToRequest handleResponse fromChunks value = asyncResult {
            do! value |> Send.DuplexStream.dto request id toStream chunkToRequest

            return! Read.DuplexStream.Chunks.value cancellation request handleResponse fromChunks
        }

        module AsyncSeq =
            let streamImmediately cancellation (request: AsyncDuplexStreamingCall<'Request, 'Response>) logError (chunkToRequest: 'Chunk -> 'Request) handleResponse (dataStream: AsyncSeq<'Chunk>) = asyncResult {
                dataStream
                |> Send.DuplexStream.asyncSeq request chunkToRequest
                |> startImmediately cancellation logError

                return! Read.DuplexStream.Chunks.ignoreValueAsync cancellation request handleResponse
            }
