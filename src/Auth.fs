namespace Feather.Grpc

open Grpc.Core
open Feather.ErrorHandling

type AuthError =
    | Unauthenticated of string
    | Unauthorized of string

type AuthInterceptor<'Context> = (ServerCallContext -> Result<'Context, AuthError>)
type AsyncAuthInterceptor<'Context> = (ServerCallContext -> AsyncResult<'Context, AuthError>)

[<RequireQualifiedAccess>]
module AuthInterceptor =
    let private raiseOnError = function
        | Ok ctx -> ctx
        | Error (Unauthenticated msg) -> raise (RpcException(Status(StatusCode.Unauthenticated, msg)))
        | Error (Unauthorized msg) -> raise (RpcException(Status(StatusCode.PermissionDenied, msg)))

    let validate (authInterceptor: AuthInterceptor<'Context>) =
        authInterceptor >> raiseOnError

    let validateAsync (authInterceptor: AsyncAuthInterceptor<'Context>) context =
        async {
            let! result = authInterceptor context
            return raiseOnError result
        }
