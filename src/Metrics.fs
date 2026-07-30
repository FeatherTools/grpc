namespace Feather.Grpc

module Metrics =
    open Alma.ServiceIdentification
    open Alma.WebApplication

    type GrpcMetrics = {
        IncrementGrpcErrorCount: Spot option -> GrpcError -> unit
    }

    [<RequireQualifiedAccess>]
    module GrpcMetrics =
        open Alma.Metrics

        type private Count = Count of int

        type private ApplicationMetric =
            | GrpcErrorOccurred of Instance * Spot option * GrpcError

        [<AutoOpen>]
        module private InternalState =
            let private createGrpcErrorOccurred instance spot (grpcError: GrpcError) =
                SimpleDataSetKeys [
                    "error", grpcError.Name
                ]
                |> Metrics.createDataSetKey instance spot

            let metricGrpcErrorOccurred = "grpc_error_occurred" |> MetricName.createOrFail

            let private metricValueToCount = function
                | Int int -> Count int
                | _ -> Count 0

            let incrementState = function
                | GrpcErrorOccurred (instance, spot, grpcError) ->
                    grpcError
                    |> createGrpcErrorOccurred instance spot
                    |> State.incrementMetricSetValue (Int 1) metricGrpcErrorOccurred
                    |> metricValueToCount

        let currentState() =
            [
                metricGrpcErrorOccurred |> Metrics.Format.counter "Grpc error occurred count."
            ]

        // Changing state

        let incrementGrpcErrorOccurred instance spot grpcError =
            (instance, spot, grpcError)
            |> GrpcErrorOccurred
            |> incrementState
            |> ignore

        let metrics (currentInstance: Instance): GrpcMetrics = {
            IncrementGrpcErrorCount = incrementGrpcErrorOccurred currentInstance
        }
