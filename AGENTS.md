# AGENTS.md

Guidance for AI coding agents working in this repository.

## What this is

`Feather.Grpc` is an F# library published to NuGet (`Feather.Grpc`) providing low- and
high-level helpers for building gRPC clients and servers. It targets **.NET 10** and
sits on top of `Grpc.Net.Client` / `Grpc.Core.Api`.

The code was extracted from a larger internal service and is now a standalone public
package. It is opinionated toward the `Feather.*` / `Alma.*` ecosystem (contracts,
error handling, cryptography, service identification).

## Layout

Everything compiles from `Grpc.fsproj`; compile order matters in F#, so keep the
`<Compile>` order in the `.fsproj` consistent with dependencies between files.

- `src/Utils.fs` — internal helpers (`Guid`, `DateTimeOffset`, `Gzip`, `Async`).
- `src/Grpc.fs` — low-level channel creation and `AsyncSeq` ⟷ gRPC stream conversions.
- `src/Error.fs` — `ContractError`, `GrpcError`, and computation-expression extensions.
- `src/CoreTypes.fs` — domain types and contract conversions (`Timestamp`, `CorrelationId`, `Spot`, `Instance`, `Box`).
- `src/Auth.fs` — `AuthInterceptor` / `AsyncAuthInterceptor` for server-side auth.
- `src/Metrics.fs` — `GrpcMetrics` error counters.
- `src/Serialization.fs` — `SerializedForChunking`: serialize + chunk large payloads for streaming (plain, gzip, raw parts, text).
- `src/HighLevel.fs` — high-level `Read` / `Send` / `Duplex` / `Response` orchestration built on the modules above.
- `tests/` — Expecto test suite mirroring the module structure.

## Build & test

Use the build script (restores tools + Paket, then runs the FAKE targets):

```bash
./build.sh build        # build the library
./build.sh -t tests     # run the Expecto test suite
```

Dependencies are managed with **Paket**, not raw `PackageReference`. To change
dependencies edit `paket.dependencies` + `paket.references`, then let the build
script restore. Do not hand-edit lock files.

## Conventions

- **No abbreviations** in code or domain names. Use `Language`, not `Lang`; `Latitude`,
  not `Lat`. The only exception is the proper name of an adopted standard, which is
  written FULLY UPPERCASE (e.g. `JWT`, `GERSId`).
- Prefer established open standards for data representation (ISO 8601 for time, etc.)
  so data migrates between tools without translation.
- Follow the existing functional style: `Result` / `AsyncResult` for errors,
  `[<RequireQualifiedAccess>]` modules, `AsyncSeq` for streaming. Avoid exceptions for
  control flow — convert to `GrpcError` / `ContractError` at boundaries.
- Keep the contract boundary explicit: `ofContract` / `asContract` pairs convert between
  `Feather.Contracts.Core.V1.*` proto types and domain types.
- Lint config lives in `fsharplint.json`.

## Release

Releasing is manual (see `README.md`):

1. Bump `<Version>` in `Grpc.fsproj`.
2. Add an entry to `CHANGELOG.md` (there is always an `Unreleased` section at the top;
   use `Add` / `Changed` / `Fix` / `Removed` subsections). Mark breaking changes with `[**BC**]`.
3. Commit the new version and tag it.

## When editing

- Read the target file and its neighbours first; F# is order-sensitive.
- Add or update tests in `tests/` alongside behavioural changes.
- Only make the change requested — no unsolicited refactors or extra files.
- Do not add a `.gitignore` or other scaffolding unless explicitly asked.
