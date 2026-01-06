# Sustainalytics WASM FDW (Supabase Wrappers)

This project is a Supabase **WASM Foreign Data Wrapper (FDW)** that wraps the Sustainalytics API:
- `POST /auth/token` to get a bearer token
- `GET /v2/DataService` (exposed as `endpoint='DataServices'`) with pagination using `Skip` + `Take`
- `GET /v2/FieldMappingDefinitions` (exposed as `endpoint='FieldMappingDefinitions'`) flattened into rows

## Supported foreign table options

### DataServices
Table options (case-sensitive):
- `endpoint = 'DataServices'` (**required**)
- `ProductId` (**required**)
- `PackageIds` (optional, comma-separated)
- `FieldClusterIds` (optional, comma-separated)
- `FieldIds` (optional, comma-separated)
- `Take` (optional) — defaults to 10, and clamps to <= 10 (or uses <10 if provided, per your requested rule)

Notes:
- `Skip` is managed internally by the FDW, starting at 0 and increasing by `Take`.

Columns for DataServices tables:
- `entityId text`
- `entityName text`
- `fields jsonb`

### FieldMappingDefinitions
Table options:
- `endpoint = 'FieldMappingDefinitions'` (**required**)

Columns (flattened):
- `product_id text`
- `product_name text`
- `package_id bigint`
- `package_name text`
- `field_cluster_id bigint`
- `field_cluster_name text`
- `field_id bigint`
- `field_name text`
- `description text`
- `field_type text`
- `field_length text`
- `possible_values text`
- `grouping text`
- `parentage jsonb`

## Server options

Server options:
- `base_url` (default `https://api.sustainalytics.com`)
- `client_id` (**required**)
- `client_secret` (**required**)


## Build

```bash
cargo install cargo-component
rustup target add wasm32-unknown-unknown
cargo component build --release --target wasm32-unknown-unknown
```

## Release

This repo includes a GitHub Actions workflow that builds the WASM on tagged releases (`v*.*.*`) and
creates a GitHub Release with the `.wasm` asset, printing the SHA256 in logs.
