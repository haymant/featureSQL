# Storage Abstraction Design

## Objective
Abstract file system operations to support creating, reading, updating, and deleting (CRUD) files across various storage backends.

> **Note:** the feature database built on top of this storage abstraction
> only supports numeric (float/float) columns for feature values.  Text or
> other data types are not persisted.  The time index is represented by an
> implicit `date` column which is always stored as a `TIMESTAMP` and
> enforced as a **NOT NULL PRIMARY KEY** by the DuckDB engine; every
> `INSERT` must supply a date and duplicate dates are rejected.
>
> DDL is intentionally very limited: the SQL layer supports `CREATE`,
> `DROP` and `ALTER TABLE` for adding/dropping numeric columns.  When a
> column is added a zero‑filled `.day.bin` file is created with a length
> that matches the rest of the table; dropping a column simply removes the
> corresponding file.

## Supported Storage Types (Enum: `StoreType`)
1. `fs`: Local File System (Default)
2. `gcs`: Google Cloud Storage
3. `s3`: AWS S3 (Planned)
4. `vb`: Vercel Blob (Planned)

## Interface Design: `StorageBackend`
A base abstract class `StorageBackend` will define the primary operations:
- `exists(path: str) -> bool`: Check if a file or directory exists.
- `mkdir(path: str, parents: bool, exist_ok: bool)`: Create directory (no-op for object storage).
- `glob(path: str, pattern: str) -> List[str]`: Search for files matching a pattern.
- `read_text(path: str) -> str`: Read text content.
- `write_text(path: str, text: str)`: Write text content.
- `read_bytes(path: str) -> bytes`: Read binary content.
- `write_bytes(path: str, data: bytes)`: Write binary content.
- `write_dataframe(path: str, df: pd.DataFrame)`: Write a pandas DataFrame to CSV.
- `read_dataframe(path: str) -> pd.DataFrame`: Read a CSV into a pandas DataFrame.
- `joinpath(*args) -> str`: Safely join path segments.

### Implementation Details
- **`fs` (FileSystemStore)**: Wraps `pathlib.Path` and standard `os` / `pandas` methods.
- **`gcs` (GCSStore)**: 
  - Authentication may come from either a service‑account JSON (via the
    `GCS_SC_JSON` environment variable) or from an HMAC key pair
    (`GCS_KEY_ID`/`GCS_KEY_SECRET`).  The latter uses `gcsfs` under the
    hood and is chosen whenever both key variables are present.
  - Authenticates via `$GCS_SC_JSON` environment variable containing service account credentials.
  - Treats `--data_path` as the GCS `bucket_name` or `bucket_name/prefix`.
  - Uses `google.cloud.storage.Client` to perform bucket and blob operations (e.g. `blob.upload_from_string`, `blob.download_as_bytes`, `client.list_blobs(prefix=...)`).
  - To support DataFrame operations, `write_dataframe` will convert the DataFrame to a CSV string in memory and upload it, and `read_dataframe` will download bytes to a `io.BytesIO` buffer and read via `pd.read_csv()`.
- **`s3` / `vb`**: To be implemented in the future, throwing `NotImplementedError` for now.

## Changes to CLI & Core Code
- Update `featureSQL.cli.Run` methods (`download`, `dump_all`, `query`, etc.) to accept a `--store-type` parameter.
- Pass the `StoreType` down or initialize the corresponding `StorageBackend` inside CLI and pass it to logic classes (`YahooCollectorUS`, `DumpData*`, `DuckQueryService` etc.).
- Replace `Path` usage with the `StorageBackend` instance.

## Unit Test Plan
- **Test FileSystemStore**: Ensure basic CRUD, glob, and dataframe read/writes map to local filesystem correctly.
- **Test GCSStore (Mocked)**: Mock `os.getenv` and `google.cloud.storage` to verify GCS credential loading, bucket interactions, and `upload_from_string` for expected blob names.
- **Test CLI Parameters**: Ensure `--store-type` properly initializes the correct backend.
- **Test Fallback**: Ensure specifying 's3' or 'vb' raises a `NotImplementedError`.
