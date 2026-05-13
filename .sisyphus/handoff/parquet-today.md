# Parquet File Version Metadata: Current (Stub) Behavior

## 1. UPLOAD FLOW

**Entry:** `RemoteStoreRefreshListener.syncSegments()` (line 223) gets `CatalogSnapshot` via `indexShard.getCatalogSnapshot()` (IndexShard:6090).

**Metadata upload:** `RemoteStoreRefreshListener.uploadMetadata()` (line 479) calls `remoteDirectory.uploadMetadata(localSegmentsPostRefresh, catalogSnapshot, ...)` (line 496).

**Version stamping:** `RemoteSegmentStoreDirectory.uploadMetadata()` (line 877):
```java
metadata.setWrittenByMajor(catalogSnapshot.getFormatVersionForFile(metadata.originalFilename).major);
```
For `DataformatAwareCatalogSnapshot`, `getFormatVersionForFile()` (line 241) returns `Version.LATEST` unconditionally → `writtenByMajor = 10`.

**On-disk format:** `UploadedSegmentMetadata.toString()` (line 362) serializes as:
```
_parquet_file_generation_1.parquet::uploaded_name::checksum::length::10
```
Stored in the remote metadata file via `RemoteSegmentMetadata.write()` (RemoteSegmentMetadata:119).

## 2. DOWNLOAD / REPLICA FLOW

**Parsing:** `RemoteSegmentMetadata.fromMapOfStrings()` (line 103) → `UploadedSegmentMetadata.fromString()` (line 380) → `setWrittenByMajor(10)` validates `10 <= LATEST.major && 10 >= MIN_SUPPORTED_MAJOR` (line 400). Passes.

**getCheckpointMetadata:** `RemoteStoreReplicationSource.getCheckpointMetadata()` (line 66):
- Gets `version = catalogSnapshotRef.get().getCommitDataFormatVersion()` (line 75) → `Version.LATEST` (10.x.y).
- Stamps **every** file (including parquet) with this version in `StoreFileMetadata` (line 110-116):
  ```java
  new StoreFileMetadata(e.getValue().getOriginalFilename(), length, checksum, version, null)
  ```
- **Note:** This path does NOT use per-file `writtenByMajor` from the metadata — it uses a single `getCommitDataFormatVersion()` for ALL files.

**Diff on replica:** `AbstractSegmentReplicationTarget` (line 229) calls `Store.segmentReplicationDiff()` (Store:497) which compares only by **checksum** (Store:509). `writtenBy` is NOT compared. No breakage for parquet.

## 3. Store.loadMetadata (CatalogSnapshot overload)

**Location:** `Store.java:1335-1365`

**Flow for DFA shard:**
1. `maxVersion = catalogSnapshot.getMinSegmentFormatVersion()` → **null** (stub).
2. Iterates all files including parquet: `catalogSnapshot.getFormatVersionForFile(file)` → `Version.LATEST` for each.
3. `maxVersion` updated to `Version.LATEST` (since null < anything).
4. Each file gets `StoreFileMetadata(file, length, checksum, Version.LATEST)` via `checksumFromFile()` (Store:1468).
5. segments_N file (if present) also stamped with `maxVersion = LATEST`.

**Downstream:** `MetadataSnapshot` constructed (Store:1181). Used for segment replication diff (checksum-only) and snapshot persistence.

## 4. RemoteStoreReplicationSource.getCheckpointMetadata

**File:** `RemoteStoreReplicationSource.java:66-120`

**Version source:** `catalogSnapshotRef.get().getCommitDataFormatVersion()` (line 75) = `Version.LATEST`.

**Applied to parquet?** YES — line 108-116 iterates ALL entries from `mdFile.getMetadata()` (which includes parquet files) and stamps each with the single `version` value. The per-file `writtenByMajor` from `UploadedSegmentMetadata` is **ignored** on this path.

## 5. CURRENT LIE: Fields Carrying "Lucene 10" for Parquet Files

| Location | Field | Value | Benign? |
|----------|-------|-------|---------|
| Remote metadata file (on-disk) | `UploadedSegmentMetadata.writtenByMajor` | 10 | **Benign** — only validated in range `[MIN_SUPPORTED_MAJOR, LATEST.major]` on parse (RemoteSegmentStoreDirectory:400). Never used for format-specific logic. |
| `StoreFileMetadata.writtenBy` (via `Store.loadMetadata`) | `Version.LATEST` (10.x.y) | **Benign** — used in `isSame()` only for checksum comparison (StoreFileMetadata:157). Asserted non-null (Store:725,748) but never version-compared. |
| `StoreFileMetadata.writtenBy` (via `getCheckpointMetadata`) | `Version.LATEST` (10.x.y) | **Benign** — same as above; replica diff uses checksum only (Store:509). |
| `FileChunkRequest` wire format | `metadata.writtenBy().toString()` | "10.x.y" | **Benign** — serialized/deserialized (FileChunkRequest:142/72) but receiver only stores it in `StoreFileMetadata`; no version gate. |
| `BlobStoreIndexShardSnapshot` (snapshot JSON) | `WRITTEN_BY` field | "10.x.y" | **Benign** — persisted in snapshot metadata (BlobStoreIndexShardSnapshot:278). Used during restore to construct `StoreFileMetadata` but no version-gated logic. |

## Summary

**No active breakage.** The "Lucene 10" stamp on parquet files is a **label-only lie** today:
- `writtenByMajor` is validated to be in `[MIN_SUPPORTED_MAJOR, LATEST.major]` — passes trivially since we set it to LATEST.
- `StoreFileMetadata.writtenBy` is never compared for version compatibility decisions.
- `segmentReplicationDiff` uses checksum only.
- No code path gates download/recovery of a file based on its `writtenBy` version.

**Risk:** If future code adds version-gated logic (e.g., "skip files written by incompatible Lucene"), parquet files stamped as "Lucene 10" would be incorrectly included/excluded. The redesign should assign a format-specific version (e.g., Parquet 1.x) to avoid this latent hazard.
