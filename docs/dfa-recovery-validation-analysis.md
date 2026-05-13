# DFA Recovery & File Validation Analysis

Analysis of how Lucene file corruption is detected during recovery and what breaks for non-Lucene (DataFormat-Aware) files.

## 1. Validation mechanisms in the codebase

Two fundamentally different checksum strategies:

| Strategy | Reads | Cost | Used for |
|---|---|---|---|
| `CodecUtil.retrieveChecksum(input)` | Last 8 bytes (footer) | O(1) | Lucene files only |
| `CodecUtil.checksumEntireFile(input)` | Every byte | O(file size) | Lucene files only |
| `GenericCRC32ChecksumHandler.computeChecksum` | Every byte | O(file size) | DFA files (Parquet, etc.) |
| `LuceneChecksumHandler.computeChecksum` | Last 8 bytes | O(1) | Lucene files via `FormatChecksumStrategy` SPI |

`DataFormatAwareStoreDirectory.calculateUploadChecksum(name)` dispatches to the correct strategy based on the file's format prefix (`parquet/_0.parquet` → `GenericCRC32ChecksumHandler`).

## 2. Recovery flows and where validation happens

| # | Flow | Body-level CRC? | Where |
|---|---|---|---|
| 1 | Peer recovery target — every file chunk | Yes, inline | `MultiFileWriter.openAndPutIndexOutput` → `Store.createVerifyingOutput` → `LuceneVerifyingIndexOutput` (Store.java:1562) |
| 2 | Peer recovery source on error | Yes | `SegmentFileTransferHandler.handleErrorOnSendFiles:199` → `Store.checkIntegrityNoException` → `checksumEntireFile` |
| 3 | Startup `check_on_startup=checksum` | Yes | `IndexShard.doCheckIndex:4148` → `Store.checkIntegrity` |
| 4 | Startup `check_on_startup=true` | Yes, structural | `Store.checkIndex` → `org.apache.lucene.index.CheckIndex` |
| 5 | Snapshot restore from repository | Yes, inline | `BlobStoreRepository.restoreFile:4297` → `createVerifyingOutput` |
| 6 | Snapshot catalog blobs read | Yes | `ChecksumBlobStoreFormat.readBlobAndVerify:132` |
| 7 | Translog checkpoint read | Yes | `Checkpoint.read:206` |
| 8 | Remote metadata blob read | Yes | `VersionedCodecStreamWrapper.readStreamWithFooter:63` |
| 9 | Remote-store download (happy path) | **No** | `RemoteStoreFileDownloader.copyOneFile` streams bytes with no check |
| 10 | Post-download dedupe / skip | Footer-only for Lucene; full-body for DFA | `IndexShard.localDirectoryContains:5897` |
| 11 | Reader-time codec integrity checks | Yes, per codec | `Composite912DocValuesReader.checkIntegrity` etc. — only triggered by CheckIndex or merges |

## 3. End-to-end guarantee by recovery type

### Remote-store restart / restore

1. Primary upload: `RemoteStoreRefreshListener` → `RemoteSegmentStoreDirectory.uploadMetadata` records `UploadedSegmentMetadata` with checksum computed by `getChecksumOfLocalFile` (format-aware).
2. Download: `RemoteStoreFileDownloader.copyOneFile` → `destination.copyFrom(source, file, ...)` — **no validation**.
3. Dedupe skip: `localDirectoryContains` reads stored footer (O(1) for Lucene) or whole-file CRC (O(n) for DFA) and compares to `UploadedSegmentMetadata.getChecksum()`. On mismatch deletes and re-downloads.
4. Engine open: Lucene readers validate codec footers as they open files.
5. Optional: `check_on_startup` for body-level revalidation.

**For Lucene files**: body-level validation only happens if `check_on_startup=checksum` or later during reads / merges.

**For DFA files**: `localDirectoryContains` already does full-body CRC32 (because `GenericCRC32ChecksumHandler` reads the whole file) — this is a lucky accident.

### Peer recovery (shard migration, replica init)

- `LuceneVerifyingIndexOutput` validates every byte inline as it arrives.
- Source re-verifies suspect files on any error.
- Strict: any byte-flip → `CorruptIndexException` → `markStoreCorrupted` → shard re-recovers.

### Snapshot restore

- Same `LuceneVerifyingIndexOutput` path → strict inline validation.

## 4. Gaps for DFA files

| # | Gap | Impact |
|---|---|---|
| G1 | `Store.checkIntegrity` hardcodes `CodecUtil.checksumEntireFile` | Throws on every DFA file. Currently masked because `MetadataSnapshot` from `getMetadata(IndexCommit)` doesn't include DFA files — fix that omission and startup check breaks. |
| G2 | `Store.checkIndex` wraps Lucene `CheckIndex` | No structural validation for DFA (Parquet, etc.) files. |
| G3 | `MultiFileWriter.openAndPutIndexOutput` uses `LuceneVerifyingIndexOutput` unconditionally | Peer recovery of DFA files would fail on the first footer-byte write — non-Lucene files have no codec footer. |
| G4 | `SegmentFileTransferHandler.handleErrorOnSendFiles` calls `Store.checkIntegrityNoException` | Source-side verification on error would falsely flag DFA files as corrupt. |
| G5 | No `markStoreCorrupted` hook from format-specific readers | A Parquet read failure surfaces as a query error, not a recovery trigger. The shard stays assigned with a broken file. |
| G6 | `Lucene.checkSegmentInfoIntegrity` / codec `checkIntegrity` | Not extensible for DFA formats — no `FormatIntegrityChecker` SPI exists. |

## 5. Summary

- **Peer recovery** is strict for Lucene, broken for DFA (G3, G4).
- **Remote-store recovery** is lax for Lucene (relies on later reads) but coincidentally full-body for DFA (via G-free CRC32 dedupe). If `check_on_startup=checksum` is enabled, G1 breaks DFA.
- **Snapshot restore** is strict for Lucene, broken for DFA (same mechanism as peer recovery).
- **Reader-time checks** (CheckIndex, merge integrity) are Lucene-only — no extension point for DFA formats.

Next: design a minimal fix that closes G1–G5 without rewriting the recovery path.
