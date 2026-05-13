# Peer Recovery DFA Gaps Analysis

## Executive Summary

Peer recovery IS reachable for DFA indices. The code has been adapted to use `CatalogSnapshot` 
instead of `IndexCommit` on the source side, and `DataFormatAwareStoreDirectory` correctly routes 
format-prefixed filenames on the target side. However, several gaps and risks remain.

## 1. When Does Peer Recovery Happen for DFA Indices?

**Answer: YES, peer recovery is reachable.** There is NO gating by replication type or DFA setting.

- `IndexShard.startRecovery` (line 4347): The `case PEER:` branch has no DFA/segment-rep guard.
- Replicas get `PeerRecoverySource.INSTANCE` by default (`IndexRoutingTable.java:524,549,587,596,671,693`).
- Primary relocation always uses peer recovery (`ShardRouting.java:520`).
- `RecoverySourceHandlerFactory.java:26-28`: Primary relocation → `LocalStorePeerRecoverySourceHandler`.
  Replica recovery on remote-store node → `RemoteStorePeerRecoverySourceHandler`.

**When peer recovery triggers for segment-rep/DFA indices:**
1. Primary relocation (shard moves between nodes) — always uses peer recovery
2. Replica initialization when remote store metadata is not yet available
3. Fallback when segment replication cannot proceed

## 2. Source Side (Primary Sends Files) — Adapted for DFA ✓

Both handlers already use `acquireSafeCatalogSnapshot` instead of `acquireSafeIndexCommit`:

| File | Line | What it does |
|------|------|-------------|
| `LocalStorePeerRecoverySourceHandler.java` | ~100 | `acquireSafeCatalogSnapshot(shard)` |
| `RemoteStorePeerRecoverySourceHandler.java` | ~80 | `acquireSafeCatalogSnapshot(shard)` |
| `RecoverySourceHandler.java` | 353-358 | `acquireSafeCatalogSnapshot` helper method |
| `RecoverySourceHandler.java` | 427-428 | `phase1(CatalogSnapshot snapshot, ...)` |
| `RecoverySourceHandler.java` | 440 | `store.getMetadata(snapshot)` — builds MetadataSnapshot from CatalogSnapshot |
| `RecoverySourceHandler.java` | 445 | `snapshot.getFiles(true)` — returns format-prefixed names |

**Key: `DataFormatAwareEngine.acquireSafeIndexCommit()` (line 1374) throws `UnsupportedOperationException`.**
This is NOT called by peer recovery anymore — both handlers use `acquireSafeCatalogSnapshot`.

## 3. Target Side (Replica Receives Files) — Works via Directory Routing ✓

| File | Line | Mechanism |
|------|------|-----------|
| `MultiFileWriter.java` | 97-103 | `getTempNameForFile` preserves format prefix: `parquet/_0.pqt` → `parquet/recovery.xxx._0.pqt` |
| `MultiFileWriter.java` | 135 | `store.createVerifyingOutput(tempFileName, ...)` |
| `Store.java` | 728-729 | `directory().createOutput(fileName, context)` → goes through `DataFormatAwareStoreDirectory` |
| `DataFormatAwareStoreDirectory.java` | 161-167 | `resolveFileName` parses format prefix and routes to subdirectory |
| `Store.java` | 562 | `directory().rename(tempFile, origFile)` — also routed correctly |

**The `DataFormatAwareStoreDirectory` correctly handles format-prefixed filenames in:**
- `createOutput` (line 185-186)
- `openInput` (line 181)
- `deleteFile` (line 190-191)
- `rename` (line 205-206)
- `fileLength` (line 195-196)
- `sync` (line 200-201)

## 4. Recovery Finalization — Potential Gaps

### Gap 4a: `store.getMetadata()` during `cleanFiles` (LOW RISK)
- **File:** `RecoveryTarget.java:386`
- **What:** `store.getMetadata().getCommitUserData().get(TRANSLOG_UUID_KEY)`
- **Analysis:** `Store.getMetadata()` (no-arg) calls `readSegmentsInfo(null, directory)` → `fromSegmentInfos` 
  which checks for `CATALOG_SNAPSHOT_KEY` in userData. Since the transferred `segments_N` contains this key, 
  it correctly deserializes the `DataformatAwareCatalogSnapshot`. **Works correctly.**

### Gap 4b: Engine Opening After Peer Recovery (MEDIUM RISK)
- **File:** `IndexShard.java:3137-3141`
- **What:** `syncSegmentsFromRemoteSegmentStore(false)` is called during `openEngineAndSkipTranslogRecovery`
  if `indexSettings.isRemoteStoreEnabled()`.
- **Risk:** For DFA indices with remote store, this downloads segments from remote store AFTER peer recovery 
  already transferred files. This could overwrite peer-transferred files or cause inconsistency.
- **Mitigation:** `copySegmentFiles` (line 5989-5991) checks `localDirectoryContains(storeDirectory, file, checksum)` 
  before downloading, so files already present with correct checksum are skipped.
- **Symptom if broken:** Stale files from remote store overwriting newer peer-transferred files.

### Gap 4c: `DataFormatAwareNRTReplicationEngine` Initialization (LOW RISK)
- **File:** `DataFormatAwareNRTReplicationEngine.java:193`
- **What:** Reads `CatalogSnapshot.CATALOG_SNAPSHOT_KEY` from committed segments_N userData.
- **Analysis:** The primary's `DataFormatAwareEngine` writes this key (line 899 of DataFormatAwareEngine.java).
  The `segments_N` file is transferred during peer recovery. The replica engine should find the key.
- **Works correctly** as long as the `segments_N` file is transferred intact.

## 5. Relocation (Primary Moves Between Nodes) — Key Gap

### Gap 5a: Primary Relocation Uses `LocalStorePeerRecoverySourceHandler` (WORKS)
- **File:** `RecoverySourceHandlerFactory.java:26`
- **What:** `isPrimaryRelocation() == true` → always uses `LocalStorePeerRecoverySourceHandler`
- **Analysis:** This handler calls `acquireSafeCatalogSnapshot` which works for DFA.

### Gap 5b: After Relocation, New Primary Opens as `DataFormatAwareEngine` (WORKS)
- **File:** `DataFormatAwareIndexerFactory.java:26-29`
- **What:** `config.isReadOnlyReplica()` → NRT engine, else → `DataFormatAwareEngine`
- **Analysis:** After relocation, the new primary is not a read-only replica, so it gets `DataFormatAwareEngine`.
  The engine reads `CATALOG_SNAPSHOT_KEY` from the committed segments_N to restore state.

## 6. Identified Gaps Summary

### GAP 1: `Lucene.cleanLuceneIndex` in Error Path (LOW SEVERITY)
- **File:** `RecoveryTarget.java:424`, `Lucene.java:253`
- **What:** On corruption, `cleanLuceneIndex` opens an `IndexWriter` on the DFA directory.
- **Risk:** `IndexWriter` may not handle non-lucene files in subdirectories correctly.
- **Symptom:** Recovery corruption cleanup fails, leaving orphan DFA files.
- **When triggered:** Only on corruption detection during `cleanFiles`.

### GAP 2: `store.cleanupAndVerify` File Deletion (LOW SEVERITY)
- **File:** `Store.java:880-905`
- **What:** Iterates `directory.listAll()` and deletes files not in `sourceMetadata`.
- **Analysis:** `DataFormatAwareStoreDirectory.listAll()` returns format-prefixed names for non-lucene files.
  `sourceMetadata.contains(existingFile)` checks against the same format-prefixed names from `CatalogSnapshot.getFiles(true)`.
- **Risk:** If any file naming inconsistency exists between `listAll()` normalization and `getFiles()` serialization,
  valid DFA files could be deleted.
- **Symptom:** Missing DFA segment files after recovery cleanup.

### GAP 3: No Integration Test Coverage
- There are no DFA-specific peer recovery integration tests visible in the recovery test files.
- The happy path appears to work by design, but edge cases (partial transfer, retry, corruption) are untested.

## 7. Conclusion

The peer recovery path has been **largely adapted** for DFA indices:
- Source side uses `CatalogSnapshot` instead of `IndexCommit` ✓
- Target side routes format-prefixed filenames via `DataFormatAwareStoreDirectory` ✓
- `segments_N` carries `CATALOG_SNAPSHOT_KEY` for engine initialization ✓
- `MultiFileWriter.getTempNameForFile` preserves format prefix ✓

**Primary risks are in edge cases:**
1. Error/corruption handling paths that use raw `IndexWriter`
2. Potential file naming inconsistencies between `listAll()` and `getFiles()` serialization
3. Lack of integration test coverage for DFA peer recovery scenarios
