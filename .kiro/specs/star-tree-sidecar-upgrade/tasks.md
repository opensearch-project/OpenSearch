# Implementation Plan: Star Tree Sidecar Upgrade

## Overview

Replace the codec-switching approach with a sidecar architecture. GC protection via `SidecarProtectedDirectory` wrapping `Store.directory()`, returned by new `Store.engineDirectory()` method used by all engine construction paths. `Store.cleanupAndVerify()` patched to skip protected files. Lucene-style CAS ref-counted readers with deferred `unprotect()` + file deletion inside `decRef()`. Cleanup lock held only for in-memory state. `starTreeUpgradeInProgress` flag cleared by `TransportStarTreeUpgradeAction` after ALL shards complete — not in shard's finally block.

## Tasks

- [x] 1. Create SidecarProtectedDirectory + Store.engineDirectory()
  - [x] 1.1 Create `SidecarProtectedDirectory` class extending `FilterDirectory`
    - `deleteFile(String)` — if `protectedFiles.contains(name)`, silently return; otherwise delegate
    - `protect(Set<String>)`, `unprotect(Set<String>)`, `isProtected(String)`, `getDelegate()`
    - `ConcurrentHashMap.newKeySet()` for thread-safe protected set
    - _Requirements: 2.1_
  - [x] 1.2 Add `Store.engineDirectory()` and `Store.installSidecarDirectory()`
    - `private volatile Directory engineDirectory` field (null until wrapper installed)
    - `installSidecarDirectory(SidecarProtectedDirectory)` — sets `engineDirectory`
    - `engineDirectory()` — returns `engineDirectory` if non-null, else `directory()`
    - `Store.directory()` unchanged — continues returning raw `StoreDirectory`
    - _Requirements: 2.2_
  - [x] 1.3 Change `IndexShard.newEngineConfig()` to use `store.engineDirectory()`
    - In `engineConfigFactory.newEngineConfig()` call: the `Store` object is passed, but the engine must call `store.engineDirectory()` instead of `store.directory()` for IndexWriter construction
    - Verify `resetEngineToGlobalCheckpoint()` → `newEngineConfig()` → `store.engineDirectory()` chain works
    - _Requirements: 2.2_
  - [x] 1.4 Patch `Store.cleanupAndVerify()` to skip protected files
    - Before `directory.deleteFile(reason, existingFile)`: check `engineDirectory instanceof SidecarProtectedDirectory spd && spd.isProtected(existingFile)` → skip
    - Audit other Store methods that call `deleteFile()`: `removeCorruptionMarker()`, `deleteQuiet()`, `trimUnsafeCommits()` — verify they don't delete sidecar files
    - _Requirements: 2.3_
  - [ ]* 1.5 Write unit tests
    - `deleteFile()` no-op for protected, passthrough for unprotected
    - `Store.engineDirectory()` returns wrapper when installed, raw when not
    - `Store.cleanupAndVerify()` skips protected files
    - `resetEngineToGlobalCheckpoint()` engine uses wrapped directory
    - _Requirements: 2.1, 2.2, 2.3_

- [x] 2. Create StarTreeValuesProvider interface
  - [x] 2.1 Create standalone `StarTreeValuesProvider` interface (NOT extending `CompositeIndexReader`)
    - `getCompositeIndexFields()`, `getCompositeIndexValues(CompositeIndexFieldInfo)`
    - _Requirements: 4.2_
  - [x] 2.2 Make `Composite912DocValuesReader` implement `StarTreeValuesProvider` directly
    - Add `implements StarTreeValuesProvider` — methods already exist
    - Do NOT modify `CompositeIndexReader`
    - _Requirements: 4.2_

- [x] 3. Create StarTreeSidecarMetadata with generational files
  - [x] 3.1 Create `StarTreeSidecarMetadata` class
    - Generational file pattern: `_startree_sidecar_genN.meta`
    - `load(Directory)` — scan gen files, load highest valid, fall back if corrupt
    - In-memory `ConcurrentHashMap`, `commit(Directory)` atomic write, `removeOrphanedSegments(SegmentInfos)`
    - `commit(Directory)` — write `_startree_sidecar_genN+1.meta` atomically (temp → fsync → rename), then delete previous generation file `_startree_sidecar_genN.meta`. Only current generation exists on disk.
    - `load(Directory)` startup scan deletes all generation files except the highest valid one (handles crash between write and delete of previous gen)
    - `getAllSidecarFileNames()` — union of all sidecar files (for protected set + commit data). Includes the current metadata gen file itself.
    - `containsFile(String)` — check if any entry contains this file
    - JSON format with version field
    - _Requirements: 3.1, 3.3, 3.4, 3.5, 3.6_
  - [ ]* 3.2 Write unit tests
    - CRUD, generational commit/load, crash recovery (corrupt gen fallback), removeOrphanedSegments
    - Test `commit()` deletes previous generation file after writing new one
    - Test `load()` startup scan deletes all but highest valid generation (simulate crash leaving two gen files)
    - _Requirements: 3.1, 3.3, 3.4, 3.5_

- [x] 4. Create LiveDocsFilteredDocValuesProducer
  - [x] 4.1 Create wrapper class
    - Filter deleted docs, remap to contiguous IDs
    - Self-contained aggregation index, ordinals internal
    - _Requirements: 1.2, 1.5, 1.7_
  - [ ]* 4.2 Write unit tests
    - No-deletes passthrough, deletes filtering, contiguous IDs
    - _Requirements: 1.2_

- [x] 5. Checkpoint - Ensure all tests pass

- [x] 6. Create StarTreeSidecarReader with Lucene-style CAS ref counting
  - [x] 6.1 Create `StarTreeSidecarReader` implementing `StarTreeValuesProvider`
    - Constructor: open `.cim/.cid/.cidvd/.cidvm`, reuse parsing from `Composite912DocValuesReader`
    - Takes `SidecarProtectedDirectory` reference for deferred file deletion
    - CAS ref counting:
      - `incRef()` — CAS loop: if count ≤ 0, throw `AlreadyClosedException`; else CAS increment
      - `decRef()` — decrement; if 0: `closeInternal()`; if `pendingDeletion`: `deleteFiles()`
      - `markPendingDeletion()` — set volatile flag
      - `close()` → `decRef()`
    - `deleteFiles()` — called ONLY at refCount=0 with pendingDeletion: `sidecarProtectedDirectory.unprotect(files)` THEN `sidecarProtectedDirectory.getDelegate().deleteFile(file)`
    - Constructor throws `FileNotFoundException` on stale segment
    - _Requirements: 4.3, 4.4, 4.5_
  - [ ]* 6.2 Write unit tests
    - CAS: `incRef` on closed throws `AlreadyClosedException`
    - `unprotect()` happens inside `deleteFiles()` at refCount=0, NOT before `decRef()`
    - `pendingDeletion`: files deleted when refCount=0, not before
    - `FileNotFoundException` on stale segment
    - _Requirements: 4.3, 4.4, 4.5_

- [x] 7. Modify StarTreeUpgradeService for sidecar approach
  - [x] 7.1 Add `buildSidecarStarTreeData()` method
    - Open `DirectoryReader` snapshot, record generation
    - Iterate segments: skip native/sidecar, build with `LiveDocsFilteredDocValuesProducer`
    - Track files in per-run `Set<String> filesWrittenThisRun`
    - Yield between segments + configurable throttle
    - Close `DirectoryReader`
    - Under `sidecarMetadataLock`: compare generations, discard merged-away, register survivors (in-memory only)
    - Outside lock: commit metadata
    - Finally block: delete any file in `filesWrittenThisRun` not in committed metadata
    - _Requirements: 1.1-1.7, 9.2, 10.1_
  - [x] 7.2 Add `buildSidecarStarTreeDataForSegment()` method
    - Uses `LiveDocsFilteredDocValuesProducer`, `numLiveDocs` for `SegmentWriteState`
    - _Requirements: 1.1, 1.2_

- [x] 8. Checkpoint - Ensure all tests pass

- [x] 9. Modify StarTreeQueryHelper for sidecar discovery
  - [x] 9.1 Extend `getStarTreeValues()` with sidecar fallback
    - First: `instanceof CompositeIndexReader` (existing, unchanged)
    - Second: get sidecar reader from cache, `incRef()`, try `getCompositeIndexValues()`, `decRef()` in finally
    - Catch `FileNotFoundException`/`AlreadyClosedException` → remove stale entry, return null
    - _Requirements: 4.1, 4.5_
  - [x] 9.2 Add `isStarTreeUpgradeInProgress()` check to `isStarTreeSupported()`
    - Skip star tree on shards where upgrade in progress
    - _Requirements: 11.3_
  - [x] 9.3 Wire sidecar metadata + reader cache into IndexShard
    - `StarTreeSidecarMetadata` field, loaded on shard start
    - `ConcurrentHashMap<String, StarTreeSidecarReader>` cache
    - Close all readers on shard close
    - _Requirements: 3.6, 4.4_

- [x] 10. Checkpoint - Ensure all tests pass

- [x] 11. Modify IndexShard for sidecar upgrade orchestration
  - [x] 11.1 Install SidecarProtectedDirectory before engine creation
    - In `openEngineAndRecoverFromTranslog()` and all equivalent recovery entry points:
      - Load `StarTreeSidecarMetadata` from generational files
      - Run crash recovery: metadata + files on disk → valid; metadata + files missing → stale; cross-check commit data
      - Create `SidecarProtectedDirectory(store.directory(), metadata.getAllSidecarFileNames())`
      - Call `store.installSidecarDirectory(wrapper)` BEFORE engine creation
    - Verify this covers: normal recovery, peer recovery, snapshot restore
    - _Requirements: 2.2, 2.5, 3.3, 8.5_
  - [x] 11.2 Rewrite `upgradeToStarTree()` — flag NOT cleared in finally
    - Phase 1 (engine live): `buildSidecarStarTreeData()` → `sidecarProtectedDirectory.protect(newFiles)` → commit metadata
    - Pre-drain: `flush(force=true)` OUTSIDE `blockOperations()`
    - Phase 2 (brief block): `updateCommitDataForSidecarFiles()` → `flush(force=true)` → `codecServiceOverride` → `resetEngineToGlobalCheckpoint()` → unblock
    - `starTreeUpgradeInProgress.compareAndSet(false, true)` at start
    - On failure: `starTreeUpgradeInProgress.set(false)` in catch block (allow retry)
    - On success: flag stays set — `TransportStarTreeUpgradeAction` clears it after all shards complete
    - Expose `clearStarTreeUpgradeInProgress()` and `isStarTreeUpgradeInProgress()` methods
    - _Requirements: 5.1-5.5, 7.2, 7.3, 8.1-8.4, 9.1, 11.1, 11.2_

- [x] 12. Modify TransportStarTreeUpgradeAction for flag coordination
  - [x] 12.1 Clear `starTreeUpgradeInProgress` on all shards across all nodes after broadcast completes
    - Wrap `super.doExecute()` listener: on response, broadcast `clearStarTreeUpgradeInProgress()` to all shards on all nodes
    - Use `TransportBroadcastByNodeAction` (reuse existing broadcast mechanism) or `TransportNodesAction` to reach remote nodes — NOT local `indicesService` iteration (which only sees local shards)
    - On total failure: still broadcast flag clearing to all reachable nodes
    - If flag clearing broadcast fails on some nodes: log warning, return upgrade response anyway. Flags on unreachable nodes will be cleared on next shard start or upgrade retry.
    - _Requirements: 11.2, 11.4_

- [x] 13. Implement sidecar cleanup on merge
  - [x] 13.1 Add cleanup dispatch in `EngineMergeScheduler.afterMerge()`
    - Dispatch to `ThreadPool.Names.FLUSH` (same pattern as existing post-merge flush)
    - _Requirements: 6.2_
  - [x] 13.2 Implement cleanup callback
    - Under `sidecarMetadataLock` (in-memory only): remove from metadata, `markPendingDeletion()` + `decRef()` on cached reader
    - Outside lock: commit metadata, update commit data
    - `unprotect()` + file deletion inside reader's `decRef()` — NOT in this callback
    - _Requirements: 6.1, 6.4_
  - [x] 13.3 Add periodic orphan cleanup
    - On shard start + periodically (on refresh): compare metadata vs SegmentInfos, clean up stale entries
    - _Requirements: 6.3, 6.5_

- [x] 14. Checkpoint - Ensure all tests pass

- [ ] 15. Integration testing
  - [ ]* 15.1 Upgrade with deletes — correct aggregation values
    - _Requirements: 1.2_
  - [ ]* 15.2 IndexWriter GC protection — `deleteUnusedFiles()` + crash recovery don't delete sidecar files
    - _Requirements: 2.1, 2.5_
  - [ ]* 15.3 `Store.cleanupAndVerify()` doesn't delete sidecar files
    - _Requirements: 2.3_
  - [ ]* 15.4 `resetEngineToGlobalCheckpoint()` uses wrapped directory
    - _Requirements: 2.2_
  - [ ]* 15.5 Concurrent query + cleanup — ref counting prevents FileNotFoundException
    - _Requirements: 4.3, 4.4_
  - [ ]* 15.6 Replica consistency — star tree skipped until transport action clears flags on all shards
    - _Requirements: 11.2, 11.3_
  - [ ]* 15.7 Crash recovery — metadata loaded, wrapper installed before engine
    - _Requirements: 2.5, 3.3, 8.5_
  - [ ]* 15.8 TOCTOU benign handling — stale entries caught, query falls back
    - _Requirements: 4.5_
  - [ ]* 15.9 Write availability — writes blocked only during final flush+restart
    - _Requirements: 5.4_
  - [ ]* 15.10 Background merge transition + idempotent upgrade + post-upgrade ingest
    - _Requirements: 6.1, 8.4_

- [ ] 16. Final checkpoint - Ensure all tests pass

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- **Issue #1 (starTreeUpgradeInProgress timing)**: Flag NOT cleared in IndexShard's finally block. TransportStarTreeUpgradeAction broadcasts `clearStarTreeUpgradeInProgress()` to ALL shards on ALL nodes after the upgrade broadcast completes — uses `TransportBroadcastByNodeAction` or `TransportNodesAction` to reach remote nodes, NOT local `indicesService` iteration. On failure: catch block clears flag on the failed shard only (allow retry). If flag-clearing broadcast fails on some nodes: log warning, flags cleared on next shard start.
- **Issue #2 (Store.cleanupAndVerify bypasses wrapper)**: `Store.cleanupAndVerify()` patched to check `engineDirectory instanceof SidecarProtectedDirectory` and skip protected files. Other Store deleteFile paths audited.
- **Issue #3 (resetEngineToGlobalCheckpoint directory)**: New `Store.engineDirectory()` method returns wrapper when installed. `newEngineConfig()` uses `store.engineDirectory()`. All engine construction paths — including `resetEngineToGlobalCheckpoint()` — get the wrapper.
- **Store contract**: `store.directory()` unchanged (raw StoreDirectory). `store.engineDirectory()` returns wrapper for engine construction. Less disruptive than changing `directory()` return value.
- **unprotect ordering**: `unprotect()` + `deleteFile()` happen inside `StarTreeSidecarReader.deleteFiles()` at refCount=0 — NEVER before `decRef()`. Files stay protected while any query holds a reference.
- **Cleanup lock scope**: `sidecarMetadataLock` held only for in-memory state. Disk I/O (commit, flush) outside lock. Prevents deadlock with concurrent flush.
- **CAS ref counting**: `incRef()` uses CAS loop, throws `AlreadyClosedException` if count was 0. Handles race between cache release and query acquisition correctly.
- **Crash recovery**: Metadata file is primary source of truth. Commit data is secondary (detects incomplete builds only).
- **Generation cleanup**: `commit()` deletes previous generation file after writing new one. `load()` startup scan deletes all but highest valid generation. Only one gen file on disk at steady state.
- **Windows compatibility**: Ref-counted reader ensures `deleteFiles()` is called only at refCount=0 (all handles closed). Safe on POSIX and Windows. `deleteFiles()` wraps each call in try-catch for robustness.
