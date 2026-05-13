# Implementation Plan: Star Tree Upgrade Read Availability

## Overview

Refactor the star tree upgrade flow to maintain read availability during the upgrade window. The core change replaces the current "close engine → upgrade → reopen engine" pattern with a "swap to ReadOnlyEngine → upgrade → swap to InternalEngine" pattern, ensuring `currentEngineReference` is never null. Additionally, split `StarTreeUpgradeService.upgradeSegments()` into two public methods, persist `codecServiceOverride` after upgrade, and set `index.composite_index=true` in cluster state during `applyStarTreeMapping()`.

## Tasks

- [x] 1. Split StarTreeUpgradeService into separate Phase 1 and Phase 2 methods
  - [x] 1.1 Extract `buildStarTreeDataForSegments()` as a new public method
    - Move the Phase 1 loop from `upgradeSegments()` into `buildStarTreeDataForSegments(Directory, StarTreeField, MapperService)`
    - Return `Set<String>` of successfully upgraded segment names instead of a count
    - Keep the existing per-segment error handling (catch, log, skip failed segments)
    - _Requirements: 4.1, 4.4_
  - [x] 1.2 Make `rewriteSegmentInfos()` public and add write lock acquisition
    - Change `rewriteSegmentInfos(Directory, Set<String>)` visibility from package-private to `public static`
    - Acquire `IndexWriter.WRITE_LOCK_NAME` via `directory.obtainLock()` at the start of the method
    - Release the lock in a `finally` block
    - _Requirements: 4.2, 8.1, 8.2, 8.3_
  - [x] 1.3 Add `getCandidateSegmentNames()` method
    - Implement `public static Set<String> getCandidateSegmentNames(Directory)` that reads `SegmentInfos.readLatestCommit()` and returns names of all segments NOT already using Composite912Codec
    - This is used by the caller to track ALL candidate segments for cleanup on failure (not just successful ones)
    - _Requirements: 5.3, 5.4_
  - [x] 1.4 Add `cleanupStarTreeFiles()` method for orphaned file cleanup
    - Implement `public static void cleanupStarTreeFiles(Directory, Set<String>)` that deletes `.cid`, `.cim`, `.cidvd`, `.cidvm` files for the given segment names
    - Use `directory.deleteFile()` with try-catch per file (best-effort cleanup, log warnings on failure)
    - _Requirements: 5.3_
  - [x] 1.5 Retain `upgradeSegments()` as a convenience method with cleanup
    - Rewrite `upgradeSegments()` to call `getCandidateSegmentNames()`, then `buildStarTreeDataForSegments()`, then `rewriteSegmentInfos()`, returning the count
    - Add cleanup on failure targeting all candidate segments (not just successful ones)
    - _Requirements: 4.3_
  - [ ]* 1.6 Write unit tests for `buildStarTreeDataForSegments()` return value
    - Verify it returns the correct subset of non-Composite912Codec segment names
    - Verify star tree files (.cid, .cim, .cidvd, .cidvm) exist for each returned segment name
    - _Requirements: 4.1_
  - [ ]* 1.7 Write property test for buildStarTreeDataForSegments segment subset
    - **Property 2: buildStarTreeDataForSegments returns correct segment subset**
    - **Validates: Requirements 4.1**
  - [ ]* 1.8 Write property test for rewriteSegmentInfos codec switch
    - **Property 3: rewriteSegmentInfos switches codec for upgraded segments only**
    - **Validates: Requirements 4.2**
  - [ ]* 1.9 Write property test for cleanupStarTreeFiles
    - **Property 4: cleanupStarTreeFiles removes orphaned files without affecting others**
    - **Validates: Requirements 5.3**

- [ ] 2. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 3. Implement ReadOnlyEngine bridge in IndexShard.upgradeToStarTree()
  - [x] 3.1 Implement Swap 1: InternalEngine → ReadOnlyEngine
    - Within `synchronized(engineMutex)`: create `ReadOnlyEngine` with `obtainLock=false` FIRST (if constructor throws, old engine is still current), then swap `currentEngineReference`, then close old engine, then set `codecServiceOverride` (inside mutex, after close, to prevent race with concurrent `newEngineConfig()` calls)
    - Pass `null` for `seqNoStats` and `translogStats`, `false` for `obtainLock`, `Function.identity()` for reader wrapper, `false` for `requireCompleteHistory`
    - The ROE uses the stale codec in its EngineConfig — this is intentional and harmless since ROE only reads
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 2.1, 2.4_
  - [x] 3.2 Wire Phase 1 and Phase 2 between the two swaps
    - Call `StarTreeUpgradeService.getCandidateSegmentNames()` BEFORE Phase 1 to get the full candidate set for cleanup
    - Call `StarTreeUpgradeService.buildStarTreeDataForSegments()` to execute Phase 1
    - If returned set is non-empty, call `StarTreeUpgradeService.rewriteSegmentInfos()` for Phase 2
    - On exception, call `StarTreeUpgradeService.cleanupStarTreeFiles()` with the FULL candidate set (not just successful segments) to delete orphaned files from both successful and failed segments
    - _Requirements: 2.2, 2.3, 5.4_
  - [x] 3.3 Implement Swap 2: ReadOnlyEngine → InternalEngine
    - Within `synchronized(engineMutex)`: create new InternalEngine via `engineFactory.newReadWriteEngine(newEngineConfig(replicationTracker))`, call `onNewEngine()`, swap `currentEngineReference`, then close ReadOnlyEngine
    - Call `newEngine.refresh("star-tree-upgrade")` OUTSIDE the `synchronized(engineMutex)` block, wrapped in try-catch (non-fatal if fails — next scheduled refresh picks it up)
    - Set `active.set(true)` after refresh attempt
    - _Requirements: 3.1, 3.2, 3.3, 3.4_
  - [x] 3.4 Implement error recovery for Swap 2 failures
    - Wrap Swap 2 in try-catch
    - If new InternalEngine fails to open with composite codec: clear `codecServiceOverride` to null, attempt recovery by opening with original codec configuration
    - If recovery also fails: call `failShard()` to mark the shard as failed for cluster reallocation
    - _Requirements: 5.1, 5.2_
  - [ ]* 3.5 Write unit tests for engine swap atomicity
    - Verify `currentEngineReference` transitions InternalEngine → ReadOnlyEngine → InternalEngine without null
    - Verify ReadOnlyEngine is created with `obtainLock=false`
    - Verify error recovery when new InternalEngine fails to open
    - _Requirements: 1.1, 1.3, 3.3, 5.1, 5.2_
  - [ ]* 3.6 Write property test for engine reference never null
    - **Property 1: Engine reference never null during upgrade**
    - **Validates: Requirements 1.3, 3.3**

- [ ] 4. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Fix codecServiceOverride lifecycle
  - [x] 5.1 Remove the `codecServiceOverride = null` line after engine restart in `upgradeToStarTree()`
    - The override is set inside Swap 1's `synchronized(engineMutex)` block (after old engine close)
    - Do NOT clear it after Swap 2 — keep it set so `resetEngineToGlobalCheckpoint()` uses Composite912Codec
    - _Requirements: 6.1, 6.2, 6.3_
  - [x] 5.2 Null codecServiceOverride after first post-upgrade engine reset confirms persistent setting
    - In `resetEngineToGlobalCheckpoint()` (or equivalent engine restart path), after creating the new engine: if `mapperService.isCompositeIndexPresent()` is true, set `codecServiceOverride = null` — the fresh `codecService` already includes Composite912Codec via the persistent `index.composite_index=true` setting
    - This avoids permanent volatile read overhead on every `newEngineConfig()` call
    - _Requirements: 6.1, 6.2_
  - [ ]* 5.2 Write unit test for codecServiceOverride persistence
    - Verify `codecServiceOverride` is non-null after `upgradeToStarTree()` completes
    - Verify `newEngineConfig()` returns an EngineConfig using the override's codec service
    - _Requirements: 6.1, 6.2_
  - [ ]* 5.3 Write property test for newEngineConfig codec override
    - **Property 5: newEngineConfig uses codecServiceOverride for write engines when set**
    - Test only write engine (InternalEngine) configs, not ReadOnlyEngine configs (ROE is an intentional exception)
    - **Validates: Requirements 6.2**

- [x] 6. Set index.composite_index in cluster state during applyStarTreeMapping()
  - [x] 6.1 Extend `applyStarTreeMapping()` in `TransportStarTreeUpgradeAction`
    - Add a check: if `StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.get(indexMetadata.getSettings()) == false`, set `index.composite_index=true` in the settings alongside the existing `append_only=true` setting
    - If `index.composite_index` is already `true`, skip setting it to avoid unnecessary `settingsVersion` increments
    - Use the same `Settings.builder().put()` + `indexMetadataBuilder.settings()` + `indexMetadataBuilder.settingsVersion()` pattern already used for `append_only`
    - _Requirements: 7.1, 7.2, 7.3_
  - [ ]* 6.2 Write unit test for composite_index setting in applyStarTreeMapping
    - Verify `index.composite_index=true` is set when it was previously `false`
    - Verify `settingsVersion` is incremented when the setting changes
    - Verify `settingsVersion` is NOT incremented when `index.composite_index` is already `true`
    - _Requirements: 7.1, 7.2_
  - [ ]* 6.3 Write property test for idempotent composite_index setting
    - **Property 6: Idempotent composite_index setting**
    - **Validates: Requirements 7.2**

- [ ] 7. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Wire everything together and integration testing
  - [x] 8.1 Verify end-to-end upgrade flow with ReadOnlyEngine bridge
    - Ensure `upgradeToStarTree()` correctly orchestrates: flush → block → Swap 1 → Phase 1 → Phase 2 → Swap 2 → refresh → unblock
    - Verify the `starTreeUpgradeInProgress` guard still works correctly with the new flow
    - _Requirements: 1.1, 2.1, 3.1_
  - [ ]* 8.2 Write integration tests for read availability during upgrade
    - Index documents → start upgrade → issue concurrent search requests → verify all searches succeed during the upgrade window
    - Verify post-upgrade search correctness (star tree aggregation queries return correct results)
    - Verify post-upgrade ingest works (new documents indexed with Composite912Codec)
    - Test `resetEngineToGlobalCheckpoint()` after upgrade uses composite codec (validates codecServiceOverride persistence)
    - _Requirements: 1.1, 2.1, 2.2, 2.3, 2.4, 3.1, 6.1, 6.2, 7.3_

- [ ] 9. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document
- Unit tests validate specific scenarios and edge cases
- The key insight is that `ReadOnlyEngine(obtainLock=false)` holds a `DirectoryReader` snapshot of `segments_N` that is unaffected by Phase 2's `segments_N+1` commit, so reads remain available throughout
- The `codecServiceOverride` is set inside Swap 1's mutex (after old engine close) to prevent a race where concurrent `newEngineConfig()` calls see the override while the old engine is still active
- The `codecServiceOverride` is kept set after upgrade but nulled after the first post-upgrade engine reset confirms the persistent `index.composite_index=true` setting took effect — avoids permanent volatile read overhead
- On Phase 1 failure, cleanup targets ALL candidate segments (from `getCandidateSegmentNames()`), not just the successful ones — failed segments may have partial files too
- Swap 2 includes full try-catch recovery: first retry with original codec, then `failShard()` if that also fails
- Windows compatibility for `.si` file deletion during Phase 2 is unverified — initial implementation targets Linux/macOS
- `blockOperations()` accepts `CheckedRunnable<Exception>` — checked exceptions propagate unwrapped to caller
