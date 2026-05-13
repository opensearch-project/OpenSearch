# NRT Replication Engine Duplication Extraction Plan

## 1. TOTAL FILE SIZES

| Metric | NRTReplicationEngine (NRT) | DataFormatAwareNRTReplicationEngine (DFANRT) |
|--------|---------------------------|----------------------------------------------|
| Total LOC | 620 | 914 |
| Imports | 40 | 64 |
| Public methods | ~30 | ~35 |
| Private methods | ~5 | ~8 |
| Protected methods | ~3 | 0 |

NRT: `server/src/main/java/org/opensearch/index/engine/NRTReplicationEngine.java`
DFANRT: `server/src/main/java/org/opensearch/index/engine/DataFormatAwareNRTReplicationEngine.java`

## 2. CLASS HIERARCHY

**NRTReplicationEngine** extends `Engine` (abstract class, Engine.java:137).
- Engine implements `LifecycleAware, Closeable`.
- Engine provides: `shardId`, `logger`, `engineConfig`, `store`, `isClosed`, `closedLatch`, `failEngineLock`, `rwl`, `readLock`, `writeLock`, `failedEngine`, `lastWriteNanos`, `eventListener`.
- Engine provides concrete methods: `ensureOpen()`, `failEngine()`, `maybeFailEngine()`, `close()`, `flushAndClose()`, `awaitPendingClose()`, `getTranslogDeletionPolicy()`, `loadHistoryUUID()`, `commitStats()`, `config()`.
- Engine declares abstract: `closeNoLock()`, `getLastCommittedSegmentInfos()`, `getLatestSegmentInfos()`, `translogManager()`, plus ~30 abstract methods for index/delete/noOp/flush/refresh/etc.

**DataFormatAwareNRTReplicationEngine** implements `Indexer` (interface, exec/Indexer.java:56).
- Indexer extends: `LifecycleAware`, `Closeable`, `IndexerEngineOperations`, `IndexerStateManager`, `IndexerLifecycleOperations`, `IndexerStatistics`, `IndexReaderProvider`.
- DFANRT must declare ALL fields that NRT inherits from Engine (logger, shardId, store, rwl, readLock, writeLock, isClosed, closedLatch, failedEngine, failEngineLock, lastWriteNanos).
- DFANRT must implement ALL methods that Engine provides as concrete (ensureOpen, failEngine, maybeFailEngine, close, flushAndClose, awaitPendingClose, config, commitStats).

**Constraint**: A shared abstract base class is NOT feasible. NRT extends Engine (which is deeply wired into IndexShard and the rest of OpenSearch). DFANRT implements Indexer (a separate interface hierarchy). Making DFANRT extend Engine would pull in Lucene-specific abstractions (getReferenceManager, getLastCommittedSegmentInfos returning SegmentInfos, acquireSearcher, etc.) that don't apply to data-format engines. Making NRT implement Indexer would require adding CatalogSnapshot/Reader methods that don't apply to Lucene-only engines. **Composition and utility methods are the only viable extraction paths.**

## 3. METHOD-BY-METHOD COMPARISON TABLE

| Method Signature | NRT Behavior | DFANRT Behavior | Status |
|-----------------|-------------|-----------------|--------|
| **Constructor** | NRT:77-150. Calls `super(engineConfig)`, builds ReplicaFileTracker, reads lastCommittedSegmentInfos, builds NRTReplicationReaderManager, wires listeners, creates WriteOnlyTranslogManager with TranslogEventListener. | DFANRT:118-240. Manually sets logger/shardId/store/etc., bootstraps empty commit if needed, restores CatalogSnapshot from userData, builds per-format EngineReaderManagers, creates WriteOnlyTranslogManager with identical TranslogEventListener. | NEAR-IDENTICAL translog setup (~30 LOC); DIFFERENT bootstrap/reader logic |
| `translogManager()` | NRT:168. Returns `translogManager`. | DFANRT:245. Returns `translogManager`. | IDENTICAL (1 LOC) |
| `updateSegments(SegmentInfos)` / `updateCatalogSnapshot(CatalogSnapshot)` | NRT:172-192. writeLock, ensureOpen, parse maxSeqNo, update readerManager, flush-on-gen-change, rollTranslogGeneration, fastForwardProcessedSeqNo. | DFANRT:249-275. writeLock, ensureOpen, parse maxSeqNo, applySnapshot+notifyReaders, update historyUUID, invokeRefreshListeners, flush-on-gen-change, rollTranslogGeneration, fastForwardProcessedSeqNo. | NEAR-IDENTICAL algorithm skeleton (~10 LOC shared: gen-change guard + fastForward); DIFFERENT reader update + extra historyUUID/listener logic in DFANRT |
| `commitSegmentInfos()` / `commitCatalogSnapshot()` | NRT:200-215. Gets prev commit files, calls store.commitSegmentInfos, updates lastCommittedSegmentInfos, incRef/decRef via replicaFileTracker, syncTranslog. | DFANRT:306-355. Acquires CatalogSnapshot, builds commitData map, creates synthetic SegmentInfos, calls store.commitSegmentInfos, updates lastCommittedSnapshot, syncTranslog. Optional SI counter bump. | DIFFERENT — NRT commits real SegmentInfos; DFANRT builds synthetic SegmentInfos with CatalogSnapshot serialization. Both end with `store.commitSegmentInfos()` + `translogManager.syncTranslog()`. |
| `getHistoryUUID()` | NRT:219. `loadHistoryUUID(lastCommittedSegmentInfos.userData)` (inherited from Engine). | DFANRT:360-369. Reads from lastCommittedSnapshot userData, falls back to field. | DIFFERENT — NRT delegates to Engine.loadHistoryUUID; DFANRT has custom logic for CatalogSnapshot. |
| `getWritingBytes()` | NRT:224. Returns 0. | DFANRT:372. Returns 0. | IDENTICAL |
| `completionStats(...)` | NRT:229. `completionStatsCache.get(fieldNamePatterns)`. | DFANRT:377. Returns `new CompletionStats()`. | DIFFERENT |
| `getIndexThrottleTimeInMillis()` | NRT:234. Returns 0. | DFANRT:383. Returns 0. | IDENTICAL |
| `isThrottled()` | NRT:239. Returns false. | DFANRT:388. Returns false. | IDENTICAL |
| `index(Index)` | NRT:244-254. ensureOpen, create IndexResult, add to translog, setTranslogLocation, setTook, freeze, advanceMaxSeqNo. | DFANRT:393-402. Identical logic with `Engine.` prefix on types. | IDENTICAL (~10 LOC) |
| `delete(Delete)` | NRT:256-266. Same pattern as index. | DFANRT:405-414. Identical. | IDENTICAL (~10 LOC) |
| `noOp(NoOp)` | NRT:268-278. Same pattern. | DFANRT:417-426. Identical. | IDENTICAL (~10 LOC) |
| `get(Get, BiFunction)` | NRT:280-282. Delegates to `getFromSearcher`. | DFANRT: MISSING (Indexer has no `get`). | MISSING in DFANRT |
| `getReferenceManager(SearcherScope)` | NRT:285. Returns readerManager. | DFANRT: MISSING (not in Indexer). | MISSING in DFANRT |
| `acquireReader()` | NRT: MISSING (not in Engine). | DFANRT:429-446. Acquires CatalogSnapshot, gets per-format readers. | MISSING in NRT |
| `refreshNeeded()` | NRT:297. Returns false. | DFANRT:450. Returns false. | IDENTICAL |
| `acquireHistoryRetentionLock()` | NRT:302. Throws UnsupportedOperationException. | DFANRT:455. Throws UnsupportedOperationException. | IDENTICAL |
| `newChangesSnapshot(...)` | NRT:307-313. Throws UnsupportedOperationException. | DFANRT:460-467. Throws UnsupportedOperationException. | IDENTICAL |
| `countNumberOfHistoryOperations(...)` | NRT:317-319. Returns 0. | DFANRT:470-472. Returns 0. | IDENTICAL |
| `hasCompleteOperationHistory(...)` | NRT:323. Returns false. | DFANRT:476. Returns false. | IDENTICAL |
| `getMinRetainedSeqNo()` | NRT:328. `localCheckpointTracker.getProcessedCheckpoint()`. | DFANRT:481. Same. | IDENTICAL |
| `getPersistedLocalCheckpoint()` | NRT:333. `localCheckpointTracker.getPersistedCheckpoint()`. | DFANRT:486. Same. | IDENTICAL |
| `getProcessedLocalCheckpoint()` | NRT:338. `localCheckpointTracker.getProcessedCheckpoint()`. | DFANRT:491. Same. | IDENTICAL |
| `lastRefreshedCheckpoint()` | NRT: MISSING (not overridden; Engine has no default). | DFANRT:497. `localCheckpointTracker.getProcessedCheckpoint()`. | MISSING in NRT |
| `getSeqNoStats(long)` | NRT:343. `localCheckpointTracker.getStats(globalCheckpoint)`. | DFANRT:502. Same. | IDENTICAL |
| `getLastSyncedGlobalCheckpoint()` | NRT:348. `translogManager.getLastSyncedGlobalCheckpoint()`. | DFANRT:507. Same. | IDENTICAL |
| `getIndexBufferRAMBytesUsed()` | NRT:353. Returns 0. | DFANRT:512. Returns 0. | IDENTICAL |
| `segments(boolean)` | NRT:358. Returns segment info array. | DFANRT: MISSING. | MISSING in DFANRT |
| `refresh(String)` | NRT:363-365. No-op (comment only). | DFANRT:517. Empty body. | IDENTICAL |
| `maybeRefresh(String)` | NRT:368. Returns false. | DFANRT:520. Returns false. | IDENTICAL |
| `writeIndexingBuffer()` | NRT:372. Empty. | DFANRT:524. Empty. | IDENTICAL |
| `shouldPeriodicallyFlush()` | NRT:376. Returns false. | DFANRT:528. Returns false. | IDENTICAL |
| `flush(boolean, boolean)` | NRT:380-405. ensureOpen, warm-index guard, readLock, flushLock tryLock/lock, commitSegmentInfos, maybeFailEngine, FlushFailedEngineException. | DFANRT:533-556. Identical structure, calls commitCatalogSnapshot instead. | NEAR-IDENTICAL (~20 LOC, differs only in commit call) |
| `flush()` | NRT: inherited from Engine (calls flush(false,true)). | DFANRT:559. `flush(false, true)`. | IDENTICAL |
| `forceMerge(...)` | NRT:409-416. Empty. | DFANRT:563-570. Empty. | IDENTICAL |
| `acquireLastIndexCommit(boolean)` | NRT:419-432. flush-if-needed, get IndexCommit from lastCommittedSegmentInfos, incRef files via replicaFileTracker. | DFANRT:573-594. ensureOpen, flush-if-needed, get IndexCommit from store.readLastCommittedSegmentsInfo, pin CatalogSnapshot. | NEAR-IDENTICAL structure; DIFFERENT pinning mechanism |
| `acquireSafeIndexCommit()` | NRT:436. Delegates to `acquireLastIndexCommit(false)`. | DFANRT:597. Same. | IDENTICAL |
| `getSafeCommitInfo()` | NRT:440. Uses `lastCommittedSegmentInfos.totalMaxDoc()`. | DFANRT:602. Uses hardcoded 0 for doc count. | NEAR-IDENTICAL (1 field differs) |
| `closeNoLock(...)` | NRT:444-487. isClosed CAS, assert lock, SI counter bump (conditional), commitSegmentInfos, mark-corrupt-on-failure, IOUtils.close(readerManager, translogManager), store.decRef, closedLatch.countDown. | DFANRT:618-654. Same structure: isClosed CAS, assert lock, commitCatalogSnapshot(bumpCounter), mark-corrupt-on-failure, IOUtils.close(readerManagers+catalogSnapshotManager+translogManager), store.decRef, closedLatch.countDown. | NEAR-IDENTICAL (~35 LOC shared pattern; differs in commit call and closeables list) |
| `activateThrottling()` | NRT:490. Empty. | DFANRT:658. Empty. | IDENTICAL |
| `deactivateThrottling()` | NRT:493. Empty. | DFANRT:661. Empty. | IDENTICAL |
| `fillSeqNoGaps(long)` | NRT:496-501. Returns 0. | DFANRT:664. Returns 0. | IDENTICAL |
| `maybePruneDeletes()` | NRT:504. Empty. | DFANRT:669. Empty. | IDENTICAL |
| `getMergeStats()` | NRT:507-510. Creates MergeStats, adds from tracker. | DFANRT:674-677. Identical. | IDENTICAL (~3 LOC) |
| `updateMaxUnsafeAutoIdTimestamp(long)` | NRT:514. Empty. | DFANRT:682. Empty. | IDENTICAL |
| `getMaxSeqNoOfUpdatesOrDeletes()` | NRT:517. `localCheckpointTracker.getMaxSeqNo()`. | DFANRT:686. Same. | IDENTICAL |
| `advanceMaxSeqNoOfUpdatesOrDeletes(long)` | NRT:521. Empty. | DFANRT:691. Empty. | IDENTICAL |
| `onSettingsChanged(...)` | NRT:524-528. Gets deletion policy, sets retention age + size. | DFANRT:695-699. Identical. | IDENTICAL (~4 LOC) |
| `ensureOpen()` | NRT: inherited from Engine:904. `if (isClosed.get()) throw AlreadyClosedException`. | DFANRT:710-713. Identical logic, manually implemented. | IDENTICAL (~3 LOC) |
| `failEngine(...)` | NRT: inherited from Engine:1288. tryLock, check failedEngine, set failure, closeNoLock, log, notify eventListener, mark corrupt if needed. | DFANRT:740-762. Similar but simpler: no maybeDie, no corruption marking in failEngine (done in closeNoLock instead), notifies eventListener. | NEAR-IDENTICAL structure; Engine's version is more complex |
| `maybeFailEngine(...)` | NRT: inherited from Engine:1390. Checks corruption, fails engine. | DFANRT:765-773. Checks corruption + translog tragic event + AlreadyClosedException. More comprehensive. | DIFFERENT — DFANRT has extra translog tragic-event checks |
| `getTranslogDeletionPolicy()` | NRT: inherited from Engine:968. Custom factory or default. | DFANRT:775-785. Identical logic, but takes no args (uses `this.engineConfig`). | IDENTICAL (~10 LOC) |
| `close()` | NRT: inherited from Engine:2107. writeLock, closeNoLock, awaitPendingClose. | DFANRT:608-614. writeLock, closeNoLock, awaitPendingClose. | IDENTICAL structure (~6 LOC) |
| `flushAndClose()` | NRT: inherited from Engine:2086. writeLock, flush, close, awaitPendingClose. | DFANRT:720-737. Identical. | IDENTICAL (~15 LOC) |
| `awaitPendingClose()` | NRT: inherited from Engine:2119. closedLatch.await with interrupt handling. | DFANRT:756-761. Identical. | IDENTICAL (~5 LOC) |
| `prepareIndex(...)` | NRT: MISSING (not in Engine). | DFANRT:800-822. Parses doc, creates Engine.Index. | MISSING in NRT |
| `prepareDelete(...)` | NRT: MISSING (not in Engine). | DFANRT:825-840. Creates Engine.Delete. | MISSING in NRT |
| `commitStats()` | NRT: inherited from Engine:908. `new CommitStats(getLastCommittedSegmentInfos())`. | DFANRT:845-854. Reads from store, falls back to empty SegmentInfos. | DIFFERENT |
| `docStats()` | NRT: inherited from Engine (uses searcher). | DFANRT:857. Returns `new DocsStats(0,0,0)`. | DIFFERENT |
| `segmentsStats(...)` | NRT: inherited from Engine. | DFANRT:862. Returns `new SegmentsStats()`. | DIFFERENT |
| `pollingIngestStats()` | NRT: MISSING. | DFANRT:867. Returns null. | MISSING in NRT |
| `getMaxSeenAutoIdTimestamp()` | NRT: MISSING (Engine has it). | DFANRT:872. Returns Long.MIN_VALUE. | DIFFERENT source |
| `getLastWriteNanos()` | NRT: inherited from Engine field. | DFANRT:877. Returns `lastWriteNanos` field. | IDENTICAL |
| `unreferencedFileCleanUpsPerformed()` | NRT: inherited from Engine counter. | DFANRT:882. Returns 0. | DIFFERENT |
| `verifyEngineBeforeIndexClosing()` | NRT: inherited from Engine. | DFANRT:887. Empty. | DIFFERENT |

## 4. SHARED STATE

### Intrinsic Engine State (present in both)

| Field | Type | Purpose | NRT Source | DFANRT Source |
|-------|------|---------|------------|---------------|
| `logger` | Logger | Logging | Inherited from Engine:152 | DFANRT:99 (declared) |
| `engineConfig` | EngineConfig | Configuration | Inherited from Engine:149 | DFANRT:100 (declared) |
| `shardId` | ShardId | Shard identifier | Inherited from Engine:148 | DFANRT:101 (declared) |
| `store` | Store | Index store | Inherited from Engine:150 | DFANRT:102 (declared) |
| `localCheckpointTracker` | LocalCheckpointTracker | Seq-no tracking | NRT:68 | DFANRT:90 |
| `translogManager` | WriteOnlyTranslogManager | Translog ops | NRT:69 | DFANRT:91 |
| `flushLock` | Lock (ReentrantLock) | Flush serialization | NRT:70 | DFANRT:92 |
| `lastReceivedPrimaryGen` | volatile long | Generation tracking | NRT:73 | DFANRT:94 |
| `SI_COUNTER_INCREMENT` | static final int (100000) | Counter bump on close | NRT:75 | DFANRT:96 |
| `rwl` | ReentrantReadWriteLock | Read/write locking | Inherited from Engine:155 | DFANRT:109 (declared) |
| `readLock` | ReleasableLock | Read lock wrapper | Inherited from Engine:156 | DFANRT:110 (declared) |
| `writeLock` | ReleasableLock | Write lock wrapper | Inherited from Engine:157 | DFANRT:111 (declared) |
| `failEngineLock` | ReentrantLock | Fail-engine serialization | Inherited from Engine:158 | DFANRT:112 (declared) |
| `isClosed` | AtomicBoolean | Closed flag | Inherited from Engine:151 | DFANRT:114 (declared) |
| `failedEngine` | SetOnce\<Exception\> | Failure cause | Inherited from Engine:159 | DFANRT:115 (declared) |
| `closedLatch` | CountDownLatch | Close synchronization | Inherited from Engine:153 | DFANRT:116 (declared) |
| `lastWriteNanos` | volatile long | Activity tracking | Inherited from Engine:170 | DFANRT:106 (declared) |
| `historyUUID` | String (volatile in DFANRT) | History UUID | NRT: via loadHistoryUUID on lastCommittedSegmentInfos | DFANRT:105 (declared field) |
| `internalRefreshListeners` | List\<RefreshListener\> | Refresh notification | NRT: wired via readerManager.addListener (NRT:107-112) | DFANRT:107 (declared list) |

### Replica-Specific State (divergent)

| Field | NRT | DFANRT |
|-------|-----|--------|
| Segment/snapshot tracking | `lastCommittedSegmentInfos` (SegmentInfos, NRT:64) | `lastCommittedSnapshot` (CatalogSnapshot, DFANRT:87) |
| Mutex for above | `lastCommittedSegmentInfosMutex` (Object, NRT:65) | `lastCommittedSnapshotMutex` (Object, DFANRT:88) |
| Reader manager | `readerManager` (NRTReplicationReaderManager, NRT:66) | `readerManagers` (Map\<DataFormat, EngineReaderManager\>, DFANRT:89) |
| File tracking | `replicaFileTracker` (ReplicaFileTracker, NRT:71) | `catalogSnapshotManager` (CatalogSnapshotManager, DFANRT:103) |
| Completion cache | `completionStatsCache` (CompletionStatsCache, NRT:67) | None |

## 5. COMMON ALGORITHMS

### (a) Pre-commit translog sync + rollGeneration + trimUnreferencedReaders

The TranslogEventListener anonymous class is identical in both constructors:

**NRT:119-133:**
```java
new TranslogEventListener() {
    @Override public void onFailure(String reason, Exception ex) { failEngine(reason, ex); }
    @Override public void onAfterTranslogSync() {
        try { translogManager.trimUnreferencedReaders(); }
        catch (IOException ex) { throw new TranslogException(shardId, "failed to trim unreferenced translog readers", ex); }
    }
}
```

**DFANRT:201-216:** Identical code.

**Duplication: ~12 LOC, IDENTICAL.**

### (b) Close-with-commit-or-mark-corrupt pattern

**NRT:444-487 (closeNoLock):**
```
isClosed CAS → assert lock → [SI counter bump if not remote] → try commitSegmentInfos → catch IOException → if not failEngine-held && not corrupted → markStoreCorrupted → IOUtils.close(readerManager, translogManager) → finally store.decRef + closedLatch.countDown
```

**DFANRT:618-654 (closeNoLock):**
```
isClosed CAS → assert lock → try [warm-index guard] → commitCatalogSnapshot(bumpCounter) → catch IOException → if not failEngine-held && not corrupted → markStoreCorrupted → IOUtils.close(readerManagers.values + catalogSnapshotManager + translogManager) → finally store.decRef + closedLatch.countDown
```

**Duplication: ~25 LOC of structural pattern. The commit call and closeables list differ. The mark-corrupt-on-failure block is identical (~8 LOC).**

### (c) fastForwardProcessedSeqNo after applying incoming

**NRT:191:** `localCheckpointTracker.fastForwardProcessedSeqNo(maxSeqNo);`
**DFANRT:274:** `localCheckpointTracker.fastForwardProcessedSeqNo(maxSeqNo);`

**Duplication: 1 LOC — not worth extracting standalone.**

### (d) Flush-on-generation-change guard

**NRT:185-189:**

**DFANRT:268-273:**

**Duplication: ~6 LOC, IDENTICAL.**

### (e) Safe-commit acquisition + pinning via GatedCloseable

**NRT:419-432:** Gets IndexCommit, pins files via replicaFileTracker.incRef, returns GatedCloseable with decRef on close.
**DFANRT:573-594:** Gets IndexCommit from store, pins CatalogSnapshot via catalogSnapshotManager.acquireSnapshot, returns GatedCloseable.

**Duplication: Structural similarity only (~5 LOC). Pinning mechanisms are fundamentally different. NOT worth extracting.**

### (f) Flush method body (flushLock pattern)

**NRT:380-405:**

**DFANRT:533-556:** Identical structure, only `commitSegmentInfos()` vs `commitCatalogSnapshot()`.

**Duplication: ~20 LOC, NEAR-IDENTICAL. Extractable with a `Runnable` or `IORunnable` for the commit call.**

### (g) ensureOpen + AlreadyClosedException

**Engine:894-906 (inherited by NRT):**

**DFANRT:710-713:**

**Duplication: ~3 LOC. DFANRT's version is simpler (no suppressed param). Too small to extract.**

### (h) Constructor bootstrap of historyUUID / translogUUID from store userData

**NRT:86-87 + 113-114:** Reads `lastCommittedSegmentInfos.getUserData()`, extracts translogUUID.
**DFANRT:133-145 + 178-179:** Reads userData from store (with IndexNotFoundException fallback), extracts translogUUID.

**Duplication: The translogUUID extraction is 1 line. The userData reading differs (NRT reads SegmentInfos directly; DFANRT has fallback logic). NOT worth extracting.**

### (i) TranslogDeletionPolicy factory selection

**Engine:968-981 (inherited by NRT):**
```java
protected TranslogDeletionPolicy getTranslogDeletionPolicy(EngineConfig engineConfig) {
    TranslogDeletionPolicy custom = null;
    if (engineConfig.getCustomTranslogDeletionPolicyFactory() != null) {
        custom = engineConfig.getCustomTranslogDeletionPolicyFactory()
            .create(engineConfig.getIndexSettings(), engineConfig.retentionLeasesSupplier());
    }
    return Objects.requireNonNullElseGet(custom, () -> new DefaultTranslogDeletionPolicy(...));
}
```

**DFANRT:775-785:** Identical logic, but instance method with no args.

**Duplication: ~10 LOC, IDENTICAL.**

### (j) index/delete/noOp translog-only stubs

**NRT:244-278 (index+delete+noOp):** Each follows: ensureOpen → create Result → translogManager.add → setTranslogLocation → setTook → freeze → advanceMaxSeqNo.
**DFANRT:393-426:** Identical code (with `Engine.` prefix on inner types since not inside Engine).

**Duplication: ~30 LOC total (3 × ~10 LOC), IDENTICAL.**

### (k) onSettingsChanged

**NRT:524-528:** Gets deletion policy, sets retention age and size.
**DFANRT:695-699:** Identical.

**Duplication: ~4 LOC, IDENTICAL.**

### (l) flushAndClose / close / awaitPendingClose

**Engine:2086-2125 (inherited by NRT):** flushAndClose acquires writeLock, flushes, closes. close acquires writeLock, calls closeNoLock. awaitPendingClose awaits latch.
**DFANRT:608-614 + 720-737 + 756-761:** Identical logic, manually implemented.

**Duplication: ~25 LOC, IDENTICAL.**

### (m) failEngine

**Engine:1288-1370 (inherited by NRT):** Complex: maybeDie, tryLock, set failedEngine, closeNoLock, log, mark corrupt, cleanup unreferenced files.
**DFANRT:740-762:** Simpler: tryLock, check failedEngine already set, set failedEngine, closeNoLock, log, notify eventListener. No maybeDie, no corruption marking (done in closeNoLock).

**Duplication: ~15 LOC of shared structure. The Engine version is significantly more complex. DIFFERENT enough that extraction is risky.**

## 6. EXTRACTION OPTIONS

### 6.1 index/delete/noOp translog-only stubs (~30 LOC)

- **Abstract base class**: Infeasible (NRT extends Engine, DFANRT implements Indexer).
- **Utility class with static methods**: YES — `NRTReplicaTranslogOps.index(ensureOpenFn, translogManager, localCheckpointTracker, index)` etc. Clean contracts, no state coupling.
- **Shared helper object (composition)**: Possible but overkill for stateless operations.
- **Default methods on Indexer**: Would require Indexer to know about translogManager/localCheckpointTracker. Pollutes the interface.
- **BEST**: Static utility methods in a new `NRTReplicaTranslogOps` class.

### 6.2 Flush method body (~20 LOC)

- **Abstract base class**: Infeasible.
- **Utility class with static methods**: YES — `NRTReplicaFlushHelper.flush(ensureOpenFn, engineConfig, readLock, flushLock, shardId, commitAction, maybeFailEngineFn, force, waitIfOngoing)`. The commit action is passed as an `IORunnable`.
- **Shared helper object**: Possible but the method is self-contained.
- **Default methods on Indexer**: Too many dependencies to pass.
- **BEST**: Static utility method. The flush body is a pure algorithm over injected dependencies.

### 6.3 TranslogEventListener (~12 LOC)

- **Utility class with static methods**: YES — factory method `NRTReplicaTranslogOps.createTranslogEventListener(failEngineFn, translogManagerSupplier, shardId)`.
- **BEST**: Static factory method in the same `NRTReplicaTranslogOps` class.

### 6.4 TranslogDeletionPolicy factory (~10 LOC)

- **Already exists in Engine** as `getTranslogDeletionPolicy(EngineConfig)`. DFANRT duplicates it because it doesn't extend Engine.
- **Utility class**: YES — move to a static method accessible to both.
- **BEST**: Static method `NRTReplicaTranslogOps.getTranslogDeletionPolicy(EngineConfig)`. Engine's existing method can delegate to it.

### 6.5 closeNoLock pattern (~25 LOC shared structure)

- **Utility class**: Partially — the mark-corrupt-on-failure block (~8 LOC) is extractable. The overall close orchestration differs too much (different commit calls, different closeables).
- **Shared helper object**: A `CloseHelper` that takes an `IORunnable commitAction` and `List<Closeable> closeables` could work but adds indirection for marginal gain.
- **BEST**: Extract only the mark-corrupt-on-failure block as a static method. Leave the rest as-is.

### 6.6 flushAndClose / close / awaitPendingClose (~25 LOC)

- **Already in Engine** for NRT. DFANRT re-implements because it doesn't extend Engine.
- **Utility class**: YES — `NRTReplicaLifecycleHelper.close(isClosed, writeLock, closeNoLockFn)`, `flushAndClose(isClosed, writeLock, flushFn, closeFn, awaitFn)`, `awaitPendingClose(closedLatch)`.
- **Default methods on Indexer**: `close()` and `flushAndClose()` could be default methods on Indexer IF the interface exposed `isClosed()`, `writeLock()`, etc. This would pollute the interface.
- **BEST**: Static utility methods. These are pure orchestration over injected dependencies.

### 6.7 onSettingsChanged (~4 LOC)

- Too small to extract. **Leave as-is.**

### 6.8 ensureOpen (~3 LOC)

- Too small to extract. Already in Engine for NRT. DFANRT must implement LifecycleAware.ensureOpen(). **Leave as-is.**

### 6.9 failEngine (~15 LOC shared)

- Engine's version is significantly more complex (maybeDie, corruption marking). DFANRT's is simpler. Extracting a common version would either lose Engine's extra safety or force DFANRT to adopt it.
- **BEST**: Leave as-is. The implementations serve different safety profiles.

### 6.10 Flush-on-generation-change guard (~6 LOC)

- Part of updateSegments/updateCatalogSnapshot. Too small and too tightly coupled to the surrounding method to extract standalone.
- **BEST**: Leave as-is.

### 6.11 getMergeStats (~3 LOC)

- Too small. **Leave as-is.**

## 7. PROPOSED EXTRACTIONS

### Extraction 1: `NRTReplicaTranslogOps` — translog operation stubs + event listener + deletion policy

- **Name**: `NRTReplicaTranslogOps`
- **Location**: `server/src/main/java/org/opensearch/index/engine/NRTReplicaTranslogOps.java`
- **From NRT**: index() body (NRT:244-254), delete() body (NRT:256-266), noOp() body (NRT:268-278), TranslogEventListener creation (NRT:119-133).
- **From DFANRT**: index() body (DFANRT:393-402), delete() body (DFANRT:405-414), noOp() body (DFANRT:417-426), TranslogEventListener creation (DFANRT:201-216), getTranslogDeletionPolicy() (DFANRT:775-785).
- **API surface**:
  ```java
  public final class NRTReplicaTranslogOps {
      public static Engine.IndexResult index(
          Runnable ensureOpen, WriteOnlyTranslogManager translogManager,
          LocalCheckpointTracker tracker, Engine.Index index) throws IOException;
      public static Engine.DeleteResult delete(
          Runnable ensureOpen, WriteOnlyTranslogManager translogManager,
          LocalCheckpointTracker tracker, Engine.Delete delete) throws IOException;
      public static Engine.NoOpResult noOp(
          Runnable ensureOpen, WriteOnlyTranslogManager translogManager,
          LocalCheckpointTracker tracker, Engine.NoOp noOp) throws IOException;
      public static TranslogEventListener createTranslogEventListener(
          BiConsumer<String, Exception> failEngine,
          Supplier<WriteOnlyTranslogManager> translogManagerSupplier, ShardId shardId);
      public static TranslogDeletionPolicy getTranslogDeletionPolicy(EngineConfig config);
  }
  ```
- **Risk/Complexity**: LOW. Pure functions, no state. Easy to test in isolation.
- **Test impact**: Existing NRTReplicationEngineTests and any DFANRT tests continue to pass. Add unit tests for the static methods. NRT's index/delete/noOp become one-line delegations.

### Extraction 2: `NRTReplicaFlushHelper` — flush orchestration

- **Name**: `NRTReplicaFlushHelper`
- **Location**: `server/src/main/java/org/opensearch/index/engine/NRTReplicaFlushHelper.java`
- **From NRT**: flush(boolean, boolean) body (NRT:380-405).
- **From DFANRT**: flush(boolean, boolean) body (DFANRT:533-556).
- **API surface**:
  ```java
  public final class NRTReplicaFlushHelper {
      @FunctionalInterface
      public interface IORunnable { void run() throws IOException; }

      public static void flush(
          Runnable ensureOpen, EngineConfig engineConfig, ReleasableLock readLock,
          Lock flushLock, ShardId shardId, IORunnable commitAction,
          BiConsumer<String, Exception> maybeFailEngine,
          boolean force, boolean waitIfOngoing);
  }
  ```
- **Risk/Complexity**: LOW. The flush body is a self-contained algorithm. Both callers pass their respective commit action.
- **Test impact**: Minimal. Flush behavior is already tested via engine-level tests. Add a unit test for the helper.

### Extraction 3: `NRTReplicaLifecycleHelper` — close/flushAndClose/awaitPendingClose + mark-corrupt

- **Name**: `NRTReplicaLifecycleHelper`
- **Location**: `server/src/main/java/org/opensearch/index/engine/NRTReplicaLifecycleHelper.java`
- **From NRT**: close() is inherited from Engine — no change needed for NRT.
- **From DFANRT**: close() (DFANRT:608-614), flushAndClose() (DFANRT:720-737), awaitPendingClose() (DFANRT:756-761), mark-corrupt block from closeNoLock (DFANRT:636-644).
- **API surface**:
  ```java
  public final class NRTReplicaLifecycleHelper {
      public static void close(
          AtomicBoolean isClosed, ReleasableLock writeLock,
          Consumer<String> closeNoLock, Runnable awaitPendingClose);
      public static void flushAndClose(
          AtomicBoolean isClosed, Logger logger, ReleasableLock writeLock,
          Runnable flush, Closeable closeAction, Runnable awaitPendingClose);
      public static void awaitPendingClose(CountDownLatch closedLatch);
      public static void markStoreCorruptedOnCommitFailure(
          IOException commitException, ReentrantLock failEngineLock,
          Store store, Logger logger);
  }
  ```
- **Risk/Complexity**: MED. close/flushAndClose are lifecycle-critical paths. The mark-corrupt extraction is LOW risk.
- **Test impact**: DFANRT's close/flushAndClose become delegations. NRT is unaffected (uses Engine's versions). Engine could optionally delegate to these too in a future cleanup, but that's out of scope.

## 8. NON-GOALS

| Item | Why NOT extract |
|------|----------------|
| **Shared abstract base class** | NRT extends Engine; DFANRT implements Indexer. A common base would require one to change its inheritance, breaking existing contracts with IndexShard and the rest of OpenSearch. The cost far exceeds the benefit. |
| **ensureOpen()** | 3 LOC. Already provided by Engine for NRT. DFANRT must implement LifecycleAware. No shared abstraction saves meaningful code. |
| **failEngine()** | Engine's version (inherited by NRT) includes maybeDie, corruption marking, unreferenced file cleanup. DFANRT's version is intentionally simpler. Unifying them would either regress DFANRT's simplicity or lose Engine's safety features. |
| **maybeFailEngine()** | DFANRT's version has extra translog-tragic-event checks that Engine's doesn't. They serve different failure models. ~5 LOC each. |
| **onSettingsChanged()** | 4 LOC. Identical but too small to justify a helper. |
| **getMergeStats()** | 3 LOC. Trivial. |
| **getHistoryUUID()** | Different implementations: NRT uses Engine.loadHistoryUUID on SegmentInfos; DFANRT reads from CatalogSnapshot. No common code. |
| **Constructor bootstrap** | The constructors share the WriteOnlyTranslogManager creation call, but the surrounding bootstrap logic (SegmentInfos vs CatalogSnapshot, ReplicaFileTracker vs CatalogSnapshotManager, reader manager creation) is fundamentally different. Extracting the translog creation alone saves ~15 LOC but requires passing 12+ parameters — net negative readability. The TranslogEventListener factory (Extraction 1) is the only clean piece to extract. |
| **updateSegments / updateCatalogSnapshot** | The flush-on-gen-change guard is ~6 LOC of shared logic embedded in methods with different reader-update and listener-notification logic. Extracting it would require passing 5+ parameters for 6 lines of code. Leave as-is. |
| **acquireLastIndexCommit** | Structurally similar but pinning mechanisms are fundamentally different (ReplicaFileTracker vs CatalogSnapshotManager). No clean shared abstraction. |
| **closeNoLock overall structure** | The commit call and closeables list differ. Only the mark-corrupt block is cleanly extractable (included in Extraction 3). |
| **Trivial no-op stubs** (activateThrottling, deactivateThrottling, fillSeqNoGaps, maybePruneDeletes, etc.) | 1 LOC each. No value in extraction. |
| **commitSegmentInfos / commitCatalogSnapshot** | Fundamentally different: one commits real SegmentInfos with file tracking; the other builds synthetic SegmentInfos with serialized CatalogSnapshot. Both end with `store.commitSegmentInfos()` + `translogManager.syncTranslog()` but the setup is completely different. |

## 9. IMPLEMENTATION PHASES

### Phase 1: `NRTReplicaTranslogOps` (safest, smallest)

**Scope**: Extract index/delete/noOp stubs, TranslogEventListener factory, TranslogDeletionPolicy factory.

**Steps**:
1. Create `NRTReplicaTranslogOps.java` with 5 static methods.
2. Update NRT's `index()`, `delete()`, `noOp()` to delegate to static methods.
3. Update DFANRT's `index()`, `delete()`, `noOp()` to delegate.
4. Update NRT constructor to use `createTranslogEventListener()`.
5. Update DFANRT constructor to use `createTranslogEventListener()`.
6. Update DFANRT's `getTranslogDeletionPolicy()` to delegate to static method.
7. Optionally update Engine's `getTranslogDeletionPolicy()` to delegate (low risk but wider blast radius — can defer).
8. Add unit tests for `NRTReplicaTranslogOps`.
9. Verify existing NRTReplicationEngineTests pass.

**Risk**: LOW. All changes are mechanical delegations. No behavioral change.

### Phase 2: `NRTReplicaFlushHelper` (low risk, moderate scope)

**Scope**: Extract flush orchestration.

**Steps**:
1. Create `NRTReplicaFlushHelper.java` with `flush()` static method and `IORunnable` interface.
2. Update NRT's `flush(boolean, boolean)` to delegate, passing `this::commitSegmentInfos` as commitAction.
3. Update DFANRT's `flush(boolean, boolean)` to delegate, passing `this::commitCatalogSnapshot` as commitAction.
4. Add unit test for `NRTReplicaFlushHelper`.
5. Verify existing tests pass.

**Risk**: LOW. Flush is well-tested. The extraction is a pure refactor.

### Phase 3: `NRTReplicaLifecycleHelper` (moderate risk, lifecycle-critical)

**Scope**: Extract close/flushAndClose/awaitPendingClose for DFANRT, plus mark-corrupt helper.

**Steps**:
1. Create `NRTReplicaLifecycleHelper.java` with 4 static methods.
2. Update DFANRT's `close()`, `flushAndClose()`, `awaitPendingClose()` to delegate.
3. Extract mark-corrupt block from DFANRT's `closeNoLock()` to `markStoreCorruptedOnCommitFailure()`.
4. Optionally extract same block from NRT's `closeNoLock()` (it's inherited from Engine, so this would mean changing Engine — defer unless desired).
5. Add unit tests.
6. Verify existing tests pass.

**Risk**: MED. These are lifecycle-critical paths. The close/flushAndClose methods are straightforward delegations, but any bug here causes data loss or hung shards. Thorough testing required.

## 10. NET LOC IMPACT

| Phase | New File LOC | Lines Deleted (NRT) | Lines Deleted (DFANRT) | Net Change |
|-------|-------------|--------------------|-----------------------|------------|
| Phase 1: NRTReplicaTranslogOps | +80 (class + 5 methods + javadoc) | -25 (index/delete/noOp bodies become 1-liners; TranslogEventListener inlined → factory call) | -35 (same + getTranslogDeletionPolicy body) | **+80 -60 = +20** |
| Phase 2: NRTReplicaFlushHelper | +40 (class + 1 method + IORunnable + javadoc) | -15 (flush body → 1-line delegation) | -18 (flush body → 1-line delegation) | **+40 -33 = +7** |
| Phase 3: NRTReplicaLifecycleHelper | +60 (class + 4 methods + javadoc) | 0 (NRT uses Engine's versions) | -35 (close + flushAndClose + awaitPendingClose + mark-corrupt → delegations) | **+60 -35 = +25** |
| **Total** | **+180** | **-40** | **-88** | **+52 net** |

**Assessment**: The net LOC increases by ~52 lines. This is expected — extracting shared logic into utility classes adds boilerplate (class declarations, javadoc, parameter lists). The value is NOT in LOC reduction but in:

1. **Single source of truth** for translog-only index/delete/noOp (~30 LOC of identical logic maintained in one place).
2. **Single source of truth** for flush orchestration (~20 LOC).
3. **Reduced risk of drift** as these engines evolve independently.
4. **Testability** — the extracted methods are independently unit-testable.

If the team values LOC reduction over drift prevention, **only Phase 1 is clearly worth doing** — it eliminates 30 LOC of identical translog stubs and the TranslogEventListener duplication, which are the most likely to drift. Phases 2 and 3 are marginal and could be deferred.

### Honest Bottom Line

The duplication between these two files is **moderate but not severe**. The truly identical code is concentrated in:
- index/delete/noOp stubs (~30 LOC)
- TranslogEventListener (~12 LOC)
- flush body (~20 LOC)
- TranslogDeletionPolicy factory (~10 LOC)
- close/flushAndClose/awaitPendingClose (~25 LOC, but NRT inherits from Engine)

Total extractable identical code: **~97 LOC**. Much of the remaining "near-identical" code (closeNoLock, updateSegments/updateCatalogSnapshot, constructor) has enough structural differences that extraction would require contorted abstractions. The pragmatic recommendation is **Phase 1 only** (translog ops + event listener + deletion policy), with Phase 2 (flush) as a nice-to-have.
