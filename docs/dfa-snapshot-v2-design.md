# DFA × Snapshot V2 — Design Doc

**Status:** Draft
**Owner:** Kamal (`askkamal`)
**Scope:** Make Data Format Aware (DFA) indices (the `pluggable-dataformat` feature) work end-to-end with Snapshot V2 — without altering the existing Lucene-only snapshot/restore path.

---

## 1. Problem statement

Snapshot V2 (shallow-v2) already works for Lucene-only remote-store indices: it pins a timestamp, relies on a single metadata-file reference to protect all blobs, and restores/deletes via `RemoteSegmentStoreDirectory`.

DFA indices store files in **format-specific sibling containers** (`segments/data/`, `segments/parquet/`, …) routed by `FormatBlobRouter`. Upload, refresh-time GC, and metadata tracking are already format-aware and V2-compatible.

**Two code paths lose format-awareness** because they call `RemoteSegmentStoreDirectoryFactory.newDirectory(...)` overloads that pass `null` for `IndexSettings`:

| Path | Consequence |
|------|-------------|
| V2 restore — builds source `RemoteSegmentStoreDirectory` from snapshot metadata | Downloads of parquet blob keys fail (`NoSuchFileException`) — directory only sees `segments/data/` |
| V2 cleanup — builds `RemoteSegmentStoreDirectory` to delete after snapshot deletion | `segments/parquet/` (and any format sibling) orphaned in the remote store — storage leak per deletion |

Both manifest **only for DFA-enabled indices**. Lucene indices are unaffected.

---

## 2. Goals & non-goals

### Goals
1. DFA indices can be restored from V2 snapshots with all format files intact.
2. Deleting a V2 snapshot that is the last reference to a DFA index fully cleans its remote-store footprint (no orphan sibling containers).
3. Every existing Lucene-only code path produces **byte-identical behavior** before and after this change — no new method calls on the Lucene-only hot path.
4. SOLID: no speculative abstractions. Minimal, targeted parameter plumbing.

### Non-goals
- Does not change V1 (legacy shallow-copy) semantics.
- Does not change the factory's format-awareness decision rule — still gated on `indexSettings.isPluggableDataFormatEnabled()`.
- Does not introduce a new index setting, metadata field, or migration.
- Does not change `RemoteSegmentMetadata`, `UploadedSegmentMetadata`, or upload-time behavior. These are already V2-compatible.

---

## 3. Background (condensed)

### 3.1 Remote layout
```
<repo-base>/<index-uuid>/<shard>/segments/
├── data/           ← Lucene files + metadata_<N>_<TS>_<UUID>...  (basePath)
├── parquet/        ← DFA sibling: parquet segment blobs
└── <format>/       ← one sibling per registered format
```

### 3.2 Metadata already covers DFA
`RemoteSegmentStoreDirectory.uploadMetadata(...)` writes a **single** metadata file that references files via `UploadedSegmentMetadata.originalFilename`. For DFA files this preserves the format prefix (`"parquet/_0.pqt"`). A pinned timestamp protecting the metadata file transitively protects every blob it references, across every sibling container. **No changes needed here.**

### 3.3 Factory behavior (the gate)
```java
// RemoteSegmentStoreDirectoryFactory.java:150
RemoteDirectory dataDirectory = indexSettings != null && indexSettings.isPluggableDataFormatEnabled()
    ? new DataFormatAwareRemoteDirectory(...)   // format-aware: routes to siblings
    : new RemoteDirectory(...);                 // plain: knows only basePath
```
The fix is to ensure `indexSettings` reaches this branch on both the restore and cleanup paths. For every other caller that didn't provide `indexSettings` before, we stay exactly on the `RemoteDirectory` branch — **Lucene flow unchanged**.

### 3.4 `PLUGGABLE_DATAFORMAT_ENABLED_SETTING` is persisted
Declared `Property.IndexScope, Property.Final` → travels with `IndexMetadata` into the snapshot and out on restore. Nothing new to serialize.

---

## 4. Design

### 4.1 Architectural principle
Fix the data flow, not the structure. Both gap sites already have `IndexMetadata` (via `prevIndexMetadata`) or `IndexSettings` in scope. The factory already exposes the full 8-argument overload accepting `IndexSettings`. The only change is: **use the correct overload, and thread `IndexSettings` one level deeper when necessary.**

### 4.2 SOLID mapping

| Principle | Application |
|-----------|-------------|
| **Single Responsibility** | `RemoteSegmentStoreDirectoryFactory` already owns the "should this be format-aware?" decision. We don't duplicate that logic elsewhere — we just feed it the input it needs. |
| **Open/Closed** | We add new method overloads or optional parameters; existing method signatures remain callable. Lucene callers don't change. |
| **Liskov Substitution** | `DataFormatAwareRemoteDirectory extends RemoteDirectory`. Every V2 path already holds a `Directory`/`RemoteDirectory` reference and doesn't care which subtype. No `instanceof` dispatch required. |
| **Interface Segregation** | No new interface. Existing `RemoteSegmentStoreDirectoryFactory` surface expands by one additional parameter on two `static` helpers. |
| **Dependency Inversion** | Callers depend on the factory abstraction; the format-awareness decision stays behind that abstraction. |

### 4.3 Change surface

Three small, localized changes. Everything else is derivative.

#### Change A — Restore path
Switch to the 8-arg factory overload and build `IndexSettings` from the already-in-scope `prevIndexMetadata`.

**File:** `server/src/main/java/org/opensearch/index/shard/StoreRecovery.java`
**Method:** `recoverShallowSnapshotV2(...)`

Before:
```java
RemoteSegmentStoreDirectory sourceRemoteDirectory = (RemoteSegmentStoreDirectory) directoryFactory.newDirectory(
    remoteSegmentStoreRepository,
    prevIndexMetadata.getIndexUUID(),
    shardId,
    remoteStorePathStrategy,
    null,
    RemoteStoreUtils.isServerSideEncryptionEnabledIndex(prevIndexMetadata)
);
```

After:
```java
final IndexSettings prevIndexSettings = new IndexSettings(prevIndexMetadata, nodeSettings);
RemoteSegmentStoreDirectory sourceRemoteDirectory = (RemoteSegmentStoreDirectory) directoryFactory.newDirectory(
    remoteSegmentStoreRepository,
    prevIndexMetadata.getIndexUUID(),
    shardId,
    remoteStorePathStrategy,
    null,                                                                 // indexFixedPrefix
    RemoteStoreUtils.isServerSideEncryptionEnabledIndex(prevIndexMetadata),
    false,                                                                // isWarmIndex
    prevIndexSettings                                                     // ← format-awareness gate
);
```

The same two-line adjustment applies to `recoverFromSnapshotAndRemoteStore` (V1 shallow restore), but V1 is a non-goal for correctness here — defer unless easily bundled.

#### Change B — Cleanup helper signature
Add `IndexSettings` to the static cleanup helper so it can reach the factory's format-aware branch.

**File:** `server/src/main/java/org/opensearch/index/store/RemoteSegmentStoreDirectory.java`
**Methods:** `remoteDirectoryCleanup`, `remoteDirectoryCleanupAsync`

Before:
```java
public static void remoteDirectoryCleanup(
    RemoteSegmentStoreDirectoryFactory remoteDirectoryFactory,
    String remoteStoreRepoForIndex,
    String indexUUID,
    ShardId shardId,
    RemoteStorePathStrategy pathStrategy,
    boolean forceClean
) { ... newDirectory(repo, uuid, shardId, pathStrategy); ... }
```

After:
```java
public static void remoteDirectoryCleanup(
    RemoteSegmentStoreDirectoryFactory remoteDirectoryFactory,
    String remoteStoreRepoForIndex,
    String indexUUID,
    ShardId shardId,
    RemoteStorePathStrategy pathStrategy,
    boolean forceClean,
    @Nullable IndexSettings indexSettings     // ← new, nullable to preserve callers
) {
    RemoteSegmentStoreDirectory dir = (RemoteSegmentStoreDirectory) remoteDirectoryFactory.newDirectory(
        remoteStoreRepoForIndex, indexUUID, shardId, pathStrategy,
        null, false, false, indexSettings
    );
    ...
}
```

`indexSettings` is **nullable on purpose**:
- When null → factory takes the plain-`RemoteDirectory` branch → identical to today's behavior.
- When non-null & DFA-enabled → factory takes the format-aware branch → siblings get cleaned.

This preserves every existing Lucene call site's behavior without requiring them to change.

#### Change C — V2 cleanup caller supplies `IndexSettings`
**File:** `server/src/main/java/org/opensearch/repositories/blobstore/BlobStoreRepository.java`
**Method:** `cleanRemoteStoreDirectoryIfNeeded(...)` (and the `remoteDirectoryCleanupAsync` call site it drives)

Before:
```java
remoteDirectoryCleanupAsync(
    remoteSegmentStoreDirectoryFactory, threadPool,
    remoteStoreRepository, prevIndexMetadata.getIndexUUID(),
    shard, ThreadPool.Names.REMOTE_PURGE, remoteStorePathStrategy, forceClean
);
```

After:
```java
final IndexSettings prevIndexSettings = new IndexSettings(prevIndexMetadata, clusterService.getSettings());
remoteDirectoryCleanupAsync(
    remoteSegmentStoreDirectoryFactory, threadPool,
    remoteStoreRepository, prevIndexMetadata.getIndexUUID(),
    shard, ThreadPool.Names.REMOTE_PURGE, remoteStorePathStrategy, forceClean,
    prevIndexSettings                                // ← new
);
```

`clusterService` and `prevIndexMetadata` are both already in scope in the enclosing method; no new dependencies.

### 4.4 What does not change

Must be preserved bit-for-bit:

| Area | Why untouched |
|------|---------------|
| `RemoteSegmentStoreDirectory` public API | Only one new **overload** via an extra parameter in static helpers; old entry points still exist. |
| Any caller path that today calls the 4-arg / 6-arg factory overloads | Their `indexSettings == null` → same plain-`RemoteDirectory` selection as before. |
| `uploadMetadata`, refresh-time upload, GC via `deleteStaleSegments` | Already V2-compatible, already format-aware where required. |
| `RemoteSegmentMetadata`, `UploadedSegmentMetadata` on-disk format | No change — format prefix in `originalFilename` already does the job. |
| V1 snapshot creation, restore, and deletion | Out of scope. |
| Snapshot creation path (`createSnapshotV2`) | Unchanged — metadata writes already cover DFA files. |

---

## 5. Implementation plan

Sequenced into three commits so each is independently reviewable and revertable.

### Step 1 — Static helper signature (additive)
- Extend `RemoteSegmentStoreDirectory.remoteDirectoryCleanup` and `remoteDirectoryCleanupAsync` with a new nullable `IndexSettings` parameter.
- Add an internal overload preserving the old signature that delegates with `null`.
- Update internal tests that instantiate these helpers to pass `null` (byte-identical to today).

**Verification:** All existing tests pass without modification beyond accepting the new overload.

### Step 2 — Cleanup caller plumbing
- `BlobStoreRepository.cleanRemoteStoreDirectoryIfNeeded` builds `IndexSettings` from `prevIndexMetadata` and passes it to `remoteDirectoryCleanupAsync`.
- Add one integration test: delete a DFA snapshot → verify `segments/parquet/` is deleted from the repo blob store.

**Verification:** Lucene-only snapshot delete integration tests unchanged and green. New DFA deletion test green.

### Step 3 — Restore caller plumbing
- `StoreRecovery.recoverShallowSnapshotV2` builds `IndexSettings` from `prevIndexMetadata` and calls the 8-arg factory overload.
- Add one integration test: create DFA index → V2 snapshot → delete index → restore → verify searches over parquet data succeed.

**Verification:** Lucene-only V2 restore tests unchanged and green. New DFA restore test green.

### Optional Step 4 — V1 restore (defer unless trivial)
- Mirror Step 3 in `recoverFromSnapshotAndRemoteStore`.
- Behind the same setting, gated the same way. Only pursue if touching the file anyway.

---

## 6. Backward compatibility

### Lucene-only indices
Every Lucene-only code path reaches the factory via overloads where `indexSettings` was already `null`. The factory's ternary selects the plain `RemoteDirectory` branch today, and continues to select it after this change.

**Invariant:** `indexSettings == null || indexSettings.isPluggableDataFormatEnabled() == false` ⟹ plain `RemoteDirectory`, exactly as today.

### DFA indices
Today: silent breakage on V2 restore, silent storage leak on V2 delete.
After: correct behavior.

### Rollback
Each of the three commits is a pure additive or parameter-threading change. Reverting restores the pre-change behavior for DFA (broken again) without affecting Lucene.

### Feature-flag posture
Format-awareness is already gated at the IndexSettings layer via `FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG` plus `index.pluggable.dataformat.enabled`. No additional flag needed.

---

## 7. Testing strategy

### Unit
1. `RemoteSegmentStoreDirectoryFactoryTests` — assert the 8-arg overload with DFA-enabled `IndexSettings` returns a directory whose data component is a `DataFormatAwareRemoteDirectory`; with `null` or DFA-disabled settings, returns a plain `RemoteDirectory`. *(May already exist; extend if needed.)*
2. `RemoteSegmentStoreDirectoryTests` — verify `remoteDirectoryCleanup(..., indexSettings=null)` behaves identically to the old call.

### Integration
3. **V2 restore — DFA index**
   - Create DFA index with parquet-backed data
   - Take V2 snapshot
   - Delete index
   - Restore snapshot
   - Assert: parquet files present locally under `index/parquet/`, searches return the expected docs
4. **V2 delete — DFA index**
   - Create DFA index with parquet data, V2 snapshot, delete index (snapshot holds it)
   - Delete snapshot
   - Assert: both `segments/data/` and `segments/parquet/` containers are gone in the repo blob store
5. **V2 regression — Lucene-only**
   - Existing Lucene-only V2 snapshot/restore/delete tests continue to pass with zero modification.

### Negative
6. **DFA disabled on restore target** (same index was DFA at snapshot time, but cluster now has feature flag off)
   - Assert: restore gracefully fails with a clear error (format-aware paths cannot be constructed). Document this as an expected constraint.

---

## 8. Risks & mitigations

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Lucene-only regression from signature change | Low | Nullable `IndexSettings`; factory behavior identical when `null`. Existing tests catch. |
| `new IndexSettings(prevIndexMetadata, nodeSettings)` side effects during restore | Low | Construction is cheap and side-effect-free; we use it only to read `isPluggableDataFormatEnabled()`. Consider adding a lightweight `IndexSettingsSupplier` helper if construction proves non-trivial. |
| Registered format containers not cleaned if `registeredFormats()` is empty at cleanup time | Low | `DataFormatAwareRemoteDirectory.delete()` iterates `formatBlobRouter.registeredFormats()`. For V2 cleanup we never called `initializeToSpecificTimestamp` on this directory, so formats may be unregistered. **Action:** Have cleanup path either (a) call `init()` or `listAll()` to populate the cache before delete, or (b) derive formats from `IndexSettings.getPluggedDataFormat()` directly. Pick (b) — cheaper and deterministic. |
| `prevIndexMetadata.getSettings()` missing the flag (e.g., very old snapshots) | Low | Falls through to plain `RemoteDirectory` branch — same as today's behavior. No regression. |

### Risk detail: format registration at cleanup time

`DataFormatAwareRemoteDirectory` builds its `formatBlobRouter` registrations lazily as files are accessed. A fresh directory constructed purely for cleanup has no registrations, so its `delete()` would iterate an empty `registeredFormats()` set and delete nothing from siblings.

**Proposed fix inside Change B**: populate `formatBlobRouter` from `IndexSettings.getPluggedDataFormat()` at construction time. The setting already carries the format name. A deterministic, cheap seeding step inside `DataFormatAwareRemoteDirectory`'s constructor closes this loop. Add this as an additional bullet under Change B if verification confirms the gap — validate in Step 2 integration test.

---

## 9. Out of scope / future work

- **V1 shallow-copy restore path.** Same signature-plumbing fix could be applied to `recoverFromSnapshotAndRemoteStore`. Deferred unless demand emerges.
- **Cross-index format migration during restore.** Not in scope — if the target cluster's feature flag is off, restore of a DFA index is disallowed, not silently converted.
- **Multi-format shard cleanup optimization.** Current broadcast delete across all registered format containers is O(formats × files). For very large format counts this could be batched; not currently a problem.
- **Snapshot-time consistency check.** We assume `DataformatAwareCatalogSnapshot` and `uploadMetadata` agree on the file list at snapshot finalize. If they diverged mid-refresh, V2 pinning would protect a consistent metadata file view. Explicit sanity-check logging could be added if field failures emerge.

---

## 10. Open questions

1. Do we need to pass node-level `Settings` into `StoreRecovery`? Looking at the method signature, `nodeSettings` may not be in scope. If not, extend the method signature (internal, trivial) or plumb through `IndexService`. — *To verify during implementation.*
2. Does `cleanRemoteStoreDirectoryIfNeeded` have a `Settings` handle? `clusterService.getSettings()` is available from `BlobStoreRepository` fields. — *To verify.*

Both questions are implementation detail, not design risk.

---

## 11. Summary

- **Root cause:** Two callers of `RemoteSegmentStoreDirectoryFactory.newDirectory(...)` call overloads that drop `IndexSettings`, defeating the format-awareness gate.
- **Fix:** Plumb `IndexSettings` to those two call sites. Nullable parameter preserves every existing caller's behavior.
- **Lines of production code:** ~30 spread across 3 files. **Lines of test code:** ~200 for the two new integration tests.
- **SOLID:** No new abstraction. The factory already owns the decision; we just feed it the input it already knows how to consume.
- **Lucene flow impact:** Zero. Every pre-existing overload and caller path is preserved; behavior is byte-identical when `indexSettings` is null or DFA is disabled.
