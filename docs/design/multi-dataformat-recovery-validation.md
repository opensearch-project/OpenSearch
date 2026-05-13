# Multi-DataFormat Recovery Validation — Design Doc

**Status:** Draft v3 (typed-payload wire format; pre-rollout)
**Author:** Kamal
**Date:** 2026-05-11

---

## 1. Context & Problem

OpenSearch's remote-store recovery was designed around one on-disk format (Lucene). The DFA engine now allows multiple formats per index (Lucene, parquet, future), but recovery still has Lucene-specific paths hardcoded into core classes. This doc proposes a pluggable, format-agnostic recovery validation protocol, with a typed wire envelope that carries per-format state without tunneling through Lucene `SegmentInfos.userData`.

### Symptoms this design must address

1. **Phase 1 fix is Lucene-special-case.** Core classes (`RemoteStoreRefreshListener`, `RemoteSegmentStoreDirectory`, `Store`) did `instanceof DataFormatAwareEngine` and passed `SegmentInfos` through. Every new format would repeat this pattern.
2. **No per-format validation on recovery.** Parquet file corruption is undetected until first query. No schema-version check, no manifest verification, no integrity check.
3. **`Committer` interface leaks Lucene types.** `captureInMemorySegmentInfos()` was added in Phase 1 to unblock the bug — does not belong on a generic interface.
4. **Wire format tunneling.** Intermediate Phase 2 design carried the DFA catalog inside a fake Lucene `SegmentInfos` envelope via `userData`. Works, but forces every DFA upload/recovery through a Lucene framing layer even for parquet-only indices.

---

## 2. Goals & Non-Goals

### Goals
1. **Format-agnostic core.** `Store`/`RSRL`/`RemoteSegmentStoreDirectory` name no format-specific types.
2. **Pluggable validation per format** via an explicit contract.
3. **Backwards compatible** — existing Lucene-only (v1/v2) indices keep working with zero changes; rolling upgrades from pre-DFA clusters just work.
4. **SOLID compliance** verified section by section.
5. **Testable coordinators** — each unit-testable without a cluster.
6. **Typed wire format.** The DFA catalog and per-format state travel as first-class fields on the remote metadata, not tunneled through Lucene envelope bytes.
7. **Future-proof.** New formats plug in with zero core modifications. `RemoteSegmentMetadata` can evolve without bumping wire version when adding optional fields.

### Non-Goals
1. Re-architecting the DFA catalog layer.
2. Changing the remote-store wire format for Lucene-only indices (v1/v2 bytes remain readable).
3. Cross-shard / cross-index validation.
4. Replacing per-file checksum verification in `RemoteSegmentStoreDirectory`.
5. Changing the **local** `segments_N` on-disk layout — DFA catalog still lives in `segments_N.userData` locally (only available Lucene extension point). That's a local-persistence concern, not a wire-format concern.

---

## 3. Design Overview

Two concepts, one typed envelope:

1. **`FormatRecoveryCoordinator`** — a per-format plugin that participates in upload + recovery. Format-specific logic moves into coordinators; core orchestrates.
2. **`DfaRecoveryPayload`** — a typed struct on the remote metadata carrying the serialized catalog + per-format state map. Replaces the Phase 2 `SyntheticSegmentInfos` envelope.

```
  FormatRecoveryRegistry  ←  DataFormatRegistry (boot-time population)
         │
    ┌────┼───────────────┐
    ▼    ▼               ▼
  Lucene Parquet  (future)
  Coord  Coord    Coord

Upload (RSRL):
  for coord: captureForUpload() → bytes → formatStates[coord.name()]
  catalogBytes = catalogSnapshot.serialize()
  emit RemoteSegmentMetadata { ..., dfaPayload: { catalogBytes, formatStates } }

Recovery (syncSegmentsFromRemoteSegmentStore):
  if dfaPayload != null:
      catalog = deserialize(dfaPayload.catalogBytes)
      for fmt in catalog.getDataFormats():
          registry.get(fmt).onRecovery(dfaPayload.formatStates[fmt], ctx)
  else:
      // non-DFA Lucene path — unchanged
      commitSegmentInfos(buildSegmentInfos(segmentInfosBytes))
```

Core calls are format-agnostic. All casts, Lucene imports, and per-format logic live in coordinators. The wire format has one clear branch: DFA vs non-DFA.

---

## 4. Detailed Design

### 4.1 Core interfaces (server module)

#### `FormatRecoveryCoordinator`

```java
package org.opensearch.index.engine.exec.recovery;

/**
 * Per-format hook for the remote-store recovery protocol. Each format that needs to
 * contribute state for validation/reconstruction provides one implementation.
 *
 * Lifecycle: Singleton, stateless, thread-safe. Registered once at node boot via the
 * {@link org.opensearch.plugins.DataFormatPlugin}. Methods may be invoked concurrently
 * for different shards on the same node.
 *
 * Failure semantics: onRecovery throwing fails the shard recovery attempt; the cluster
 * retries via the standard allocation path. Coordinators MUST NOT perform partial
 * destructive writes — all changes idempotent or atomic.
 */
public interface FormatRecoveryCoordinator {

    /** The data format name this coordinator handles (e.g. "lucene", "parquet"). */
    String formatName();

    /**
     * Upload hook. Returns opaque bytes stored in the remote metadata alongside the
     * catalog. Returning null means "this format has nothing to contribute at upload
     * time" — valid and common.
     *
     * MUST NOT: modify the local store directory, trigger a commit, block indefinitely.
     * MUST: complete within the refresh time budget (~100ms typical).
     */
    byte[] captureForUpload(UploadContext context) throws IOException;

    /**
     * Recovery hook. Called exactly once per shard recovery, after remote files are
     * downloaded and the catalog is reconstructed from the typed payload. Bytes are
     * verbatim what captureForUpload returned.
     *
     * Typical responsibilities: verify files exist, validate integrity, reconstruct
     * format-specific on-disk structures (e.g. Lucene segments_N).
     *
     * MAY: call back into {@link RecoveryOps} for controlled mutations.
     * MUST: fail fast on unrecoverable corruption by throwing RecoveryValidationException.
     */
    void onRecovery(byte[] state, RecoveryContext context) throws IOException;
}
```

#### Context objects (immutable, ISP-compliant)

```java
public final class UploadContext {
    private final CatalogSnapshot snapshot;
    private final Directory storeDirectory;
    private final String formatName;
    private final Optional<Committer> committer;   // coordinators that need format-specific
                                                   // access (e.g. Lucene) cast and use
    // getters only; no IndexShard reference
}

public final class RecoveryContext {
    private final CatalogSnapshot snapshot;        // already-reconstructed DFA catalog
    private final Directory storeDirectory;
    private final Path shardDataPath;
    private final String formatName;
    private final RecoveryOps ops;                 // narrow mutation API
    // getters only
}
```

#### `RecoveryOps` — narrow mutation API for coordinators

```java
/**
 * Operations coordinators may perform during recovery. Deliberately narrow —
 * coordinators should not have arbitrary access to Store or IndexShard.
 *
 * Tracks whether any coordinator wrote segments_N so the caller can run a fallback
 * commit for DFA indices where no format owns segments_N (e.g. parquet-only).
 */
public interface RecoveryOps {
    /** Write a Lucene segments_N file from a SegmentInfos object. Idempotent. */
    void writeSegmentsN(SegmentInfos infos, long localCheckpoint) throws IOException;

    /** Delete stale segments_* files in the main Lucene index directory. */
    void deleteStaleSegmentsFiles() throws IOException;

    /** List files in a format's subdirectory (e.g. <shard>/parquet/). */
    String[] listFormatFiles(String formatName) throws IOException;

    /** True iff any coordinator has called {@link #writeSegmentsN} during this recovery. */
    boolean didWriteSegmentsN();

    /** Fail fast on corruption — wraps in RecoveryValidationException. */
    void failRecovery(String reason, Throwable cause);
}
```

#### `FormatRecoveryRegistry`

```java
public interface FormatRecoveryRegistry {
    Optional<FormatRecoveryCoordinator> get(String formatName);
    Collection<FormatRecoveryCoordinator> all();
    FormatRecoveryRegistry EMPTY = new EmptyFormatRecoveryRegistry();
}
```

Owned by **`DataFormatRegistry`** (node-level singleton). Accessed in `IndexShard` via `indexShard.getFormatRecoveryRegistry()`.

### 4.2 Plugin integration

```java
public interface DataFormatPlugin {
    // ... existing ...
    default FormatRecoveryCoordinator getRecoveryCoordinator() { return null; }
}
```

At boot, `DataFormatRegistry` collects non-null coordinators and builds `FormatRecoveryRegistry`. Composite plugins don't need a coordinator — they already dispatch to underlying formats at write time, so the catalog's `getDataFormats()` already returns the concrete format names.

### 4.3 Wire format (v3, typed payload)

`RemoteSegmentMetadata` v3:

```
version: int = 3
segmentFilesMap: Map<String, UploadedSegmentMetadata>
segmentInfosBytes: byte[]                    // non-DFA: real Lucene SegmentInfos bytes
                                             // DFA: empty (0 length)
dfaPayload: DfaRecoveryPayload | null        // NEW — typed payload for DFA indices
replicationCheckpoint: ReplicationCheckpoint
```

`DfaRecoveryPayload` (new typed struct):

```
class DfaRecoveryPayload {
    byte[] catalogSnapshotBytes;             // serialized DataformatAwareCatalogSnapshot
    long lastCommitGeneration;               // explicit; not inferred from envelope
    Map<String, byte[]> formatStates;        // opaque per-format state
    // room for optional fields: schemaVersion, featureFlags, etc.
    // added without bumping wire version via BWC-friendly codec
}
```

**Read path (full BWC):**

| Bytes read | Version | dfaPayload | Behavior |
|-----------|---------|------------|----------|
| v1 | 1 | null | Legacy Lucene path — `segmentInfosBytes` is real Lucene bytes |
| v2 | 2 | null | Same as v1 (v2 only added different fields, still Lucene-only semantics) |
| v3 non-DFA | 3 | null | `segmentInfosBytes` = real Lucene bytes (non-DFA write) |
| v3 DFA | 3 | present | `segmentInfosBytes` = empty; catalog + states in `dfaPayload` |

Key: the **presence of `dfaPayload`** is the DFA signal at the wire level. No instanceof-by-bytes, no userData snooping.

**Write path:**
- Writers always emit v3.
- Non-DFA: populate `segmentInfosBytes` from real Lucene SegmentInfos; `dfaPayload = null`.
- DFA: populate `dfaPayload = {catalogBytes, gen, formatStates}`; `segmentInfosBytes = empty`.

**Old-reader tolerance:**
- v2 readers encountering v3 bytes → fail with "unsupported version" (by design; rolling upgrades handle this via version gating in the cluster state).
- v3 readers encountering v1/v2 bytes → legacy non-DFA path, fully functional.

**Future evolution without version bump:**
- `DfaRecoveryPayload` serializes via a tag-length-value codec. New optional fields (e.g. `schemaVersion`, `encryption`, `compressionCodec`) appended at tail; older v3 readers skip unknown tags. Only a semantic breaking change would require v4.

### 4.4 Core flow — upload

```java
// RemoteStoreRefreshListener.uploadMetadata
Map<String, byte[]> formatStates = collectFormatStates(catalogSnapshot);   // iterate registry

remoteDirectory.uploadMetadata(
    files,
    catalogSnapshot,             // polymorphic: DFA or non-DFA
    formatStates,                // empty for non-DFA
    replicationCheckpoint,
    ...
);

// RemoteSegmentStoreDirectory.uploadMetadata
RemoteSegmentMetadata rm;
if (catalogSnapshot instanceof DataformatAwareCatalogSnapshot dfa) {
    DfaRecoveryPayload payload = new DfaRecoveryPayload(
        dfa.serialize(),                       // real catalog bytes, no Lucene envelope
        dfa.getLastCommitGeneration(),
        formatStates
    );
    rm = new RemoteSegmentMetadata(files, EMPTY_BYTES, payload, checkpoint);
} else {
    // non-DFA: carry real Lucene SegmentInfos bytes
    rm = new RemoteSegmentMetadata(files, catalogSnapshot.serialize(), null, checkpoint);
}
```

**`DataformatAwareCatalogSnapshot.serialize()` returns real catalog bytes directly** — no Lucene framing, no `SyntheticSegmentInfos` class involved. A self-describing binary format (magic + version + fields).

**No imports of `DataFormatAwareEngine` or Lucene `SegmentInfos` in `RSRL` or `RemoteSegmentStoreDirectory`.**

### 4.5 Core flow — recovery

```java
// IndexShard.syncSegmentsFromRemoteSegmentStore
RemoteSegmentMetadata rm = remoteDirectory.init();
copySegmentFiles(...);
deleteStaleSegmentsFiles();

if (rm.getDfaPayload() != null) {
    // DFA path — catalog comes from the typed payload, not from userData tunneling
    DataformatAwareCatalogSnapshot catalog =
        DataformatAwareCatalogSnapshot.deserialize(rm.getDfaPayload().getCatalogBytes());
    long localCheckpoint = catalog.getLocalCheckpoint();

    invokeRecoveryCoordinators(catalog, rm.getDfaPayload().getFormatStates(), localCheckpoint);
} else {
    // Non-DFA path — unchanged from pre-Phase-2
    SegmentInfos infos = store.buildSegmentInfos(rm.getSegmentInfosBytes(), rm.getGeneration());
    long localCheckpoint = Long.parseLong(infos.getUserData().get(LOCAL_CHECKPOINT_KEY));
    store.commitSegmentInfos(infos, localCheckpoint, localCheckpoint);
}
```

```java
// IndexShard.invokeRecoveryCoordinators
private void invokeRecoveryCoordinators(
    DataformatAwareCatalogSnapshot catalog,
    Map<String, byte[]> formatStates,
    long localCheckpoint
) throws IOException {
    StoreBackedRecoveryOps ops = new StoreBackedRecoveryOps(store, shardPath());
    FormatRecoveryRegistry registry = getFormatRecoveryRegistry();

    for (String formatName : catalog.getDataFormats()) {
        byte[] state = formatStates.get(formatName);
        if (state == null) continue;
        Optional<FormatRecoveryCoordinator> coord = registry.get(formatName);
        if (coord.isEmpty()) {
            logger.warn("format [{}] present in payload but no coordinator registered; skipping",
                        formatName);
            continue;
        }
        RecoveryContext ctx = new RecoveryContext(
            catalog, store.directory(), shardPath().getDataPath(), formatName, ops);
        coord.get().onRecovery(state, ctx);   // throws to fail shard
    }

    // Fallback: if no coord wrote segments_N (e.g. parquet-only DFA), write an empty
    // synthetic one from the catalog so the engine can open. Exactly one write either way.
    if (!ops.didWriteSegmentsN()) {
        SegmentInfos synthetic = buildEmptySegmentsForCatalog(catalog);
        ops.writeSegmentsN(synthetic, localCheckpoint);
    }
}
```

**One `segments_N` write per recovery** for every index type (non-DFA, DFA+Lucene, DFA parquet-only). No double-writes, no wasted fsync.

**`buildEmptySegmentsForCatalog`** is local to `IndexShard` and produces a minimal `SegmentInfos` with the catalog string in userData (for local persistence — see §4.6). This is the ONLY place that mints the local envelope, and it's not on the wire.

**Cleanup on failure:** if any coordinator throws, the recovery attempt fails. Cluster state allocation will retry the shard. On retry, all local files are re-downloaded (unless `localDirectoryContains` matches), so partial coordinator writes from the failed attempt don't accumulate. Coordinator implementations MUST make their writes idempotent.

### 4.6 Local persistence — `segments_N.userData` (unchanged)

**Distinct from wire format.** After recovery, the Lucene coordinator merges the catalog into `segments_N.userData` before writing to disk:

```java
// LuceneFormatRecoveryCoordinator.onRecovery
Map<String, String> userData = new HashMap<>(infos.getUserData());
userData.putAll(ctx.snapshot().getUserData());
userData.put(CATALOG_SNAPSHOT_KEY, ctx.snapshot().serializeToString());
infos.setUserData(userData, false);
ctx.ops().writeSegmentsN(infos, localCheckpoint);
```

**Why:** when the node restarts locally (no remote fetch), `Engine` opens the index by reading `segments_N` and reconstructing the catalog from `userData`. Lucene's on-disk commit format offers no other per-commit extension point without introducing a sidecar file.

For parquet-only DFA, the fallback path in `invokeRecoveryCoordinators` mints an empty-synthetic `SegmentInfos` with the catalog in userData — same local-persistence trick, minted locally, never shipped on the wire.

**The wire never carries this synthetic SegmentInfos.** `SyntheticSegmentInfos` class deleted.

### 4.7 Concrete coordinators

#### `LuceneFormatRecoveryCoordinator` (`analytics-backend-lucene` plugin)

```java
public final class LuceneFormatRecoveryCoordinator implements FormatRecoveryCoordinator {
    public static final String FORMAT_NAME = "lucene";

    @Override public String formatName() { return FORMAT_NAME; }

    @Override
    public byte[] captureForUpload(UploadContext ctx) throws IOException {
        Optional<Committer> committer = ctx.committer();
        if (committer.isEmpty() || committer.get() instanceof LuceneCommitter == false) {
            return null;
        }
        LuceneCommitter lc = (LuceneCommitter) committer.get();
        SegmentInfos infos = lc.captureInMemorySegmentInfos();
        if (infos == null) return null;

        // State layout: [generation:long][SegmentInfos bytes]. Generation preserved for
        // SegmentInfos.readCommit validation during onRecovery.
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        ByteBuffersIndexOutput idx = new ByteBuffersIndexOutput(out, "lucene-upload", "lucene-upload");
        idx.writeLong(infos.getGeneration());
        infos.write(idx);
        return out.toArrayCopy();
    }

    @Override
    public void onRecovery(byte[] state, RecoveryContext ctx) throws IOException {
        SegmentInfos infos;
        try (ChecksumIndexInput in = toChecksumInput(state)) {
            long generation = in.readLong();
            infos = SegmentInfos.readCommit(ctx.storeDirectory(), in, generation);
        } catch (IOException e) {
            throw RecoveryValidationException.wrap(formatName(), e);
        }

        // Merge DFA catalog into userData for LOCAL segments_N persistence only.
        Map<String, String> userData = new HashMap<>(infos.getUserData());
        userData.putAll(ctx.snapshot().getUserData());
        userData.put(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, ctx.snapshot().serializeToString());
        infos.setUserData(userData, false);

        long localCheckpoint = parseLongOrDefault(userData, "local_checkpoint", -1);
        ctx.ops().deleteStaleSegmentsFiles();
        ctx.ops().writeSegmentsN(infos, localCheckpoint);  // sets wroteSegmentsN=true
    }
}
```

**Note:** `Committer.captureInMemorySegmentInfos()` is REMOVED from the core interface. The Lucene capture logic lives inside `LuceneCommitter` and is accessed by the coordinator via a cast (both live in the same plugin module — no public coupling leaks out).

#### `ParquetFormatRecoveryCoordinator` (`parquet-data-format` plugin)

Real validation via parquet footer magic bytes. Does not write `segments_N` — that's Lucene's responsibility when present, or the fallback's when parquet-only.

```java
public final class ParquetFormatRecoveryCoordinator implements FormatRecoveryCoordinator {
    private static final byte STATE_VERSION = 1;
    private static final byte[] PARQUET_MAGIC = {'P', 'A', 'R', '1'};

    @Override public String formatName() { return "parquet"; }

    @Override
    public byte[] captureForUpload(UploadContext ctx) throws IOException {
        int fileCount = ctx.snapshot().getSearchableFiles("parquet").size();
        return ByteBuffer.allocate(5).put(STATE_VERSION).putInt(fileCount).array();
    }

    @Override
    public void onRecovery(byte[] state, RecoveryContext ctx) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(state);
        byte version = buf.get();
        if (version != STATE_VERSION) {
            ctx.ops().failRecovery("parquet state v" + version + " unsupported", null);
        }
        int expectedCount = buf.getInt();

        int localCount = ctx.snapshot().getSearchableFiles("parquet").size();
        if (localCount != expectedCount) {
            ctx.ops().failRecovery(
                "parquet file count mismatch: expected=" + expectedCount + " local=" + localCount, null);
        }
        for (String fileName : ctx.ops().listFormatFiles("parquet")) {
            verifyParquetMagic(ctx.shardDataPath().resolve("parquet").resolve(fileName));
        }
    }

    private void verifyParquetMagic(Path file) throws IOException {
        try (FileChannel fc = FileChannel.open(file, StandardOpenOption.READ)) {
            long size = fc.size();
            if (size < 4) throw new CorruptIndexException("parquet file too small", file.toString());
            ByteBuffer magic = ByteBuffer.allocate(4);
            fc.read(magic, size - 4);
            if (!Arrays.equals(magic.array(), PARQUET_MAGIC)) {
                throw new CorruptIndexException("parquet footer magic mismatch", file.toString());
            }
        }
    }
}
```

---

## 5. SOLID Analysis

### Single Responsibility
| Class | Responsibility |
|-------|----------------|
| `FormatRecoveryCoordinator` | Orchestrate upload+recovery for one format |
| `DfaRecoveryPayload` | Typed transport struct — carries catalog + formatStates |
| `UploadContext` / `RecoveryContext` | Carry data; no logic |
| `FormatRecoveryRegistry` | Lookup only |
| `RecoveryOps` | Narrow mutations for coordinators, plus write-tracking |
| `DataformatAwareCatalogSnapshot` | Serializes catalog bytes; **no Lucene dependency** |
| Core `Store`/`RSRL`/`RSSD` | Orchestration; format-agnostic |

### Open/Closed
New format = new coordinator class + register via plugin. **Zero core modifications.** Wire format accepts new formats via the opaque `formatStates` map.

### Liskov
Registry treats all coordinators uniformly. No coordinator receives special handling. DFA and non-DFA `CatalogSnapshot` subclasses work through the same upload/recovery entry points.

### Interface Segregation
- Coordinators see `UploadContext`/`RecoveryContext` — narrow, data-only.
- Coordinators see `RecoveryOps` — narrow; no access to `Store` or `IndexShard`.
- `Committer` stays Lucene-free; Lucene capture lives inside `LuceneCommitter`, coordinator casts locally.
- `DataformatAwareCatalogSnapshot.serialize()` returns opaque bytes — no Lucene types in return signature.

### Dependency Inversion
- Core depends on `FormatRecoveryCoordinator` abstraction, not concretes.
- Registry is accessed via `IndexShard.getFormatRecoveryRegistry()` — abstraction.
- `RecoveryOps` abstracts coordinator-to-store operations.
- Wire format carries `DfaRecoveryPayload` as a typed field — no abstraction leak through Lucene serialization format.

---

## 6. Phased Rollout

| Phase | Scope | Status |
|-------|-------|--------|
| **Phase 1** | Emergency fix: Lucene-specific `luceneInMemoryInfos` param + `captureInMemorySegmentInfos` on `Committer`. Unblocks the IndexFileDeleter bug. | Done (landed earlier) |
| **Phase 2** | Introduce `FormatRecoveryCoordinator` + `FormatRecoveryRegistry` + registry integration + `LuceneFormatRecoveryCoordinator` + `ParquetFormatRecoveryCoordinator`. Wire format v3 with `formatStates` map; catalog still tunneled via `SegmentInfos.userData`. | Done |
| **Phase 3** | **Typed wire payload cleanup.** Add `DfaRecoveryPayload` struct; `DataformatAwareCatalogSnapshot.serialize()` returns real catalog bytes; delete `SyntheticSegmentInfos`; `RemoteSegmentMetadata.segmentInfosBytes` = empty for DFA; single `segments_N` write per recovery via `RecoveryOps.didWriteSegmentsN()`. | This design |
| **Phase 4** | (Future) Apply the same typed payload to `syncSegmentsFromGivenRemoteSegmentStore` (snapshot/PIT restore). | Deferred |

Phase 3 substeps:

| Step | Change | Risk |
|------|--------|------|
| 3.1 | Add `DfaRecoveryPayload` class + self-describing codec. Unit tests for roundtrip. | Low |
| 3.2 | Add `dfaPayload` field to `RemoteSegmentMetadata`, wire v3 with BWC for v1/v2. | Medium |
| 3.3 | Rewrite `DataformatAwareCatalogSnapshot.serialize()` to emit real catalog bytes (magic + version + fields). Add `deserialize(bytes)` static factory. Remove `SyntheticSegmentInfos` class. | Medium |
| 3.4 | `RemoteSegmentStoreDirectory.uploadMetadata`: branch on `instanceof DataformatAwareCatalogSnapshot`; populate `dfaPayload` for DFA, keep `segmentInfosBytes` for non-DFA. | Medium |
| 3.5 | `IndexShard.syncSegmentsFromRemoteSegmentStore`: branch on `rm.getDfaPayload() != null`; DFA goes through coordinators, non-DFA unchanged. | Medium |
| 3.6 | Add `RecoveryOps.didWriteSegmentsN()` + fallback commit in `invokeRecoveryCoordinators` for parquet-only DFA. | Low |
| 3.7 | Remove `Store.buildSegmentInfos` call from DFA upload path. Remove userData-based catalog extraction from DFA recovery path (non-DFA still uses it). | Low |
| 3.8 | Update unit + IT tests. Add v1/v2 BWC test with golden bytes. | Medium |

Each substep compiles and passes all tests; full DFA IT suite run between each.

---

## 7. Testing Strategy

### Unit
- Each coordinator: isolated tests with mocked `UploadContext`/`RecoveryContext`.
- `RecoveryOps`: in-memory implementation with write-tracking assertions.
- `FormatRecoveryRegistry`: register/lookup/iterate.
- `DfaRecoveryPayload`: serialization roundtrip; unknown-tag forward compat.
- `DataformatAwareCatalogSnapshot.serialize`/`deserialize`: roundtrip with varied catalog contents.
- Core upload/recovery: fake coordinators, assert invocation order and fallback behavior.

### Integration
- **Lucene-only regression:** existing ITs pass unchanged. Full remote-store recovery for a pure-Lucene index produces v3 bytes with `dfaPayload = null`, reads back correctly.
- **DFA Lucene-secondary:** `DataFormatAwareLuceneSecondaryRecoveryIT`. Coordinator runs, `segments_N` written once with real refs.
- **DFA parquet-only:** recovery produces `segments_N` via fallback; no Lucene coordinator runs.
- **Parquet corruption:** corrupt a parquet footer before recovery, ParquetCoord fails with `RecoveryValidationException`.
- **Full cluster restart:** 3-node restart of a DFA index with both formats — both coordinators validate; one `segments_N` write per shard.
- **Snapshot restore:** (Phase 4) typed-payload-aware restore of a DFA index.
- **Extensibility:** `TestFormatRecoveryCoordinator` roundtrips a random integer via a dummy plugin — proves end-to-end wiring.

### BWC matrix

| Write side | Read side | Expected |
|-----------|-----------|----------|
| v1 (legacy Lucene) | v3 reader | ✅ non-DFA path, no coord invoked |
| v2 (intermediate Lucene) | v3 reader | ✅ non-DFA path, no coord invoked |
| v3 non-DFA | v3 reader | ✅ non-DFA path, `segmentInfosBytes` used |
| v3 DFA | v3 reader | ✅ DFA path, `dfaPayload` used |
| v3 any | v1/v2 reader | ⚠️ rejected (by cluster-state version gating; not supported for rolling upgrade past v3) |

Golden-byte tests for v1/v2 preserved to lock in BWC.

---

## 8. Observability

### Metrics
- `recovery.coordinator.invocations{format=<name>, phase=<upload|recovery>}` — counter
- `recovery.coordinator.duration_ms{format=<name>, phase=<upload|recovery>}` — histogram
- `recovery.coordinator.failures{format=<name>, reason=<missing_state|corrupt|io>}` — counter
- `recovery.coordinator.state_bytes{format=<name>}` — gauge
- `recovery.segments_n_writes_per_shard` — gauge, always 1 (enforced by `didWriteSegmentsN`)
- `recovery.dfa_payload_bytes` — histogram (catalog + all formatStates)

### Logs
- INFO: `"format=X coordinator invoked for upload, state_bytes=N"`
- INFO: `"format=X coordinator invoked for recovery, state_bytes=N, duration_ms=T"`
- INFO: `"recovery path=<dfa|nonDfa>, catalog_bytes=N, formats=[...]"`
- WARN: `"format=X present in payload but no coordinator registered"`
- ERROR: `"format=X coordinator onRecovery failed: <reason>"` (with full exception)

---

## 9. Performance & Constraints

### Coordinator budgets
- `captureForUpload`: soft 50ms, hard 500ms. Refresh cadence ~1s; slower starves indexing.
- `onRecovery`: soft 5s, hard 60s. Recovery is per-shard and already multi-second.
- State size per format: target <10 KB, soft limit 1 MB, hard limit 10 MB (enforced at serialization).

### Concurrency
- Coordinators are invoked from recovery threads. MUST be thread-safe (stateless preferred).
- MUST NOT block on external services (no remote calls, no external locks).

### Wire envelope size
- `DfaRecoveryPayload` = catalog (typically 1-5 KB) + sum(formatStates) (Lucene ~1-5 KB, parquet ~10 B).
- Typical DFA shard: <20 KB per metadata upload.
- Non-DFA unchanged: `segmentInfosBytes` ~1-10 KB.

### I/O savings vs Phase 2
- **Single `segments_N` write per recovery** (Phase 2 wrote twice on DFA+Lucene). One fsync saved per shard recovery on the hot path.
- **No buildSegmentInfos for DFA recovery.** Catalog bytes decoded directly.

---

## 10. Alternatives Considered

### A. `instanceof` branches per format in core
Rejected. Violates OCP.

### B. Full `IndexShard` access in coordinators
Rejected. Violates ISP.

### C. Reflection-based coordinator discovery
Rejected. Boot-time magic, hard to reason about.

### D. One coordinator per engine (not per format)
Rejected. Composite engine has N formats; one coordinator per engine re-creates the problem internally.

### E. Store per-format states as separate remote files (sidecars)
Deferred. Acceptable for states under 1 MB inline in `DfaRecoveryPayload`; switch to sidecars if a format exceeds that. The payload codec reserves a `stateLocation: INLINE|SIDECAR` field for future use.

### F. Keep tunneling catalog via `SegmentInfos.userData` on the wire
Rejected in Phase 3. Forces every DFA upload/recovery through a Lucene framing layer. Typed payload is cleaner, smaller, faster, and free of Lucene format coupling.

### G. Remove `segmentInfosBytes` field entirely in v3
Rejected. v1/v2 readers still consume it. Keeping the field (empty for DFA) preserves forward-from-old compatibility at near-zero cost.

### H. Put catalog in `formatStates["__catalog__"]` instead of a separate field
Rejected. Catalog is semantically distinct from format state (one catalog vs N format states); conflating muddies the contract and makes evolution harder.

### I. Remove `Committer.captureInMemorySegmentInfos()` in Phase 2
Chosen. Moved to coordinator-local access via cast (within the same plugin module). No Lucene leak in the generic `Committer` interface.

---

## 11. Design Decisions Resolved

| Question | Decision |
|----------|----------|
| Registry ownership | `DataFormatRegistry`, node-level singleton, accessed via `IndexShard.getFormatRecoveryRegistry()` |
| Failure semantics | Fail the shard recovery; cluster retries |
| Coordinator state versioning | Coordinator-owned, first byte = version; core does not interpret |
| `DfaRecoveryPayload` evolution | Self-describing TLV codec; optional fields added without wire-version bump |
| Wire version policy | v3 is current; v4 reserved for semantic breaking changes only |
| Concurrency | Coordinators MUST be thread-safe; contract in javadoc |
| Observability | Metrics + logs per §8 |
| `Committer.captureInMemorySegmentInfos()` | Removed from interface; kept in `LuceneCommitter`; coordinator accesses directly in same plugin |
| `RecoveryContext` mutation surface | Via narrow `RecoveryOps` — no raw `Store` exposure |
| `segments_N` writes per recovery | Exactly one, enforced via `RecoveryOps.didWriteSegmentsN()` + fallback |
| Wire format for catalog | Typed `DfaRecoveryPayload.catalogSnapshotBytes`, not `SegmentInfos.userData` tunneling |
| Local `segments_N.userData` | Unchanged — catalog still persisted there locally (only Lucene extension point) |
| `SyntheticSegmentInfos` class | Deleted in Phase 3 |
| Parquet validation | Footer magic + file count (real integrity check) |
| Composite data format | No coordinator needed; catalog already lists concrete format names |

---

## 12. Summary

**Change:**
- `FormatRecoveryCoordinator` interface + `FormatRecoveryRegistry` — per-format plugin protocol for upload + recovery validation.
- `DfaRecoveryPayload` — typed wire envelope carrying serialized catalog + per-format states as first-class fields.
- Wire version v3 with full BWC read support for v1/v2 Lucene-only indices.
- Core classes (`Store`, `RSRL`, `RemoteSegmentStoreDirectory`) are format-agnostic — no Lucene or parquet imports.

**What Phase 3 cleans up over Phase 2:**
- Deletes `SyntheticSegmentInfos` class entirely.
- `DataformatAwareCatalogSnapshot.serialize()` returns real catalog bytes, not a Lucene envelope.
- Recovery reads catalog from the typed payload, not from `segmentInfosBytes → buildSegmentInfos → userData` tunneling.
- One `segments_N` write per recovery (was two for DFA+Lucene in Phase 2).

**BWC guarantees:**
- Pre-DFA Lucene-only indices (v1/v2 bytes) read by v3 readers: unchanged behavior, no coordinator invoked, no wire changes visible.
- DFA indices: always v3, typed payload path.
- Rolling upgrade from a pre-v3 cluster: standard OpenSearch version gating; v3 not served until cluster is fully upgraded.

**Future-proof:**
- `DfaRecoveryPayload` codec is TLV — new optional fields added at tail without wire version bump.
- New format plugins: one class + one plugin-method override; zero core changes.
- Sidecar storage for large states is a codec option, not a wire-format change.
- Coordinator state bytes are coordinator-versioned; formats evolve independently.

**SOLID:**
- Core classes depend only on abstractions (`FormatRecoveryCoordinator`, `FormatRecoveryRegistry`, `RecoveryOps`).
- `DataformatAwareCatalogSnapshot` no longer depends on Lucene types.
- New formats extend functionality without modifying existing code.

**Phase 1 code becomes the first implementation** (`LuceneFormatRecoveryCoordinator`) — not a special case burned into core classes.

**Outcome:** recovery is format-agnostic at the core, typed on the wire, validated per format by plugins, and trivially extensible.
