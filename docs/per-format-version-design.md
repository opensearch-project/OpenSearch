# Per-Format Version Design (v5.1 — String-typed)

> **Supersedes v5.** Refinement: `CatalogSnapshot`'s 3 version APIs return
> `String` (opaque, plugin-defined) instead of `org.apache.lucene.util.Version`.
> `DataformatAwareCatalogSnapshot` no longer depends on Lucene's `Version`
> type. Conversion to Lucene `Version` happens at the 3 caller sites via a
> single ~10-line util.

---

## 1. Goals

1. **Format-agnostic catalog API.** `CatalogSnapshot` version APIs return
   opaque `String` — each plugin defines the shape (semver, date, counter).
2. **Per-format validation.** On catalog load, reject WriterFileSets the
   local plugin cannot read — `descriptor.canRead(wfs.formatVersion())`.
3. **Zero coupling** between `DataformatAwareCatalogSnapshot` and Lucene.
4. **Inert-label preservation.** `StoreFileMetadata.writtenBy` stays
   Lucene-typed — converted at the single caller boundary, stamped, never
   compared downstream.
5. **Extensibility.** New formats plug in without touching `Store`,
   `RemoteSegmentStoreDirectory`, or any server-side code.

---

## 2. Current flow (HEAD as of this revision)

### 2.1 `CatalogSnapshot` API
```java
public abstract Version getFormatVersionForFile(String file);   // Lucene Version
public abstract Version getMinSegmentFormatVersion();           // Lucene Version (nullable)
public abstract Version getCommitDataFormatVersion();           // Lucene Version
```

### 2.2 Implementations
- **`SegmentInfosCatalogSnapshot`**: real Lucene versions from `SegmentInfos`.
- **`DataformatAwareCatalogSnapshot`**: stub — returns `Version.LATEST` for
  per-file, `null` for min, `Version.LATEST` for commit. Imports Lucene's
  `Version` just to return these constants.

### 2.3 Call sites
- `Store.loadMetadata(CatalogSnapshot, ...)` line 1330 — iterates files,
  stamps per-file `writtenBy`, tracks max.
- `RemoteStoreReplicationSource.getCheckpointMetadata` line 75 — reads
  commit version once, stamps on every `StoreFileMetadata`.
- `RemoteSegmentStoreDirectory.uploadMetadata` line 877 —
  `setWrittenByMajor(v.major)`; `setWrittenByMajor` range-checks against
  Lucene's `[MIN_SUPPORTED_MAJOR, LATEST.major]`.

### 2.4 `writtenBy` downstream behavior
Confirmed via exploration (`.sisyphus/handoff/writtenby-flow.md`):

| Behavior | Status |
|---|---|
| Compared in `StoreFileMetadata.isSame()` | **No** (checksum + length + hash only) |
| Compared in `recoveryDiff` / `segmentReplicationDiff` | **No** |
| Gated on (`onOrBefore`, `major < N`) | **No** |
| Asserted non-null | Yes (only check) |

**`writtenBy` is an inert label** — printed in logs, never branched on.
This shapes the whole v5.1 design.

---

## 3. The core insight

Version safety is **per-format** — only the format's plugin knows how to
parse/compare/validate its version string. The server's job is to:

1. Carry the plugin's opaque string end-to-end (producer writes it onto
   `WriterFileSet`; catalog returns it verbatim).
2. Hand it back to the plugin's `DataFormatDescriptor.canRead(String)` at
   catalog load to enforce read-compatibility.
3. Convert it to a Lucene `Version` only at the one legacy boundary where
   `writtenBy` is stamped — with `Version.LATEST` as the safe fallback for
   any non-Lucene string.

No shared cross-format version type is needed. Strings suffice.

---

## 4. Design principles (SOLID)

- **SRP**: `DataFormatDescriptor` owns format-specific version rules.
  `WriterFileSet` carries an opaque tag. `CatalogSnapshot` returns that
  tag as `String`.
- **OCP**: Adding a new format = new plugin + its own `canRead`. Zero
  server code changes.
- **LSP**: Every descriptor implements `canRead(String) → boolean`. All
  catalog snapshots return `String` from the 3 version APIs.
- **ISP**: One small version method-pair on descriptor; no multi-method
  version SPI.
- **DIP**: Validator depends on the descriptor abstraction, not concrete
  formats; callers depend on `String`, not Lucene `Version`.

---

## 5. Types

### 5.1 New: `UnreadableFormatVersionException`

```java
package org.opensearch.index.engine.dataformat;

public class UnreadableFormatVersionException extends IOException {
    public UnreadableFormatVersionException(String formatName, String fileName,
                                            String producedAt, String readerCurrent) {
        super(String.format(
            "Shard cannot read file [%s]: format [%s] produced at version [%s] "
            + "but this node supports up to [%s]. Upgrade the plugin or the node.",
            fileName, formatName, producedAt, readerCurrent));
    }
}
```

### 5.2 New: `LuceneVersionConverter`

```java
package org.opensearch.index.engine.exec.coord;

import org.apache.lucene.util.Version;

/** Boundary helper: opaque plugin version string → Lucene Version. */
public final class LuceneVersionConverter {
    private LuceneVersionConverter() {}

    /**
     * Parse {@code v} to a Lucene Version. Returns {@link Version#LATEST} when:
     * <ul>
     *   <li>{@code v} is null or empty (pre-versioning / unknown).</li>
     *   <li>{@code v} is not a valid Lucene version string (e.g., parquet "1.0.0.0").</li>
     * </ul>
     */
    public static Version toLuceneOrLatest(String v) {
        if (v == null || v.isEmpty()) return Version.LATEST;
        try {
            return Version.parse(v);
        } catch (Exception e) {
            return Version.LATEST;
        }
    }
}
```

Ten lines of real logic. That's the entire conversion surface.

### 5.3 Modified: `DataFormatDescriptor` — 2 new methods with permissive defaults

```java
public class DataFormatDescriptor {
    private final String formatName;
    private final FormatChecksumStrategy checksumStrategy;

    // Existing 2-arg ctor — UNCHANGED.
    public DataFormatDescriptor(String formatName, FormatChecksumStrategy cs) { ... }

    /**
     * Version string this plugin's writers stamp onto output.
     * Default: {@code ""} (pre-versioning / unversioned baseline).
     * Plugins opt in by overriding.
     */
    public String currentVersion() { return ""; }

    /**
     * Can this reader interpret a file/segment produced at {@code producedAt}?
     *
     * <p><b>BWC contract:</b>
     * <ul>
     *   <li>{@code null} or empty → {@code true} (pre-versioning data always readable).</li>
     *   <li>Non-empty → plugin MUST override to perform format-specific comparison.
     *       Default returns {@code true} to preserve today's unvalidated behavior.</li>
     * </ul>
     *
     * <p>{@code producedAt} is opaque to the server — only the plugin knows
     * how to parse and compare its own format's version strings.
     */
    public boolean canRead(String producedAt) { return true; }
}
```

### 5.4 Modified: `WriterFileSet` — optional `formatVersion` String

```java
public record WriterFileSet(String directory, long writerGeneration,
                            Set<String> files, long numRows,
                            String formatVersion) implements Writeable {

    // BWC 4-arg ctor.
    public WriterFileSet(String directory, long writerGeneration,
                        Set<String> files, long numRows) {
        this(directory, writerGeneration, files, numRows, "");
    }

    /** Stream IO — gate new field on {@code Version.CURRENT}. Older peers read
     *  4 fields; new peers read 5. Mixed cluster: old writer → new reader sees
     *  empty string (treated as pre-versioning). */
    public void writeTo(StreamOutput out) throws IOException {
        // existing 4 fields ...
        if (out.getVersion().onOrAfter(Version.CURRENT)) {
            out.writeString(formatVersion == null ? "" : formatVersion);
        }
    }

    public WriterFileSet(StreamInput in, String directory) throws IOException {
        // existing 4 fields ...
        this(directory, gen, files, rows,
             in.getVersion().onOrAfter(Version.CURRENT) ? in.readString() : "");
    }
}
```

Builder gets `.formatVersion(String v)`.

---

## 6. `CatalogSnapshot` API — String-typed

### 6.1 API change (experimental API — breaking for implementors + tests)

```java
// was: public abstract Version getFormatVersionForFile(String file);
// was: public abstract Version getMinSegmentFormatVersion();
// was: public abstract Version getCommitDataFormatVersion();

public abstract String getFormatVersionForFile(String file);
public abstract String getMinSegmentFormatVersion();
public abstract String getCommitDataFormatVersion();
```

**`CatalogSnapshot` is `@ExperimentalApi` — change is permissible. All
concrete subclasses and test mocks must migrate in the same PR.**

Migration targets:
- `SegmentInfosCatalogSnapshot` (§6.2).
- `DataformatAwareCatalogSnapshot` (§6.3).
- `MockCatalogSnapshot` (trivial — return `""`).
- `LuceneReaderManagerTests` anonymous impl (trivial).
- 3 callers (§7).
- 2 existing tests referencing the old return type.

### 6.2 `SegmentInfosCatalogSnapshot` — stringify at the edge

```java
@Override
public String getFormatVersionForFile(String file) {
    Version v = segmentFileVersionMap.get(file);
    if (v != null) return v.toString();
    if (file.equals(segmentInfos.getSegmentsFileName())) {
        return segmentInfos.getCommitLuceneVersion().toString();
    }
    Version siVersion = segmentFileVersionMap.get(
        RemoteStoreUtils.getSegmentName(file) + ".si");
    return siVersion != null ? siVersion.toString() : "";
}

@Override
public String getMinSegmentFormatVersion() {
    Version v = segmentInfos.getMinSegmentLuceneVersion();
    return v != null ? v.toString() : "";
}

@Override
public String getCommitDataFormatVersion() {
    return segmentInfos.getCommitLuceneVersion().toString();
}

// Lucene-typed accessors retained for internal callers that already have them:
public Version getLuceneVersionForFile(String file) { ... }
public Version getCommitLuceneVersion() { ... }
public Version getMinSegmentLuceneVersion() { ... }
```

Lucene stays internal; only `toString()` escapes outward.

### 6.3 `DataformatAwareCatalogSnapshot` — zero Lucene dependency

```java
// Note: NO import of org.apache.lucene.util.Version in this file.

@Override
public String getFormatVersionForFile(String file) {
    for (Segment s : segments) {
        for (WriterFileSet wfs : s.dfGroupedSearchableFiles().values()) {
            if (wfs.files().contains(file)) {
                return wfs.formatVersion();   // plugin-defined opaque string
            }
        }
    }
    return "";   // pre-versioning / unknown
}

@Override
public String getMinSegmentFormatVersion() {
    // Cross-format "min" is meaningless — parquet "1.0.0.0" and lucene "10.4.0"
    // occupy different namespaces. Empty string → Store.loadMetadata's seed is
    // Version.LATEST; the loop dominates anyway.
    return "";
}

@Override
public String getCommitDataFormatVersion() {
    // No single honest commit version for a multi-format catalog. Empty → the
    // converter produces Version.LATEST, same inert sentinel stamped today.
    return "";
}
```

**The DFA snapshot has no Lucene `Version` imports.** Version handling is
entirely in the plugin's namespace (opaque `String`); Lucene only appears
at the caller boundary (§7).

---

## 7. Caller updates — 3 sites, 1-line each

### 7.1 `Store.loadMetadata(CatalogSnapshot, ...)` (Store.java ~line 1340)

```java
Version maxVersion = LuceneVersionConverter.toLuceneOrLatest(
    catalogSnapshot.getMinSegmentFormatVersion());

for (String file : catalogSnapshot.getFiles(false)) {
    final Version version = LuceneVersionConverter.toLuceneOrLatest(
        catalogSnapshot.getFormatVersionForFile(file));

    if (maxVersion == null || version.onOrAfter(maxVersion)) {
        maxVersion = version;
    }
    final boolean isSiFile = SEGMENT_INFO_EXTENSION.equals(
        IndexFileNames.getExtension(file));
    checksumFromFile(directory, file, builder, logger, version, isSiFile);
}

if (maxVersion == null) {
    maxVersion = Version.CURRENT.minimumIndexCompatibilityVersion().luceneVersion;
}
```

Only change: wrap catalog-return values in `toLuceneOrLatest(...)`.

### 7.2 `RemoteStoreReplicationSource.getCheckpointMetadata` (~line 75)

```java
version = LuceneVersionConverter.toLuceneOrLatest(
    catalogSnapshotRef.get().getCommitDataFormatVersion());
```

One-line change. `version` stays Lucene-typed for downstream
`StoreFileMetadata` construction.

### 7.3 `RemoteSegmentStoreDirectory.uploadMetadata` (~line 877)

```java
metadata.setWrittenByMajor(
    LuceneVersionConverter.toLuceneOrLatest(
        catalogSnapshot.getFormatVersionForFile(metadata.originalFilename)
    ).major);
```

Existing Lucene-range check in `setWrittenByMajor` passes because
non-Lucene strings fall back to `Version.LATEST` (major=10).

---

## 8. Catalog-load validation — engine side

`DataFormatAwareNRTReplicationEngine`, after deserializing a
`DataformatAwareCatalogSnapshot` from remote store / peer:

```java
for (Segment s : snapshot.getSegments()) {
    for (var entry : s.dfGroupedSearchableFiles().entrySet()) {
        String formatName = entry.getKey();
        WriterFileSet wfs = entry.getValue();
        DataFormatDescriptor d = registry.getDescriptor(formatName);

        if (d == null) {
            throw new UnreadableFormatVersionException(
                formatName, firstFileName(wfs), wfs.formatVersion(), null);
        }
        if (!d.canRead(wfs.formatVersion())) {
            throw new UnreadableFormatVersionException(
                formatName, firstFileName(wfs), wfs.formatVersion(),
                d.currentVersion());
        }
    }
}
```

Pure-Lucene shards (`SegmentInfosCatalogSnapshot`) bypass this — Lucene's
own codec-level version check fires on file open. No new logic needed.

---

## 9. Plugin side — parquet opt-in

### 9.1 `ParquetDataFormatPlugin`

```java
public static final String PARQUET_FORMAT_VERSION = "1.0.0.0";

// In getFormatDescriptors(...):
return List.of(new DataFormatDescriptor("parquet", new ParquetChecksumStrategy()) {
    @Override public String currentVersion() {
        return PARQUET_FORMAT_VERSION;
    }
    @Override public boolean canRead(String producedAt) {
        if (producedAt == null || producedAt.isEmpty()) return true;   // BWC
        return parseMajor(producedAt) == parseMajor(PARQUET_FORMAT_VERSION);
    }
});
```

Parquet's rule: same major = readable. Plugins with different
compatibility rules (range, semver, etc.) implement them here.

### 9.2 `ParquetWriter.flush()` — stamp on `WriterFileSet`

```java
WriterFileSet wfs = WriterFileSet.builder()
    .directory(dir)
    .writerGeneration(gen)
    .addFiles(names)
    .addNumRows(rows)
    .formatVersion(ParquetDataFormatPlugin.PARQUET_FORMAT_VERSION)   // NEW
    .build();
```

### 9.3 File footer stamping (Rust — separate PR)

Parquet writer adds `os.format_version = "1.0.0.0"` to `KeyValueMetaData`.
Parquet reader reads it on open and enforces the same `canRead` rule
(translated via JNI error or duplicated in Rust).

Not part of this server-side refactor. The catalog-load check (§8) is the
primary safety; file-open is a second line of defense.

---

## 10. Backwards compatibility

### 10.1 Existing WriterFileSets
- 4-arg ctor sets `formatVersion = ""`.
- Stream IO gated on `Version.CURRENT` — older peers don't see the field.
- `canRead("")` returns `true` by default contract → pre-upgrade data
  always readable.

### 10.2 Existing `DataFormatDescriptor` subclasses / users
- 2-arg ctor retained.
- Default `currentVersion() = ""`, default `canRead(x) = true`.
- Plugins that don't opt into versioning get today's (unvalidated)
  behavior, byte-for-byte.

### 10.3 Existing Lucene flow (critical guarantee)
- `Store.loadMetadata(SegmentInfos, ...)` — untouched.
- `SegmentInfosCatalogSnapshot` internals — unchanged; only the public API
  return type flips from `Version` to `String`.
- `StoreFileMetadata.writtenBy` type — Lucene `Version`, untouched.
- `UploadedSegmentMetadata` on-disk schema — 5 fields, unchanged.
- `setWrittenByMajor` Lucene-range check — unchanged.
- `StoreFileMetadata` producers (7+ sites outside the 3 caller sites in
  §7) — all untouched.

### 10.4 Rolling upgrade

| Scenario | Behavior |
|---|---|
| Old primary → new replica | New replica reads 4-field WriterFileSet → `formatVersion=""` → `canRead("")=true` → passes. |
| New primary → old replica | New primary's stream-version gate suppresses the field. Old replica unchanged. |
| Parquet files on disk pre-upgrade | Footer lacks `os.format_version` → plugin reads as empty → `canRead("")=true`. |

---

## 11. Extensibility — adding format "orc"

```java
public class OrcDataFormatPlugin implements DataFormatPlugin {

    public static final String ORC_FORMAT_VERSION = "1.0.0";

    @Override public List<DataFormatDescriptor> getFormatDescriptors(...) {
        return List.of(new DataFormatDescriptor("orc", new OrcChecksumStrategy()) {
            @Override public String currentVersion() {
                return ORC_FORMAT_VERSION;
            }
            @Override public boolean canRead(String producedAt) {
                if (producedAt == null || producedAt.isEmpty()) return true;
                return orcSemver(producedAt).major() <= 1;
            }
        });
    }
}

// In OrcWriter.flush():
WriterFileSet.builder()...formatVersion(ORC_FORMAT_VERSION).build();
```

**Zero server-code changes.** The plugin fully self-contains its version
story. `Store`, `RemoteSegmentStoreDirectory`, `CatalogSnapshot`,
replication source — all untouched by the new format.

---

## 12. Implementation phases

### Phase 1 — Foundation (server)
1. `UnreadableFormatVersionException` (new).
2. `LuceneVersionConverter` (new).
3. `DataFormatDescriptor`: add `currentVersion()` + `canRead(String)` with
   permissive defaults. 2-arg ctor unchanged.
4. `WriterFileSet`: add `String formatVersion` field, BWC 4-arg ctor,
   builder `.formatVersion(...)`, stream IO version-gated.

### Phase 2 — Flip `CatalogSnapshot` return type
5. Change 3 APIs to return `String`.
6. `SegmentInfosCatalogSnapshot`: stringify via `Version.toString()`;
   retain Lucene-typed accessors for internal callers.
7. `DataformatAwareCatalogSnapshot`: real per-file lookup via
   WriterFileSet; `""` for min/commit; **drop Lucene `Version` import**.

### Phase 3 — Caller updates
8. `Store.loadMetadata(CatalogSnapshot, ...)` — wrap returns in
   `LuceneVersionConverter.toLuceneOrLatest`.
9. `RemoteStoreReplicationSource` — same pattern.
10. `RemoteSegmentStoreDirectory.uploadMetadata` — same pattern.

### Phase 4 — Catalog-load validation
11. `DataFormatAwareNRTReplicationEngine`: iterate WriterFileSets
    post-deserialize, call `descriptor.canRead(wfs.formatVersion())`,
    throw `UnreadableFormatVersionException` on mismatch.

### Phase 5 — Plugin side (parquet, Java)
12. `ParquetDataFormatPlugin`: declare `PARQUET_FORMAT_VERSION`; override
    descriptor's `currentVersion` + `canRead`.
13. `ParquetWriter.flush()`: stamp `.formatVersion(...)` on WriterFileSet.

### Phase 6 — Plugin side (parquet, Rust) — separate PR
14. Writer stamps `os.format_version` in KV metadata.
15. Reader verifies on open.

### Phase 7 — Tests
16. `MockCatalogSnapshot` + `LuceneReaderManagerTests` anon impl — return `""`.
17. `WriterFileSet` BWC tests (4-arg ctor, wire format round-trip).
18. `DataFormatDescriptor` default-behavior tests.
19. `SegmentInfosCatalogSnapshotTests` — assert String returns.
20. `DataformatAwareCatalogSnapshotTests` — real per-file lookup + empty
    for min/commit.
21. `DataFormatAwareNRTReplicationEngine` catalog-load validation tests
    (accept / reject paths).
22. `LuceneVersionConverterTests` — null/empty/valid/invalid coverage.

---

## 13. What does NOT change

| Component | Touched? |
|---|---|
| `StoreFileMetadata.writtenBy` type | No — Lucene `Version` |
| `UploadedSegmentMetadata` on-disk schema | No — 5 fields |
| `setWrittenByMajor` Lucene-range check | No |
| `Store.loadMetadata(SegmentInfos, ...)` | No |
| `SegmentInfosCatalogSnapshot` internals | No (only the API return type) |
| Recovery protocol / checkpoint format / FileChunkRequest wire | No |
| Remote store file layout | No |
| Segment replication diff logic | No |
| The 7 non-main `writtenBy` producers | No |

---

## 14. Change tally vs. prior drafts

| | v4 (stashed) | v5.1 (this) |
|---|---|---|
| New classes | 5 (`DataFormatVersion`, `...Range`, `...Util`, `BuiltinFormatDescriptors`, `UnreadableFormatVersionException`) | **2** (`UnreadableFormatVersionException`, `LuceneVersionConverter`) |
| Modified files | ~16 | **~8** (`DataFormatDescriptor`, `WriterFileSet`, `CatalogSnapshot`, 2 impls, 3 callers, test mocks) |
| Descriptor plumbing through RSSD factory | yes | **no** — validation lives in replication engine |
| `DataformatAwareCatalogSnapshot` depends on Lucene | yes | **no** |
| New version namespace / arity / compare semantics to document | yes | **no** — it's a `String`; plugins define semantics |

---

## 15. Summary

**v5.1 = v5 minus Lucene coupling in the DFA catalog.**

- `CatalogSnapshot` version APIs return opaque `String`.
- `DataformatAwareCatalogSnapshot` drops the Lucene `Version` import — it
  only surfaces plugin-defined strings from `WriterFileSet`.
- `SegmentInfosCatalogSnapshot` stringifies its Lucene versions at the
  outbound edge.
- 3 caller sites wrap the return in `LuceneVersionConverter.toLuceneOrLatest`
  to get a Lucene `Version` for the inert `writtenBy` stamp.
- Safety lives where it belongs: `DataFormatDescriptor.canRead(String)` at
  catalog-load, with plugin-defined parsing.

**Two new classes. Eight files touched. Zero shared cross-format version
type. Complete decoupling of DFA from Lucene. Same safety as v4.**

The problem is one sentence:
> *Let each format's plugin decide whether it can read a given opaque
> version string carried on the WriterFileSet, and convert to Lucene
> Version only where the legacy inert-label field demands it.*
