# Design Review: Per-Format Version Design (v4)

**Reviewer:** Momus (automated critical review)  
**Document:** `docs/per-format-version-design.md`  
**Date:** 2026-05-09  

---

## Overall Verdict: APPROVE_WITH_CHANGES ⭐⭐⭐⭐

The design is structurally sound, well-grounded in the actual code flow, and follows
the existing `FormatChecksumStrategy` pattern closely. The v4 iteration addresses
prior draft issues effectively. However, there are 2 MAJOR and several MINOR issues
that should be resolved before implementation.

### Top 3 Recommendations

1. **Fix the `WriterFileSet` wire-format version gate** — `Version.CURRENT` is wrong
   for a feature introduced in a minor release. Use a named constant for the
   introducing version (§E finding).
2. **Clarify `canRead(UNKNOWN)` contract explicitly in `DataFormatVersionRange`** —
   the doc claims UNKNOWN is "always readable" but the range `[1.0.0.0, 1.0.0.0]`
   would NOT contain `(0,0,0,0)`. Add explicit handling (§C finding).
3. **Document the `getMinSegmentFormatVersion()` null-return contract change** —
   currently returns `segmentInfos.getMinSegmentLuceneVersion()` (never null for
   committed infos); the new API returns `DataFormatVersion` which can be null for
   multi-format. Callers must be audited (§A finding).

---

## A. Correctness Against Current Lucene Version Flow

### A1. `SegmentInfos` version APIs — CORRECT ✓

The doc correctly identifies:
- `getCommitLuceneVersion()` → used by `SegmentInfosCatalogSnapshot.getCommitDataFormatVersion()` (line 182)
- `getMinSegmentLuceneVersion()` → used by `SegmentInfosCatalogSnapshot.getMinSegmentFormatVersion()` (line 177)
- Per-`SegmentInfo` version → used in `buildSegmentToLuceneVersionMap()` (line 214-228)

### A2. `StoreFileMetadata.writtenBy` population — CORRECT ✓

The doc accurately describes:
- `Store.loadMetadata(CatalogSnapshot)` at line 1342: `catalogSnapshot.getFormatVersionForFile(file)` → passed to `checksumFromFile`
- The field is `Objects.requireNonNull` (StoreFileMetadata.java:75) — never null
- `isSame()` does NOT compare `writtenBy` (StoreFileMetadata.java:152-157)

### A3. `Store.loadMetadata` CatalogSnapshot path — MINOR INACCURACY

**[MINOR]** The doc (§6.1) says "the seed from `getMinSegmentFormatVersion()` is no longer needed
(the loop computes the max from scratch)." However, the CURRENT code at Store.java:1340 uses
`catalogSnapshot.getMinSegmentFormatVersion()` as the initial `maxVersion` seed. The doc's
proposed change drops this seed, which is a behavioral change for `SegmentInfosCatalogSnapshot`
where `getMinSegmentLuceneVersion()` can be non-null. The doc should explicitly note this is
an intentional simplification and explain why it's safe (it IS safe because the loop will
always find a version >= minSegmentVersion, but the reasoning should be stated).

### A4. `RemoteSegmentStoreDirectory.uploadMetadata` — CORRECT ✓

The doc correctly identifies:
- Line 877: `metadata.setWrittenByMajor(catalogSnapshot.getFormatVersionForFile(metadata.originalFilename).major)`
- The CatalogSnapshot-based overload (line 851) is the active path for DFA shards
- `setWrittenByMajor` range check at line 399-410

### A5. `RemoteStoreReplicationSource.getCheckpointMetadata` — CORRECT ✓

The doc correctly notes:
- Line 75: `version = catalogSnapshotRef.get().getCommitDataFormatVersion()`
- This stamps ONE version on ALL files (line 110-116)
- For DFA: returns `Version.LATEST` (DataformatAwareCatalogSnapshot.java:252)

### A6. Segment replication diff — CORRECT ✓

The doc correctly states diffs do NOT compare `writtenBy` (Store.java:497-517 uses checksum only).

### A7. Return type change from `Version` to `DataFormatVersion` — MAJOR

**[MAJOR]** The doc proposes changing the return type of 3 abstract methods in `CatalogSnapshot`
from `org.apache.lucene.util.Version` to `DataFormatVersion`. This is a **breaking API change**
for all existing callers. The doc identifies the 2 main callers (Store.loadMetadata,
RemoteStoreReplicationSource) but does NOT audit:
- `RemoteSegmentStoreDirectory.uploadMetadata` line 877 which calls `.major` directly
- Any test code or mock implementations of `CatalogSnapshot`

The doc mentions tests in Phase 7 (§11) but doesn't enumerate which mocks/anonymous impls
need updating. Given `CatalogSnapshot` is `@ExperimentalApi`, this is acceptable but should
be explicitly called out as a breaking change to experimental API consumers.

**Severity:** MAJOR (design flaw in completeness of impact analysis, not in correctness)

---

## B. Consistency with Existing `FormatChecksumStrategy` Pattern

### B1. Pattern comparison — STRUCTURALLY CONSISTENT ✓

The existing pattern:
1. `FormatChecksumStrategy` is an interface (FormatChecksumStrategy.java)
2. `DataFormatDescriptor` holds a `FormatChecksumStrategy` instance (DataFormatDescriptor.java:29)
3. Plugin declares via `getFormatDescriptors()` returning `Map<String, Supplier<DataFormatDescriptor>>` (ParquetDataFormatPlugin.java:107)
4. Consumed by directory layers via `DataFormatRegistry.createChecksumStrategies()` (DataFormatRegistry.java:188)

The proposed pattern:
1. `DataFormatVersion` is a value class (analogous to checksum value)
2. `DataFormatDescriptor` holds `currentVersion` + `supportedRange` (analogous to holding `checksumStrategy`)
3. Plugin declares version constant + passes to descriptor ctor (same `getFormatDescriptors` path)
4. Consumed via `DataFormatRegistry` → factory → RSSD (same plumbing path)

**This is structurally consistent.** The version follows the same declare-in-plugin,
expose-via-descriptor, consume-via-registry pattern.

### B2. Descriptor lookup plumbing — CONSISTENT ✓

The doc proposes `Function<String, DataFormatDescriptor> descriptorLookup` on RSSD,
built from `DataFormatRegistry` in the factory. This mirrors how `checksumStrategies`
map is built from `DataFormatRegistry.createChecksumStrategies()` and passed to
`DataFormatAwareStoreDirectory` (ParquetDataFormatPlugin.java:100, indexingEngine receives
`engineConfig.checksumStrategies().get(...)`).

### B3. Minor divergence — `BuiltinFormatDescriptors.LUCENE`

**[MINOR]** The existing pattern has NO built-in Lucene descriptor — `FormatChecksumStrategy`
for Lucene files is handled by `Store.checksumFromLuceneFile` (Store.java:1370) which reads
CodecUtil footer directly, not via a strategy. The proposed `BuiltinFormatDescriptors.LUCENE`
introduces a new concept (a descriptor for the built-in format) that doesn't exist today.
This is fine architecturally but is a NEW pattern, not a mirror of the existing one.

---

## C. `DataFormatVersion` Abstraction Quality

### C1. Flexible-arity `int[] components` — SUGGESTION

**[SUGGESTION]** A fixed 4-tuple (`major, minor, bugfix, build`) would be simpler and avoid:
- Defensive copies on `components()` accessor
- Edge cases in `compareTo` when arities differ (is `1.0` == `1.0.0`?)
- Allocation overhead of arrays

However, the flexible design is defensible for future-proofing. The doc should explicitly
state the `compareTo` behavior when arities differ (pad with zeros? or reject?).

### C2. `compareTo` within-format-only contract — MINOR CONCERN

**[MINOR]** The doc says "Meaningless across formats" but `compareTo` is a public method
on a `Comparable<DataFormatVersion>`. Nothing prevents callers from comparing cross-format
versions. The `getMinSegmentFormatVersion()` returning null for multi-format is the right
mitigation, but `TreeMap<DataFormatVersion, ...>` or `Collections.sort(List<DataFormatVersion>)`
would silently produce wrong results. Consider:
- Removing `Comparable` and providing only `isAtLeast`/`isAtMost` that take a format context
- OR documenting this prominently in Javadoc with `@implNote`

### C3. String parse/display ambiguity — MINOR

**[MINOR]** Is `DataFormatVersion.parse("1.0")` equal to `DataFormatVersion.of(1, 0, 0, 0)`?
The doc doesn't specify. If `parse("1.0")` produces `[1, 0]` (arity 2) and `of(1,0,0,0)`
produces `[1, 0, 0, 0]` (arity 4), are they `equals()`? The doc should specify:
- Trailing zeros are significant (different arities → not equal), OR
- Trailing zeros are normalized away (canonical form)

This matters for `WriterFileSet.formatVersionString` round-tripping.

### C4. `UNKNOWN` as `(0,0,0,0)` — MAJOR

**[MAJOR]** The doc claims (§9.3): "descriptor's `canRead(UNKNOWN)` returns true (UNKNOWN is
the lowest version, always readable)." But `DataFormatVersionRange.contains()` does a standard
range check: `v.isAtLeast(min) && v.isAtMost(max)`. For parquet with range `[1.0.0.0, 1.0.0.0]`,
`UNKNOWN = (0,0,0,0)` is NOT `>= (1,0,0,0)`. So `canRead(UNKNOWN)` would return **false**,
contradicting the doc's claim.

The doc needs to either:
1. Add special-case handling: `if (v.equals(UNKNOWN)) return true;` in `canRead()`, OR
2. Set the range min to `UNKNOWN` (0,0,0,0) for all descriptors, OR
3. Explicitly document that `canRead(UNKNOWN)` is handled by the caller (catalog-load
   validation skips UNKNOWN versions)

This is a correctness issue that would cause `UnreadableFormatVersionException` for all
pre-upgrade parquet files on first catalog-load after upgrade.

**Severity:** MAJOR (blocks correctness of BWC story)

### C5. `DataFormatVersionRange` — JUSTIFIED

The range abstraction is justified because:
- It encapsulates the min/max bounds cleanly
- It provides both full-version and major-only checks
- It avoids spreading range logic across descriptor + upload metadata
- Future formats may have different range semantics (e.g., "supports 1.x but not 2.x")

Not over-engineered for the use case.

---

## D. The `setWrittenByVersion` Convert-Then-Store Simplification

### D1. Double validation soundness — ACCEPTABLE

The flow is:
1. `descriptor.canRead(dfv)` — validates format-specific range (e.g., parquet 1.0.0.0 in [1,1])
2. `toLuceneOrDefault(dfv, LATEST)` — parquet 1.0.0.0 → fails → LATEST (10.x.y)
3. `setWrittenByMajor(10)` — validates Lucene range [MIN_SUPPORTED, LATEST] → passes

The "double validation" is sound because they validate different things:
- Step 1: "Can this node's plugin read this file?" (format-specific)
- Step 3: "Is the stored value a valid Lucene major?" (wire-format sanity)

Step 3 will always pass when the fallback is LATEST, so it's technically redundant for
non-Lucene formats. But it's a safety net against future bugs where someone passes a
non-LATEST fallback.

### D2. Concrete benefit of format-specific validation at upload time — MINOR CONCERN

**[MINOR]** For parquet v1.0.0.0, the validation at upload time catches the case where a
NEW parquet plugin (v2.0.0.0) writes files that an OLD node (supporting only v1.x) cannot
read. Without this check, the file would be uploaded successfully but fail on the replica
during catalog-load or file-open. The upload-time check provides **fail-fast on the primary**
rather than delayed failure on replicas.

However, the doc doesn't explain this benefit clearly. It should state: "The format-specific
validation at upload time ensures that a primary running a newer plugin version cannot upload
files that replicas with older plugins cannot read."

### D3. Loss of persisted format name — ACCEPTABLE

**[MINOR]** The format name is NOT persisted in `UploadedSegmentMetadata` (schema unchanged).
This means on the deserialize path, you cannot determine which format produced a file from
the metadata alone. However:
- The `DataformatAwareCatalogSnapshot` already tracks format→files mapping via `Segment.dfGroupedSearchableFiles()`
- The file prefix (`parquet/...`) encodes the format in the file path
- No current consumer needs format name from `UploadedSegmentMetadata`

Future scenarios where this might matter: cross-format migration (converting parquet→orc).
But that's speculative and can be addressed later.

---

## E. BWC Story

### E1. Rolling upgrade scenarios — CORRECT ✓

The doc (§9.5) correctly handles:
- Old primary → new replica: UNKNOWN version, passes `canRead` (assuming C4 is fixed)
- New primary → old replica: version field suppressed in wire format

### E2. Existing parquet files without `os.format_version` — CORRECT ✓ (with C4 caveat)

Reader treats absent KV as UNKNOWN. This is correct IF `canRead(UNKNOWN)` returns true
(see C4 above).

### E3. Existing in-memory WriterFileSets — CORRECT ✓

The 4-arg ctor defaults `formatVersionString` to empty → `UNKNOWN`. After next refresh,
fresh WriterFileSets carry the real version. No data loss.

### E4. `WriterFileSet` wire format gated on `Version.CURRENT` — MAJOR CONCERN

**[MINOR]** The doc says: "Write: `if (out.getVersion().onOrAfter(Version.CURRENT))
out.writeString(formatVersionString)`". Using `Version.CURRENT` is the standard pattern
for features introduced in the NEXT release. However:

The current `WriterFileSet` stream IO (WriterFileSet.java:35-36, 57-60) has NO version
gating at all — it unconditionally reads/writes `writerGeneration`, `files`, `numRows`.
This means the DFA catalog snapshot wire format was introduced without version gating
(acceptable because DFA is `@ExperimentalApi` and only used in experimental code paths).

Adding version gating NOW for the `formatVersionString` field is correct. But the doc
should specify the EXACT version constant (e.g., `Version.V_3_1_0` or whatever the
target release is), not `Version.CURRENT` which is a moving target. In practice,
`Version.CURRENT` at build time IS the introducing version, so this is technically
correct but could be confusing in code review.

### E5. Plugin BWC with 2-arg ctor — ACCEPTABLE

**[MINOR]** The doc says 2-arg ctor gives `currentVersion = UNKNOWN` and
`range = [1, Integer.MAX_VALUE]`. But the actual proposed code shows
`range = [UNKNOWN, UNKNOWN]` with "permissive behavior preserved via
descriptor.canRead returning true for UNKNOWN inputs." This is inconsistent
within the doc itself (§4.4 says `[UNKNOWN, UNKNOWN]` but §9.4 says
`[1, Integer.MAX_VALUE]`).

The doc should pick ONE behavior and be consistent. Given C4's issue,
the 2-arg ctor should probably set `range = [UNKNOWN, DataFormatVersion.of(Integer.MAX_VALUE)]`
to be truly permissive.

---

## F. Missing Elements or Over-Engineering

### F1. Missing: Metrics and logging — SUGGESTION

**[SUGGESTION]** The doc doesn't mention:
- Metrics for version validation failures (how often does `canRead` fail?)
- Logging when UNKNOWN versions are encountered (useful for tracking upgrade progress)
- Slow-log or warning when a file's format version is at the edge of the supported range

These aren't blockers but would aid operability.

### F2. Missing: Test coverage for `compareTo` edge cases — SUGGESTION

**[SUGGESTION]** The doc's Phase 7 lists test classes but doesn't specify:
- Cross-arity comparison tests (`1.0` vs `1.0.0`)
- UNKNOWN comparison behavior
- Overflow/negative component handling

### F3. Over-engineering assessment — ACCEPTABLE

The `DataFormatVersionRange` class and `DataFormatVersionUtil` are justified:
- Range encapsulates a real concept (supported version bounds)
- Util avoids spreading Lucene conversion logic across callers
- Both have clear, immediate consumers

The `BuiltinFormatDescriptors` class is borderline — it's a single constant today.
But it provides a clean extension point for future built-in formats.

### F4. Cross-format `getMinSegmentFormatVersion` returning null — ACCEPTABLE

**[MINOR]** Returning null for multi-format catalogs is the right call because:
- The sole consumer (`Store.loadMetadata`) handles null correctly (line 1340: `maxVersion` starts null)
- Computing a cross-format min is semantically meaningless
- The doc explicitly documents this limitation (§3.8)

This is NOT a latent bug because the null case is already handled in the current code
(DFA snapshot already returns null at DataformatAwareCatalogSnapshot.java:247).

### F5. Future extensibility — GOOD

The design leaves room for:
- Richer per-format version semantics (the `DataFormatVersion` class is extensible)
- Per-file version tracking (already stubbed in `WriterFileSet.formatVersion()`)
- New formats (zero server-code changes, as demonstrated in §10)
- Version-gated feature flags (could add `DataFormatVersion.supports(Feature)` later)

It does NOT lock us in because:
- `DataFormatVersion` is a value class with no behavior beyond comparison
- The range abstraction is generic enough for any versioning scheme
- The Lucene conversion is isolated in a utility class

### F6. Missing: Cluster-state coordination — NOT NEEDED

The doc correctly avoids cluster-state coordination because:
- Version information flows through the data path (WriterFileSet → CatalogSnapshot → remote metadata)
- No cluster-wide version negotiation is needed (each node validates independently)
- Plugin version is a node-local concern

### F7. Missing: Index settings override — SUGGESTION

**[SUGGESTION]** There's no mechanism to override the format version via index settings
(e.g., force-downgrade a shard to write an older format version for compatibility).
This might be useful for rollback scenarios but is not critical for v1.

---

## Summary of Findings by Severity

| Severity | Count | Key Issues |
|----------|-------|------------|
| CRITICAL | 0 | — |
| MAJOR | 2 | C4 (`canRead(UNKNOWN)` contradiction), A7 (breaking API impact analysis) |
| MINOR | 7 | A3, B3, C2, C3, D2, E4, E5 |
| SUGGESTION | 4 | C1, F1, F2, F7 |

---

## Verdict

**APPROVE_WITH_CHANGES** — The design is well-reasoned, correctly grounded in the actual
code, and structurally consistent with existing patterns. The two MAJOR issues (C4 and A7)
must be resolved before implementation begins, but neither requires architectural rework —
they are specification gaps that need explicit handling documented.

The design achieves its stated goals of simplicity and extensibility without over-engineering.
The `setWrittenByVersion` convert-then-store approach is pragmatic and avoids schema changes.
The BWC story is mostly complete pending the C4 fix.
