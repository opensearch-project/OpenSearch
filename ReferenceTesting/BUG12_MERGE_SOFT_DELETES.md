# Bug #12: Merge Fallback Path Includes Soft-Deleted Docs in Star Tree

## Problem Statement

After upgrading an index with soft deletes and then force-merging, the star tree in the merged segment includes ALL docs (100k) instead of only live docs (90032). Aggregation results after merge return pre-delete counts.

```
Before delete:  FEMALE=50124, MALE=49876 (total=100000)
After delete:   FEMALE=45114, MALE=44918 (total=90032)  ← 9968 deleted
After upgrade:  FEMALE=45114, MALE=44918 (total=90032)  ← Bug #7 fix works ✓
After merge:    FEMALE=50124, MALE=49876 (total=100000) ← BUG: includes deleted docs
```

---

## Root Cause Analysis

### Why the Fallback Path is Hit

During force merge, `Composite912DocValuesWriter.mergeStarTreeFields()` checks if source segments have `CompositeIndexReader`:

```java
for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
    if (mergeState.docValuesProducers[i] instanceof CompositeIndexReader) { ... }
}
```

Source segment `_0` has `docValuesGen != -1` (soft deletes applied), so its codec was NOT switched to `Composite912Codec` during upgrade — it stays `Lucene104`. When Lucene opens `_0` for the merge, it uses `Lucene104Codec` whose doc values reader is NOT a `CompositeIndexReader`. Result: `starTreeSubsPerField` is empty → fallback path.

### Why `mergeState.liveDocs` Doesn't Help

**Confirmed via debug logging:**
```
totalDeadInSource=0  ← mergeState.liveDocs says ALL docs are live
```

`mergeState.liveDocs[i]` only reflects **hard deletes** (`.liv` file). Soft-deleted docs (marked via `__soft_deletes=1` in doc values) appear as "live" in `mergeState.liveDocs`. This is by design in Lucene — soft deletes are a doc values field, not a hard delete bitset.

### Why the Merged Doc Values Include Deleted Docs

Lucene's `DocValuesConsumer.merge()` (called via `super.merge()`) writes doc values for ALL docs that pass the `mergeState.liveDocs` filter. Since soft-deleted docs are "live" according to `mergeState.liveDocs`, their doc values are written into the merged segment. The merged segment then has:
- `docs=90032` (live docs count for search)
- `deleted=19936` (soft-deleted docs retained in segment)
- Doc values for ALL 109968 docs (live + soft-deleted)

### Why `__soft_deletes` is Not Captured

In `Composite912DocValuesWriter.addNumericField()`:
```java
if (mergeState.get() != null) {
    if (compositeFieldSet.contains(field.name)) {
        mergedFieldProducerMap.put(field.name, valuesProducer);
    }
}
```

`compositeFieldSet` only contains user-defined composite fields (dimensions + metrics). `__soft_deletes` is a system field — it's NOT in `compositeFieldSet`, so its producer is never captured.

---

## How Lucene Soft Deletes Work During Merge

### Configuration
```java
// In NativeLuceneIndexWriterFactory.IndexWriterConfigBuilder:
iwc.setSoftDeletesField(Lucene.SOFT_DELETES_FIELD);  // "__soft_deletes"
```

### Merge Behavior
1. `IndexWriter` creates `MergeState` with `liveDocs` = hard deletes only
2. `DocValuesConsumer.merge()` writes ALL docs (hard-live + soft-deleted)
3. `__soft_deletes` field is written as a regular numeric doc value
4. After merge, `IndexWriter` computes the merged segment's live docs by reading `__soft_deletes` from the merged segment
5. The merged segment's `SegmentCommitInfo` records `softDelCount` based on `__soft_deletes` values

### Key Insight
The `__soft_deletes` field IS written during merge (via `addNumericField`). We just need to capture it.

---

## The Fix

### Design: Single Class, Two Modes

Instead of creating a separate `SequentialLiveDocsFilteredDocValuesProducer`, extend `LiveDocsFilteredDocValuesProducer` with a second constructor for skip-only mode:

- **Remap mode** (existing, used in upgrade path): Remaps doc IDs to contiguous space via `remappedToOriginal[]` array. Uses `advanceExact()` on the delegate.
- **Skip-only mode** (new, used in merge fallback path): No remapping. Skips deleted docs during sequential `nextDoc()` iteration. Never calls `advanceExact()` on the delegate.

The mode is determined by whether `remappedToOriginal` is null.

### Step 1: Capture `__soft_deletes` During Merge

In `Composite912DocValuesWriter`, add fields and capture logic:

```java
// New fields
private DocValuesProducer softDeletesProducer;
private FieldInfo softDeletesFieldInfo;

@Override
public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    delegate.addNumericField(field, valuesProducer);
    if (mergeState.get() == null && segmentHasCompositeFields) {
        createCompositeIndicesIfPossible(valuesProducer, field);
    }
    if (mergeState.get() != null) {
        if (compositeFieldSet.contains(field.name)) {
            mergedFieldProducerMap.put(field.name, valuesProducer);
        }
        // Capture __soft_deletes producer for merge-path live docs filtering
        if ("__soft_deletes".equals(field.name)) {
            softDeletesProducer = valuesProducer;
            softDeletesFieldInfo = field;
        }
    }
}
```


### Step 2: Build Live Docs Bitset from `__soft_deletes`

```java
private FixedBitSet buildSoftDeleteLiveDocsBitset() throws IOException {
    if (softDeletesProducer == null) {
        return null; // no soft deletes field → all docs live
    }

    int mergedMaxDoc = this.mergeState.get().segmentInfo.maxDoc();
    FixedBitSet liveBits = new FixedBitSet(mergedMaxDoc);
    liveBits.set(0, mergedMaxDoc); // start: all live

    // Iterate __soft_deletes: docs with value=1 are soft-deleted
    NumericDocValues softDelDV = softDeletesProducer.getNumeric(softDeletesFieldInfo);
    int doc;
    while ((doc = softDelDV.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        if (softDelDV.longValue() == 1) {
            liveBits.clear(doc);
        }
    }

    // If all docs are live, return null (no filtering needed)
    if (liveBits.cardinality() == mergedMaxDoc) {
        return null;
    }

    return liveBits;
}
```

### Step 3: Add Skip-Only Constructor to `LiveDocsFilteredDocValuesProducer`

```java
// NEW constructor — skip-only mode (merge fallback path)
public LiveDocsFilteredDocValuesProducer(DocValuesProducer delegate, Bits liveDocs) {
    this.delegate = delegate;
    this.liveDocs = liveDocs;
    this.remappedToOriginal = null; // null = skip-only mode
    this.numLiveDocs = -1;
}
```

Then in `getSortedNumeric()` and `getSortedSet()`, branch on `remappedToOriginal == null`:

```java
@Override
public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    SortedNumericDocValues inner = delegate.getSortedNumeric(field);

    if (remappedToOriginal == null) {
        // Skip-only mode: sequential iteration, no remapping
        return new SortedNumericDocValues() {
            @Override
            public int nextDoc() throws IOException {
                int doc;
                while ((doc = inner.nextDoc()) != NO_MORE_DOCS) {
                    if (liveDocs.get(doc)) return doc;
                }
                return NO_MORE_DOCS;
            }
            @Override public int advance(int target) throws IOException {
                int doc = inner.advance(target);
                if (doc == NO_MORE_DOCS || liveDocs.get(doc)) return doc;
                return nextDoc();
            }
            @Override public boolean advanceExact(int target) throws IOException {
                throw new UnsupportedOperationException("merge path: sequential only");
            }
            @Override public long nextValue() throws IOException { return inner.nextValue(); }
            @Override public int docValueCount() { return inner.docValueCount(); }
            @Override public int docID() { return inner.docID(); }
            @Override public long cost() { return inner.cost(); }
        };
    }

    // EXISTING remap mode — unchanged
    return new SortedNumericDocValues() { /* ... existing ... */ };
}
```

Same pattern for `getSortedSet()`.

### Step 4: Wire Into mergeStarTreeFields()

```java
if (hasAllCompositeFields) {
    Map<String, DocValuesProducer> fieldProducerMapForMerge = buildFieldProducerMapFromMergeState(mergeState);

    // Build live docs bitset from __soft_deletes field captured during super.merge()
    FixedBitSet mergedLiveDocs = buildSoftDeleteLiveDocsBitset();

    Map<String, DocValuesProducer> buildProducerMap;
    if (mergedLiveDocs != null) {
        buildProducerMap = new HashMap<>();
        for (Map.Entry<String, DocValuesProducer> e : fieldProducerMapForMerge.entrySet()) {
            // skip-only constructor — no remapping, no advanceExact
            buildProducerMap.put(e.getKey(),
                new LiveDocsFilteredDocValuesProducer(e.getValue(), mergedLiveDocs));
        }
    } else {
        buildProducerMap = fieldProducerMapForMerge;
    }

    // Use live doc count for SegmentWriteState
    int liveDocCount = mergedLiveDocs != null
        ? mergedLiveDocs.cardinality()
        : mergeState.segmentInfo.maxDoc();
    SegmentWriteState mergeWriteState = createWriteStateWithDocCount(liveDocCount);

    try (StarTreesBuilder starTreesBuilder = new StarTreesBuilder(
            mergeWriteState, mapperService, fieldNumberAcrossCompositeFields)) {
        starTreesBuilder.build(metaOut, dataOut, buildProducerMap, compositeDocValuesConsumer);
    }
}
```

---

## Why This Works

1. **`addNumericField("__soft_deletes", producer)` IS called during merge** — Lucene writes the `__soft_deletes` field for all merged docs. We capture the producer.

2. **The producer iterates in merged doc ID order** — doc IDs correspond to the merged segment's doc IDs. Docs with `longValue() == 1` are soft-deleted.

3. **Skip-only mode on `LiveDocsFilteredDocValuesProducer`** — when the star tree builder calls `nextDoc()` on dimension/metric producers, the wrapper skips over doc IDs marked deleted in the bitset. No `advanceExact()` needed.

4. **`SegmentWriteState` uses live doc count** — the star tree builder knows the correct number of docs to expect.

5. **Single class, two modes** — no new class needed. `remappedToOriginal == null` distinguishes the modes.

---

## Why Previous Approaches Failed

| Approach | Why It Failed |
|----------|--------------|
| `mergeState.liveDocs` bitset | Only reflects hard deletes. Soft-deleted docs appear "live". |
| `LiveDocsFilteredDocValuesProducer` (advanceExact) | Merge producers don't support `advanceExact()` — throws `UnsupportedOperationException` |
| Adjusting `SegmentWriteState.maxDoc` | Producers still iterate all docs regardless of maxDoc |
| `SequentialLiveDocsFilteredDocValuesProducer` with `mergeState.liveDocs` | Bitset has all docs marked live (cardinality=109968) because soft deletes aren't in `mergeState.liveDocs` |

---

## Implementation Checklist

- [x] Add `softDeletesProducer` and `softDeletesFieldInfo` fields to `Composite912DocValuesWriter`
- [x] Capture `__soft_deletes` producer in `addNumericField()` during merge
- [x] Implement `buildSoftDeleteLiveDocsBitset()` method
- [x] Add skip-only constructor to `LiveDocsFilteredDocValuesProducer`: `LiveDocsFilteredDocValuesProducer(delegate, liveDocs)` with `remappedToOriginal = null`
- [x] Add skip-only mode branches in `getSortedNumeric()` and `getSortedSet()` (check `remappedToOriginal == null`)
- [x] Use skip-only constructor in fallback path: `new LiveDocsFilteredDocValuesProducer(producer, mergedLiveDocs)`
- [x] Keep `state.segmentInfo.maxDoc()` UNCHANGED (do NOT adjust to live count — builder handles sparse iteration)
- [ ] Delete `SequentialLiveDocsFilteredDocValuesProducer.java` (no longer needed)
- [ ] Remove unused `buildMergedLiveDocsBitset()` and `wrapWithLiveDocsFilter()` methods
- [x] Test with `test_soft_delete_upgrade.sh` — PASSED ✓
- [x] Test with `test_mixed_state_and_merge.sh` — PASSED ✓

---

## Expected Test Results After Fix

```
Before delete:  FEMALE=50124, MALE=49876 (total=100000)
After delete:   FEMALE=45114, MALE=44918 (total=90032)
After upgrade:  FEMALE=45114, MALE=44918 (total=90032)  ✓
After merge:    FEMALE=45114, MALE=44918 (total=90032)  ✓ ← FIXED
```

**VERIFIED** — both `test_soft_delete_upgrade.sh` and `test_mixed_state_and_merge.sh` pass.

### Key Implementation Detail

Do NOT adjust `SegmentWriteState.maxDoc()` to the live doc count. The star tree builder uses `totalSegmentDocs = writeState.segmentInfo.maxDoc()` to size arrays and iterate. The skip-only producers return non-contiguous doc IDs (skipping deleted ones), and the builder handles this correctly when `totalSegmentDocs` equals the full merged maxDoc. Setting it to the live count causes the builder to stop early (it iterates `for (i = 0; i < totalSegmentDocs; i++)` calling `nextDoc()` — if totalSegmentDocs is too small, it misses live docs at the end of the segment).

---

## Relationship to Other Bugs

| Bug | Path | Status | Relationship |
|-----|------|--------|-------------|
| #7 | Upgrade (per-segment) | ✅ Fixed | Uses `buildLiveDocsBitset()` reading `__soft_deletes` from `SegmentReader` |
| #12 | Merge (fallback) | ⚠️ This fix | Uses captured `__soft_deletes` producer from `addNumericField()` |
| Both | Same concept | Same field | Both read `__soft_deletes` to identify deleted docs, just from different sources |

The upgrade path (Bug #7) reads `__soft_deletes` from the `SegmentReader` (via `segmentReader.getNumericDocValues("__soft_deletes")`). The merge path (Bug #12) reads it from the producer captured during `addNumericField()`. Same field, different access patterns.
